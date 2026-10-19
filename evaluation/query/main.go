package main

import (
	"context"
	"database/sql"
	"dt-real-bench/config"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ======================================================================================
// CONFIGURATION & CONSTANTS
// ======================================================================================

const (
	OutputCSV      = "output/query_evaluation_results_v2.csv"
	RunsPerTest    = 30                     // 30 runs per scenario for solid P50, P95, P99 tail latency stats
	WarmUpRuns     = 5                      // Warm-up runs to stabilize connection pools/JIT
	ISUSubBatch    = 250                    // Max IDs per parallel query worker per ISU
	HistorySamples = 100                    // Number of historical time-series observations per sensor
	SimulatedDelay = 150 * time.Millisecond // Inter-run delay
)

// Global Shared State
var (
	cfg      config.Env
	pgDBs    []*sql.DB   // Data Plane: Multiple Independent Storage Units (ISUs)
	dgClient *dgo.Dgraph // Semantic Plane: Knowledge Graph
)

// ======================================================================================
// SEMANTIC PLANE ENTITY MODEL (Topological White Nodes & Articulated Black Nodes)
// ======================================================================================

// Observation represents a timestamped telemetry state node in the Centralized KG
type Observation struct {
	Uid       string    `json:"uid,omitempty"`
	DType     []string  `json:"dgraph.type,omitempty"`
	Timestamp time.Time `json:"timestamp,omitempty"`
	Value     float64   `json:"value,omitempty"`
}

// Thing represents any entity in the Knowledge Graph (WindFarm, Zone, Turbine, Component, Sensor).
// Purely topological entities (white nodes: WindFarm, Zone, Turbine, Component) have HasChild edges
// and no Data Anchor. IoT-linked entities (black nodes: Sensor) contain a Data Anchor (IsuID, ExternalID).
type Thing struct {
	Uid            string        `json:"uid,omitempty"`
	DType          []string      `json:"dgraph.type,omitempty"`
	Name           string        `json:"name,omitempty"`
	AssetCategory  string        `json:"asset_category,omitempty"` // "WindFarm", "Zone", "Turbine", "Component", "Sensor"
	SubType        string        `json:"sub_type,omitempty"`       // e.g., "Gearbox", "Generator", "Temperature", "Vibration"
	MaxThreshold   float64       `json:"max_threshold,omitempty"`
	IsuID          int           `json:"isu_id,omitempty"`         // Data Anchor: Target ISU index
	ExternalID     int           `json:"external_id,omitempty"`    // Data Anchor: Local TSDB identifier
	Value          float64       `json:"value,omitempty"`          // Dynamic telemetry state
	HasObservation []Observation `json:"hasObservation,omitempty"` // Time-series observations in Centralized KG
	HasChild       []Thing       `json:"hasChild,omitempty"`       // Topological hierarchical relationships
}

// ======================================================================================
// EPHEMERAL IN-MEMORY RDF TRIPLE STORE & EMBEDDED INFERENCE / BGP ENGINE (STAGE 3 & 4)
// ======================================================================================

// RDFTriple represents a canonical semantic triple (Subject, Predicate, Object)
type RDFTriple struct {
	Subject   string
	Predicate string
	Object    string
	NumValue  float64 // Optional parsed numeric literal for fast range filter evaluation
}

// EphemeralTripleGraph implements an in-memory RDF graph with SPO, POS, and OSP indexes
// to support Stage 3 (Semantic Hydration) and Stage 4 (SPARQL-like BGP & Rule Inference).
type EphemeralTripleGraph struct {
	Triples    []RDFTriple
	SPO        map[string]map[string][]string  // Subject -> Predicate -> []Object
	POS        map[string]map[string][]string  // Predicate -> Object -> []Subject
	OSP        map[string]map[string][]string  // Object -> Subject -> []Predicate (inverse traversal)
	NumericLit map[string]map[string]float64   // Subject -> Predicate -> float64 literal
	SensorURIs map[int]string                  // ExternalID -> Sensor URI (e.g., "dt:Sensor_1")
	Matched    int                             // Count of query bindings matched in Stage 4
}

func NewEphemeralTripleGraph(estimatedSensors int) *EphemeralTripleGraph {
	capHint := estimatedSensors * 6
	return &EphemeralTripleGraph{
		Triples:    make([]RDFTriple, 0, capHint),
		SPO:        make(map[string]map[string][]string, estimatedSensors*2),
		POS:        make(map[string]map[string][]string, 16),
		OSP:        make(map[string]map[string][]string, estimatedSensors*2),
		NumericLit: make(map[string]map[string]float64, estimatedSensors),
		SensorURIs: make(map[int]string, estimatedSensors),
	}
}

// InsertTriple synthesizes and indexes a semantic triple (s, p, o) into the ephemeral graph
func (g *EphemeralTripleGraph) InsertTriple(s, p, o string, numVal float64, isNumeric bool) {
	g.Triples = append(g.Triples, RDFTriple{Subject: s, Predicate: p, Object: o, NumValue: numVal})

	if _, ok := g.SPO[s]; !ok {
		g.SPO[s] = make(map[string][]string, 4)
	}
	g.SPO[s][p] = append(g.SPO[s][p], o)

	if _, ok := g.POS[p]; !ok {
		g.POS[p] = make(map[string][]string, 8)
	}
	g.POS[p][o] = append(g.POS[p][o], s)

	if _, ok := g.OSP[o]; !ok {
		g.OSP[o] = make(map[string][]string, 2)
	}
	g.OSP[o][s] = append(g.OSP[o][s], p)

	if isNumeric {
		if _, ok := g.NumericLit[s]; !ok {
			g.NumericLit[s] = make(map[string]float64, 2)
		}
		g.NumericLit[s][p] = numVal
	}
}

type ExpectedSensor struct {
	ExternalID    int
	IsuID         int
	ZoneName      string
	TurbineName   string
	ComponentType string
	SensorType    string
	MaxThreshold  float64
	Value         float64
}

type QueryMetrics struct {
	ExperimentType string
	NumSensors     int
	Architecture   string
	NumISUs        int
	PartitionMode  string // "Uniform", "ModerateSkew", "HotspotSkew"
	QueryType      string // "Q1_Selective", "Q2_Filtering", "Q3_Inference"
	Concurrency    int
	RunID          int
	TotalLatency   float64 // End-to-end execution time (ms): Stage 1 + 2 + 3 + 4
	Stage1AnchorMs float64 // Stage 1: Topological Traversal & Data Anchor Resolution in KG (ms)
	Stage2ISUIOMs  float64 // Stage 2: Parallel State Retrieval from ISUs (ms)
	Stage3HydrMs   float64 // Stage 3: Semantic Hydration & Triple Synthesis in ephemeral graph (ms)
	Stage4QueryMs  float64 // Stage 4: Embedded Inference & BGP Query Execution (ms)
	SuccessRate    float64 // Data Integrity percentage (0-100%)
	MemAllocMB     float64 // RAM allocated per query (MB)
}

// Raw JSON payload returned by an ISU Semantic Wrapper during Stage 2
type wrapperRecord struct {
	ID      int     `json:"asset_id"`
	Reading float64 `json:"reading"`
	Unit    string  `json:"unit"`
}

// ======================================================================================
// SECTION I: EXPERIMENT CONTROL (3 SUB-EXPERIMENTS FOR REVIEWERS)
// ======================================================================================

func main() {
	log.SetFlags(0)
	rand.Seed(42)
	cfg = config.LoadEnv()

	initInfrastructure()
	defer closeInfrastructure()

	writer := initCSVWriter()
	defer writer.Flush()

	maxISUs := len(pgDBs)
	fmt.Printf("Starting END-TO-END SEMANTIC QUERY EVALUATION (WarmUp: %d, Runs: %d, Available ISUs: %d)...\n",
		WarmUpRuns, RunsPerTest, maxISUs)

	// ----------------------------------------------------------------------------------
	// SUB-EXPERIMENT 1: Horizontal Scalability across N ISUs & Graph Sizes (End-to-End Q3)
	// Addresses Reviewer Point 2 (ISU scaling) & Point 3 (End-to-End full state + inference)
	// ----------------------------------------------------------------------------------
	fmt.Println("\n======================================================================")
	fmt.Println("SUB-EXPERIMENT 1: HORIZONTAL SCALABILITY (N = 1, 2, 4, 8 ISUs)")
	fmt.Println("======================================================================")
	sensorCounts := []int{500, 1000, 2000, 3000, 4000, 5000}
	isuConfigs := []int{0, 1, 2, 4, 8} // 0 = Centralized Dgraph Baseline

	for _, count := range sensorCounts {
		for _, numISUs := range isuConfigs {
			if numISUs > maxISUs {
				continue
			}
			archName := "Centralized"
			if numISUs > 0 {
				archName = fmt.Sprintf("Federated_%dISU", numISUs)
			}
			runScenario(writer, "Exp1_Scalability", count, archName, numISUs, "Uniform", "Q3_Inference", 1)
		}
	}

	// ----------------------------------------------------------------------------------
	// SUB-EXPERIMENT 2: Query Complexity (Q1, Q2, Q3) & Concurrent Workloads (C = 1, 5, 10, 20)
	// Addresses Reviewer Point 3 (Selective retrieval, filtering, inference, concurrency)
	// ----------------------------------------------------------------------------------
	fmt.Println("\n======================================================================")
	fmt.Println("SUB-EXPERIMENT 2: QUERY COMPLEXITY (Q1, Q2, Q3) & CONCURRENCY")
	fmt.Println("======================================================================")
	queries := []string{"Q1_Selective", "Q2_Filtering", "Q3_Inference"}
	concurrencies := []int{1, 5, 10, 20}
	targetISUs := 8
	if maxISUs < 8 {
		targetISUs = maxISUs
	}

	for _, qType := range queries {
		for _, conc := range concurrencies {
			// Centralized vs Federated (8 ISUs) at 5000 sensors
			runScenario(writer, "Exp2_Complexity_Concurrency", 5000, "Centralized", 0, "Uniform", qType, conc)
			runScenario(writer, "Exp2_Complexity_Concurrency", 5000, fmt.Sprintf("Federated_%dISU", targetISUs), targetISUs, "Uniform", qType, conc)
		}
	}

	// ----------------------------------------------------------------------------------
	// SUB-EXPERIMENT 3: Data Partition Skew Analysis (Uniform vs Moderate vs Hotspot)
	// Addresses Reviewer Point 2 ("Skewed partitions should also be considered")
	// ----------------------------------------------------------------------------------
	fmt.Println("\n======================================================================")
	fmt.Println("SUB-EXPERIMENT 3: DATA PARTITION SKEW (UNIFORM vs SKEWED PARTITIONS)")
	fmt.Println("======================================================================")
	skewModes := []string{"Uniform", "ModerateSkew", "HotspotSkew"}
	skewISUs := 4
	if maxISUs >= 8 {
		skewISUs = 8
	} else if maxISUs < 4 {
		skewISUs = maxISUs
	}

	for _, skew := range skewModes {
		runScenario(writer, "Exp3_Skew", 5000, fmt.Sprintf("Federated_%dISU", skewISUs), skewISUs, skew, "Q3_Inference", 1)
	}

	fmt.Println("\nEvaluation Complete. Raw data saved to", OutputCSV)
}

func runScenario(writer *csv.Writer, expType string, count int, arch string, numISUs int, partMode string, queryType string, concurrency int) {
	fmt.Printf("\n--- [%s] Arch: %s | Sensors: %d | Part: %s | Query: %s | Conc: %d ---\n",
		expType, arch, count, partMode, queryType, concurrency)

	fmt.Print("   > Setting up multi-hop Thing topology (hasChild) & state... ")
	expectedValues := setupEnvironment(count, numISUs, partMode)
	performUpdates(arch, numISUs, expectedValues)
	time.Sleep(2 * time.Second) // Allow Dgraph BadgerDB & Postgres indexes to settle before warm-up
	fmt.Println("Ready.")

	// Warm-up
	for w := 0; w < WarmUpRuns; w++ {
		executeWorkload(arch, numISUs, queryType, concurrency, expectedValues)
		time.Sleep(SimulatedDelay)
	}

	// Measurement Runs
	for r := 1; r <= RunsPerTest; r++ {
		time.Sleep(SimulatedDelay)

		m := executeWorkload(arch, numISUs, queryType, concurrency, expectedValues)
		m.ExperimentType = expType
		m.NumSensors = count
		m.Architecture = arch
		m.NumISUs = numISUs
		m.PartitionMode = partMode
		m.QueryType = queryType
		m.Concurrency = concurrency
		m.RunID = r

		if r == 1 || r == RunsPerTest || r%10 == 0 {
			fmt.Printf("     Run %2d/%d: Total=%.2fms (S1_Anchor=%.2fms | S2_IO=%.2fms | S3_Hydr=%.2fms | S4_Query=%.2fms) | Integrity=%.1f%% | Mem=%.2fMB\n",
				r, RunsPerTest, m.TotalLatency, m.Stage1AnchorMs, m.Stage2ISUIOMs, m.Stage3HydrMs, m.Stage4QueryMs, m.SuccessRate, m.MemAllocMB)
		}

		logResult(writer, m)
	}
}

// ======================================================================================
// SECTION II: 4-STAGE END-TO-END FEDERATED ORCHESTRATION ENGINE
// ======================================================================================

func sinceMs(t time.Time) float64 {
	return float64(time.Since(t).Nanoseconds()) / 1e6
}

func executeWorkload(arch string, numISUs int, queryType string, concurrency int, expected map[int]ExpectedSensor) QueryMetrics {
	var memBefore, memAfter runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	startWall := time.Now()

	if concurrency <= 1 {
		s1, s2, s3, s4, integrity := executeSingleQuery(arch, numISUs, queryType, expected)
		totalMs := sinceMs(startWall)
		runtime.ReadMemStats(&memAfter)
		memMB := float64(memAfter.TotalAlloc-memBefore.TotalAlloc) / 1024.0 / 1024.0

		return QueryMetrics{
			TotalLatency:   totalMs,
			Stage1AnchorMs: s1,
			Stage2ISUIOMs:  s2,
			Stage3HydrMs:   s3,
			Stage4QueryMs:  s4,
			SuccessRate:    integrity,
			MemAllocMB:     memMB,
		}
	}

	// Concurrent query execution (C > 1)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var sumTotal, sumS1, sumS2, sumS3, sumS4, sumIntegrity float64

	for c := 0; c < concurrency; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			qStart := time.Now()
			s1, s2, s3, s4, integrity := executeSingleQuery(arch, numISUs, queryType, expected)
			qTotal := sinceMs(qStart)

			mu.Lock()
			sumTotal += qTotal
			sumS1 += s1
			sumS2 += s2
			sumS3 += s3
			sumS4 += s4
			sumIntegrity += integrity
			mu.Unlock()
		}()
	}
	wg.Wait()

	runtime.ReadMemStats(&memAfter)
	memMB := (float64(memAfter.TotalAlloc-memBefore.TotalAlloc) / 1024.0 / 1024.0) / float64(concurrency)
	cFloat := float64(concurrency)

	return QueryMetrics{
		TotalLatency:   sumTotal / cFloat,
		Stage1AnchorMs: sumS1 / cFloat,
		Stage2ISUIOMs:  sumS2 / cFloat,
		Stage3HydrMs:   sumS3 / cFloat,
		Stage4QueryMs:  sumS4 / cFloat,
		SuccessRate:    sumIntegrity / cFloat,
		MemAllocMB:     memMB,
	}
}

func executeSingleQuery(arch string, numISUs int, queryType string, expected map[int]ExpectedSensor) (s1, s2, s3, s4, integrity float64) {
	if arch == "Centralized" {
		return executeCentralizedQuery(queryType, expected)
	}
	return executeFederatedQuery(numISUs, queryType, expected)
}

// executeCentralizedQuery traverses the multi-hop hasChild relationships in the monolithic KG,
// resolves the Last Known Value (LKV) via `hasObservation(orderdesc: timestamp, first: 1)`,
// and executes the same Stage 4 semantic evaluation over the resulting triples.
func executeCentralizedQuery(queryType string, expected map[int]ExpectedSensor) (s1, s2, s3, s4, integrity float64) {
	ctx := context.Background()
	t0 := time.Now()

	var q string
	switch queryType {
	case "Q1_Selective":
		q = `{
			roots(func: eq(name, "Zone_0")) {
				uid
				name
				asset_category
				hasChild {
					uid
					name
					asset_category
					hasChild {
						uid
						name
						asset_category
						sub_type
						hasChild {
							uid
							name
							asset_category
							sub_type
							external_id
							max_threshold
							hasObservation(orderdesc: timestamp, first: 1) {
								value
							}
						}
					}
				}
			}
		}`
	case "Q2_Filtering":
		q = `{
			roots(func: eq(sub_type, "Gearbox")) {
				uid
				name
				asset_category
				sub_type
				hasChild {
					uid
					name
					asset_category
					sub_type
					external_id
					max_threshold
					hasObservation(orderdesc: timestamp, first: 1) {
						value
					}
				}
			}
		}`
	case "Q3_Inference":
		q = `{
			roots(func: eq(asset_category, "WindFarm")) {
				uid
				name
				asset_category
				hasChild {
					uid
					name
					asset_category
					hasChild {
						uid
						name
						asset_category
						hasChild {
							uid
							name
							asset_category
							sub_type
							hasChild {
								uid
								name
								asset_category
								sub_type
								external_id
								max_threshold
								hasObservation(orderdesc: timestamp, first: 1) {
									value
								}
							}
						}
					}
				}
			}
		}`
	}

	resp, err := dgClient.NewTxn().Query(ctx, q)
	if err != nil {
		log.Printf("Centralized Dgraph Query Error: %v", err)
		return 0, 0, 0, 0, 0
	}

	var decode struct {
		Roots []Thing `json:"roots"`
	}
	json.Unmarshal(resp.Json, &decode)
	s1 = sinceMs(t0)

	// Stage 4: Index triples & run embedded BGP / inference engine over the retrieved hierarchy
	t4 := time.Now()
	triples := extractTopologicalTriples(decode.Roots, len(expected), nil)
	matched := runStage4SemanticEngine(queryType, triples, len(expected))
	s4 = sinceMs(t4)

	integrity = verifyIntegrity(queryType, matched, expected)
	return s1, 0, 0, s4, integrity
}

// executeFederatedQuery implements the 4-Stage Orchestration Sequence described in Section 4:
//   Stage 1: Data Anchor Resolution (KG topological traversal)
//   Stage 2: Parallel State Retrieval from ISUs via Canonical JSON Semantic Wrappers
//   Stage 3: Semantic Hydration (JSON structural flattening & RDF triple synthesis into Ephemeral Graph)
//   Stage 4: Query & Inference Execution (Triple indexing, SPARQL-like BGP matching & Forward-Chaining rules)
func executeFederatedQuery(numISUs int, queryType string, expected map[int]ExpectedSensor) (s1, s2, s3, s4, integrity float64) {
	ctx := context.Background()

	// ----------------------------------------------------------------------------------
	// STAGE 1: Topological Traversal & Data Anchor Resolution (Semantic Plane)
	// ----------------------------------------------------------------------------------
	t0 := time.Now()
	var q string
	switch queryType {
	case "Q1_Selective":
		q = `{
			roots(func: eq(name, "Zone_0")) {
				uid
				name
				asset_category
				hasChild {
					uid
					name
					asset_category
					hasChild {
						uid
						name
						asset_category
						sub_type
						hasChild {
							uid
							name
							asset_category
							sub_type
							external_id
							isu_id
							max_threshold
						}
					}
				}
			}
		}`
	case "Q2_Filtering":
		q = `{
			roots(func: eq(sub_type, "Gearbox")) {
				uid
				name
				asset_category
				sub_type
				hasChild {
					uid
					name
					asset_category
					sub_type
					external_id
					isu_id
					max_threshold
				}
			}
		}`
	default:
		q = `{
			roots(func: eq(asset_category, "WindFarm")) {
				uid
				name
				asset_category
				hasChild {
					uid
					name
					asset_category
					hasChild {
						uid
						name
						asset_category
						hasChild {
							uid
							name
							asset_category
							sub_type
							hasChild {
								uid
								name
								asset_category
								sub_type
								external_id
								isu_id
								max_threshold
							}
						}
					}
				}
			}
		}`
	}

	resp, err := dgClient.NewTxn().Query(ctx, q)
	if err != nil {
		log.Printf("Stage 1 Anchor Resolution Error: %v", err)
		return 0, 0, 0, 0, 0
	}

	var decode struct {
		Roots []Thing `json:"roots"`
	}
	json.Unmarshal(resp.Json, &decode)

	// Extract Data Anchors grouped by ISU
	isuBatches := extractDataAnchors(decode.Roots, numISUs)
	s1 = sinceMs(t0)

	// ----------------------------------------------------------------------------------
	// STAGE 2: Parallel Batch State Retrieval across Independent Storage Units (Data Plane)
	// Semantic Wrappers return consolidated canonical JSON payloads per ISU batch
	// ----------------------------------------------------------------------------------
	t1 := time.Now()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var wrapperPayloads [][]byte

	for isuID, anchorIDs := range isuBatches {
		if len(anchorIDs) == 0 {
			continue
		}
		for start := 0; start < len(anchorIDs); start += ISUSubBatch {
			end := start + ISUSubBatch
			if end > len(anchorIDs) {
				end = len(anchorIDs)
			}
			chunk := anchorIDs[start:end]
			wg.Add(1)
			go func(targetISU int, ids []int32) {
				defer wg.Done()
				jsonBatch, err := fetchISUWrapperJSON(pgDBs[targetISU], ids)
				if err != nil {
					log.Printf("Stage 2 ISU %d Retrieval Error: %v", targetISU, err)
					return
				}
				mu.Lock()
				wrapperPayloads = append(wrapperPayloads, jsonBatch)
				mu.Unlock()
			}(isuID, chunk)
		}
	}
	wg.Wait()
	s2 = sinceMs(t1)

	// ----------------------------------------------------------------------------------
	// STAGE 3: Semantic Hydration (JSON Flattening & Ephemeral RDF Triple Synthesis)
	// Flattens the received canonical JSON objects, synthesizes dynamic state predicates,
	// and merges them with the topological triples into the unified ephemeral triple set.
	// ----------------------------------------------------------------------------------
	t2 := time.Now()
	hydratedState := make(map[int]float64, len(expected))
	for _, rawJSON := range wrapperPayloads {
		var records []wrapperRecord
		if err := json.Unmarshal(rawJSON, &records); err != nil {
			continue
		}
		for _, rec := range records {
			hydratedState[rec.ID] = rec.Reading
		}
	}
	triples := extractTopologicalTriples(decode.Roots, len(expected), hydratedState)
	s3 = sinceMs(t2)

	// ----------------------------------------------------------------------------------
	// STAGE 4: Semantic Query & Inference Execution over Enriched Ephemeral Triple Graph
	// Indexes the unified RDF triples into SPO/POS/OSP tables and executes SPARQL-like
	// BGP pattern matching, numeric filtering, and Forward-Chaining inference rules.
	// ----------------------------------------------------------------------------------
	t3 := time.Now()
	matched := runStage4SemanticEngine(queryType, triples, len(expected))
	s4 = sinceMs(t3)

	integrity = verifyIntegrity(queryType, matched, expected)
	return s1, s2, s3, s4, integrity
}

// extractDataAnchors walks the topological response from Dgraph to group (isu_id, external_id) anchors
func extractDataAnchors(roots []Thing, numISUs int) map[int][]int32 {
	batches := make(map[int][]int32, numISUs)
	var walk func(n *Thing)
	walk = func(n *Thing) {
		if n.AssetCategory == "Sensor" && n.ExternalID > 0 {
			batches[n.IsuID] = append(batches[n.IsuID], int32(n.ExternalID))
		}
		for i := range n.HasChild {
			walk(&n.HasChild[i])
		}
	}
	for i := range roots {
		walk(&roots[i])
	}
	return batches
}

// extractTopologicalTriples synthesizes canonical RDF triples from the KG hierarchy and merges
// either hydrated state values (Federated Stage 3) or Dgraph observations (Centralized).
func extractTopologicalTriples(roots []Thing, estSensors int, hydratedState map[int]float64) []RDFTriple {
	triples := make([]RDFTriple, 0, estSensors*5)

	var walk func(n *Thing)
	walk = func(n *Thing) {
		uri := "dt:" + n.Name
		triples = append(triples, RDFTriple{Subject: uri, Predicate: "rdf:type", Object: "dt:" + n.AssetCategory})
		if n.SubType != "" {
			triples = append(triples, RDFTriple{Subject: uri, Predicate: "dt:hasSubType", Object: "dt:" + n.SubType})
		}
		if n.AssetCategory == "Sensor" && n.ExternalID > 0 {
			triples = append(triples, RDFTriple{Subject: uri, Predicate: "dt:maxThreshold", Object: "threshold", NumValue: n.MaxThreshold})
			if hydratedState != nil {
				if v, ok := hydratedState[n.ExternalID]; ok {
					triples = append(triples, RDFTriple{Subject: uri, Predicate: "dt:hasDynamicValue", Object: "hydrated", NumValue: v})
				}
			} else if len(n.HasObservation) > 0 {
				v := n.HasObservation[0].Value
				triples = append(triples, RDFTriple{Subject: uri, Predicate: "dt:hasDynamicValue", Object: "centralized", NumValue: v})
			}
		}
		for i := range n.HasChild {
			childURI := "dt:" + n.HasChild[i].Name
			triples = append(triples, RDFTriple{Subject: uri, Predicate: "dt:hasChild", Object: childURI})
			walk(&n.HasChild[i])
		}
	}

	for i := range roots {
		walk(&roots[i])
	}
	return triples
}

// runStage4SemanticEngine loads the RDF triples into the embedded inference engine's SPO/POS/OSP
// indexes and executes SPARQL-equivalent BGP joins, filters, and Forward-Chaining rules.
func runStage4SemanticEngine(queryType string, triples []RDFTriple, estSensors int) int {
	g := NewEphemeralTripleGraph(estSensors)
	for i := range triples {
		t := &triples[i]
		isNum := t.Predicate == "dt:hasDynamicValue" || t.Predicate == "dt:maxThreshold"
		g.InsertTriple(t.Subject, t.Predicate, t.Object, t.NumValue, isNum)
	}

	switch queryType {
	case "Q1_Selective":
		// BGP Query:
		// SELECT ?sensor ?val WHERE {
		//   dt:Zone_0 dt:hasChild ?turb .
		//   ?turb     dt:hasChild ?comp .
		//   ?comp     dt:hasChild ?sensor .
		//   ?sensor   dt:hasDynamicValue ?val .
		// }
		turbs := g.SPO["dt:Zone_0"]["dt:hasChild"]
		for _, turbURI := range turbs {
			comps := g.SPO[turbURI]["dt:hasChild"]
			for _, compURI := range comps {
				sensors := g.SPO[compURI]["dt:hasChild"]
				for _, sensorURI := range sensors {
					if _, hasVal := g.NumericLit[sensorURI]["dt:hasDynamicValue"]; hasVal {
						g.Matched++
					}
				}
			}
		}

	case "Q2_Filtering":
		// BGP Query with Relational Join + Numeric Filter:
		// SELECT ?comp ?sensor ?val WHERE {
		//   ?comp   rdf:type dt:Component ;
		//           dt:hasSubType dt:Gearbox ;
		//           dt:hasChild ?sensor .
		//   ?sensor dt:hasDynamicValue ?val .
		//   FILTER (?val > 75.0)
		// }
		gearboxComps := g.POS["dt:hasSubType"]["dt:Gearbox"]
		for _, compURI := range gearboxComps {
			sensors := g.SPO[compURI]["dt:hasChild"]
			for _, sensorURI := range sensors {
				if val, ok := g.NumericLit[sensorURI]["dt:hasDynamicValue"]; ok && val > 75.0 {
					g.InsertTriple(sensorURI, "rdf:type", "dt:ThresholdExceededSensor", 0, false)
					g.Matched++
				}
			}
		}

	case "Q3_Inference":
		// Forward-Chaining Semantic Rule Reasoning & Fault Propagation:
		// Rule R1 (Anomaly Detection):
		//   (?s rdf:type dt:Sensor) ^ (?s dt:hasDynamicValue ?v) ^ (?s dt:maxThreshold ?th) ^ (?v > ?th)
		//   => (?s rdf:type dt:AnomalousSensor)
		// Rule R2 (Upward Component & Turbine Fault Propagation via inverse dt:hasChild):
		//   (?s rdf:type dt:AnomalousSensor) ^ (?comp dt:hasChild ?s) ^ (?turb dt:hasChild ?comp)
		//   => (?comp rdf:type dt:FaultyComponent) ^ (?turb rdf:type dt:MaintenanceRequiredTurbine)
		sensors := g.POS["rdf:type"]["dt:Sensor"]
		for _, sensorURI := range sensors {
			val, hasVal := g.NumericLit[sensorURI]["dt:hasDynamicValue"]
			th, hasTh := g.NumericLit[sensorURI]["dt:maxThreshold"]
			if hasVal && hasTh && val > th {
				g.InsertTriple(sensorURI, "rdf:type", "dt:AnomalousSensor", 0, false)
				g.Matched++

				for compURI := range g.OSP[sensorURI] {
					g.InsertTriple(compURI, "rdf:type", "dt:FaultyComponent", 0, false)
					for turbURI := range g.OSP[compURI] {
						g.InsertTriple(turbURI, "rdf:type", "dt:MaintenanceRequiredTurbine", 0, false)
					}
				}
			}
		}
	}
	return g.Matched
}

func verifyIntegrity(queryType string, matchedCount int, expected map[int]ExpectedSensor) float64 {
	if len(expected) == 0 {
		return 100.0
	}

	expectedMatches := 0
	for _, exp := range expected {
		switch queryType {
		case "Q1_Selective":
			if exp.ZoneName == "Zone_0" {
				expectedMatches++
			}
		case "Q2_Filtering":
			if exp.ComponentType == "Gearbox" && exp.Value > 75.0 {
				expectedMatches++
			}
		case "Q3_Inference":
			if exp.Value > exp.MaxThreshold {
				expectedMatches++
			}
		}
	}

	if expectedMatches == 0 {
		if matchedCount == 0 {
			return 100.0
		}
		return 0.0
	}

	diff := matchedCount - expectedMatches
	if diff < 0 {
		diff = -diff
	}
	acc := (1.0 - float64(diff)/float64(expectedMatches)) * 100.0
	if acc < 0 {
		acc = 0
	}
	return acc
}

// fetchISUWrapperJSON executes the Last Known Value (LKV) query on the specific ISU TSDB
// and serializes the result into the canonical JSON exchange schema of the Semantic Wrapper.
func fetchISUWrapperJSON(db *sql.DB, ids []int32) ([]byte, error) {
	query := `
        SELECT DISTINCT ON (sensor_id) sensor_id, payload
        FROM query_test
        WHERE sensor_id = ANY($1)
        ORDER BY sensor_id, time DESC
    `
	rows, err := db.Query(query, pq.Array(ids))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	localResults := make([]wrapperRecord, 0, len(ids))
	for rows.Next() {
		var rec wrapperRecord
		rec.Unit = "degC_or_mm_s"
		if err := rows.Scan(&rec.ID, &rec.Reading); err == nil {
			localResults = append(localResults, rec)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return json.Marshal(localResults)
}

// ======================================================================================
// SECTION III: INFRASTRUCTURE & HIERARCHICAL TOPOLOGY BUILDER
// ======================================================================================

func assignTargetISU(sensorIdx int, totalSensors int, numISUs int, partMode string) int {
	if numISUs <= 1 {
		return 0
	}
	switch partMode {
	case "ModerateSkew":
		if sensorIdx <= totalSensors/2 {
			return 0
		}
		return 1 + (sensorIdx % (numISUs - 1))
	case "HotspotSkew":
		if sensorIdx <= (totalSensors*80)/100 {
			return 0
		}
		return 1 + (sensorIdx % (numISUs - 1))
	default: // "Uniform"
		return (sensorIdx - 1) % numISUs
	}
}

// setupEnvironment builds a multi-hop hierarchy of independent Thing nodes linked via `hasChild`:
// WindFarm (Thing) -> 5 Zones (Thing) -> 50 Turbines (Thing) -> 200 Components (Thing) -> N Sensors (Thing)
// Only leaf Sensor Things have Data Anchors (isu_id, external_id); upper levels are purely topological Things.
func setupEnvironment(count int, numISUs int, partMode string) map[int]ExpectedSensor {
	ctx := context.Background()
	dgClient.Alter(ctx, &api.Operation{DropAll: true})
	for _, db := range pgDBs {
		db.Exec("TRUNCATE TABLE query_test")
	}

	dgClient.Alter(ctx, &api.Operation{Schema: `
		name: string @index(exact) .
		asset_category: string @index(exact) .
		sub_type: string @index(exact) .
		external_id: int @index(int) .
		isu_id: int @index(int) .
		max_threshold: float .
		value: float @index(float) .
		timestamp: datetime @index(hour) .
		hasChild: [uid] @reverse .
		hasObservation: [uid] .
		type Observation {
			timestamp
			value
		}
		type Thing {
			name
			asset_category
			sub_type
			external_id
			isu_id
			max_threshold
			value
			hasObservation
			hasChild
		}
	`})

	expected := make(map[int]ExpectedSensor, count)
	componentTypes := []string{"Gearbox", "Generator", "Rotor", "PitchControl"}
	sensorTypes := []string{"Temperature", "Vibration", "ActivePower", "RotationalSpeed"}

	const numZones = 5
	const turbinesPerZone = 10 // 50 turbines total
	const compsPerTurbine = 4  // 200 components total

	compSensors := make(map[int][]Thing, numZones*turbinesPerZone*compsPerTurbine)

	for i := 1; i <= count; i++ {
		compGlobalIdx := (i - 1) % (numZones * turbinesPerZone * compsPerTurbine)
		zoneIdx := compGlobalIdx / (turbinesPerZone * compsPerTurbine)
		turbGlobalIdx := compGlobalIdx / compsPerTurbine
		compLocalIdx := compGlobalIdx % compsPerTurbine

		zoneName := fmt.Sprintf("Zone_%d", zoneIdx)
		turbName := fmt.Sprintf("Turbine_%d", turbGlobalIdx)
		compType := componentTypes[compLocalIdx]
		sType := sensorTypes[i%len(sensorTypes)]

		val := 20.0 + float64((i*37)%800)/10.0
		threshold := 80.0
		isuID := assignTargetISU(i, count, numISUs, partMode)

		sensorThing := Thing{
			Uid:           fmt.Sprintf("_:sensor_%d", i),
			DType:         []string{"Thing"},
			Name:          fmt.Sprintf("Sensor_%d", i),
			AssetCategory: "Sensor",
			SubType:       sType,
			ExternalID:    i,
			IsuID:         isuID,
			MaxThreshold:  threshold,
		}
		compSensors[compGlobalIdx] = append(compSensors[compGlobalIdx], sensorThing)

		expected[i] = ExpectedSensor{
			ExternalID:    i,
			IsuID:         isuID,
			ZoneName:      zoneName,
			TurbineName:   turbName,
			ComponentType: compType,
			SensorType:    sType,
			MaxThreshold:  threshold,
			Value:         val,
		}
	}

	zones := make([]Thing, 0, numZones)
	for z := 0; z < numZones; z++ {
		turbines := make([]Thing, 0, turbinesPerZone)
		for t := 0; t < turbinesPerZone; t++ {
			turbGlobalIdx := z*turbinesPerZone + t
			comps := make([]Thing, 0, compsPerTurbine)
			for c := 0; c < compsPerTurbine; c++ {
				compGlobalIdx := turbGlobalIdx*compsPerTurbine + c
				compType := componentTypes[c]
				compThing := Thing{
					Uid:           fmt.Sprintf("_:comp_%d", compGlobalIdx),
					DType:         []string{"Thing"},
					Name:          fmt.Sprintf("Comp_%d_%s", compGlobalIdx, compType),
					AssetCategory: "Component",
					SubType:       compType,
					HasChild:      compSensors[compGlobalIdx],
				}
				comps = append(comps, compThing)
			}
			turbThing := Thing{
				Uid:           fmt.Sprintf("_:turb_%d", turbGlobalIdx),
				DType:         []string{"Thing"},
				Name:          fmt.Sprintf("Turbine_%d", turbGlobalIdx),
				AssetCategory: "Turbine",
				HasChild:      comps,
			}
			turbines = append(turbines, turbThing)
		}
		zoneThing := Thing{
			Uid:           fmt.Sprintf("_:zone_%d", z),
			DType:         []string{"Thing"},
			Name:          fmt.Sprintf("Zone_%d", z),
			AssetCategory: "Zone",
			HasChild:      turbines,
		}
		zones = append(zones, zoneThing)
	}

	windFarmRoot := Thing{
		Uid:           "_:windfarm_0",
		DType:         []string{"Thing"},
		Name:          "WindFarm_Alpha",
		AssetCategory: "WindFarm",
		HasChild:      zones,
	}

	jsonBytes, err := json.Marshal(windFarmRoot)
	if err != nil {
		log.Fatalf("Failed to marshal hierarchical Thing tree: %v", err)
	}
	if _, err := dgClient.NewTxn().Mutate(ctx, &api.Mutation{SetJson: jsonBytes, CommitNow: true}); err != nil {
		log.Fatalf("Failed to mutate hierarchical Thing tree in Dgraph: %v", err)
	}

	return expected
}

func performUpdates(arch string, numISUs int, data map[int]ExpectedSensor) {
	ctx := context.Background()
	baseTime := time.Now().UTC().Add(-time.Duration(HistorySamples) * time.Second)

	if arch == "Centralized" {
		q := `{ me(func: has(external_id)) { uid, external_id } }`
		resp, _ := dgClient.NewTxn().Query(ctx, q)
		var decode struct {
			Me []struct {
				Uid        string `json:"uid"`
				ExternalID int    `json:"external_id"`
			} `json:"me"`
		}
		json.Unmarshal(resp.Json, &decode)

		var mutations []Thing
		for _, node := range decode.Me {
			if item, ok := data[node.ExternalID]; ok {
				obsList := make([]Observation, HistorySamples)
				for h := 0; h < HistorySamples-1; h++ {
					obsList[h] = Observation{
						DType:     []string{"Observation"},
						Timestamp: baseTime.Add(time.Duration(h) * time.Second),
						Value:     item.Value - 1.0,
					}
				}
				obsList[HistorySamples-1] = Observation{
					DType:     []string{"Observation"},
					Timestamp: baseTime.Add(time.Duration(HistorySamples) * time.Second),
					Value:     item.Value,
				}
				mutations = append(mutations, Thing{
					Uid:            node.Uid,
					HasObservation: obsList,
				})
			}
		}
		batchSize := 200
		for i := 0; i < len(mutations); i += batchSize {
			end := i + batchSize
			if end > len(mutations) {
				end = len(mutations)
			}
			jsonBytes, _ := json.Marshal(mutations[i:end])
			dgClient.NewTxn().Mutate(ctx, &api.Mutation{SetJson: jsonBytes, CommitNow: true})
		}
	} else {
		perISU := make(map[int][]ExpectedSensor, numISUs)
		for _, item := range data {
			perISU[item.IsuID] = append(perISU[item.IsuID], item)
		}

		var wg sync.WaitGroup
		for isuID, items := range perISU {
			wg.Add(1)
			go func(targetISU int, records []ExpectedSensor) {
				defer wg.Done()
				tx, err := pgDBs[targetISU].Begin()
				if err != nil {
					log.Fatalf("Failed to begin tx on ISU %d: %v", targetISU, err)
				}
				stmtIns, err := tx.Prepare(pq.CopyIn("query_test", "time", "sensor_id", "payload"))
				if err != nil {
					log.Fatalf("Failed to prepare CopyIn on ISU %d: %v", targetISU, err)
				}
				for _, rec := range records {
					for h := 0; h < HistorySamples-1; h++ {
						stmtIns.Exec(baseTime.Add(time.Duration(h)*time.Second), rec.ExternalID, rec.Value-1.0)
					}
					stmtIns.Exec(baseTime.Add(time.Duration(HistorySamples)*time.Second), rec.ExternalID, rec.Value)
				}
				if _, err := stmtIns.Exec(); err != nil {
					log.Fatalf("Failed to flush CopyIn on ISU %d: %v", targetISU, err)
				}
				stmtIns.Close()
				tx.Commit()
				pgDBs[targetISU].Exec("ANALYZE query_test;")
			}(isuID, items)
		}
		wg.Wait()
	}
}

func initInfrastructure() {
	conn, err := grpc.Dial(cfg.DgraphURI, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Semantic Plane Init Failed: %v", err)
	}
	dgClient = dgo.NewDgraphClient(api.NewDgraphClient(conn))

	numISUs := len(cfg.Timescale.Ports)
	pgDBs = make([]*sql.DB, numISUs)

	for i := 0; i < numISUs; i++ {
		db, err := sql.Open("postgres", cfg.PostgresConnForISU(i))
		if err != nil {
			log.Fatalf("Data Plane ISU %d Init Failed: %v", i, err)
		}
		db.SetMaxOpenConns(50)
		db.SetMaxIdleConns(25)
		db.SetConnMaxLifetime(5 * time.Minute)

		db.Exec("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
		db.Exec("DROP TABLE IF EXISTS query_test;")
		db.Exec(`CREATE TABLE query_test (
			time TIMESTAMPTZ NOT NULL,
			sensor_id INT,
			payload DOUBLE PRECISION
		);`)
		db.Exec("SELECT create_hypertable('query_test', 'time');")
		db.Exec("CREATE INDEX ON query_test (sensor_id, time DESC);")

		pgDBs[i] = db
	}
}

func closeInfrastructure() {
	for _, db := range pgDBs {
		if db != nil {
			db.Close()
		}
	}
}

func initCSVWriter() *csv.Writer {
	os.MkdirAll("output", 0755)
	file, err := os.Create(OutputCSV)
	if err != nil {
		log.Fatalf("Failed to create CSV: %v", err)
	}
	writer := csv.NewWriter(file)
	writer.Write([]string{
		"Experiment_Type",
		"Num_Sensors",
		"Architecture",
		"Num_ISUs",
		"Partition_Mode",
		"Query_Type",
		"Concurrency",
		"Run_ID",
		"Total_Latency_ms",
		"Stage1_Anchor_ms",
		"Stage2_ISU_IO_ms",
		"Stage3_Hydration_ms",
		"Stage4_QueryExec_ms",
		"Success_Rate_Pct",
		"Memory_Pressure_MB_per_Query",
	})
	writer.Flush()
	return writer
}

func logResult(w *csv.Writer, m QueryMetrics) {
	w.Write([]string{
		m.ExperimentType,
		strconv.Itoa(m.NumSensors),
		m.Architecture,
		strconv.Itoa(m.NumISUs),
		m.PartitionMode,
		m.QueryType,
		strconv.Itoa(m.Concurrency),
		strconv.Itoa(m.RunID),
		fmt.Sprintf("%.4f", m.TotalLatency),
		fmt.Sprintf("%.4f", m.Stage1AnchorMs),
		fmt.Sprintf("%.4f", m.Stage2ISUIOMs),
		fmt.Sprintf("%.4f", m.Stage3HydrMs),
		fmt.Sprintf("%.4f", m.Stage4QueryMs),
		fmt.Sprintf("%.2f", m.SuccessRate),
		fmt.Sprintf("%.4f", m.MemAllocMB),
	})
	w.Flush()
}
