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
	Value          float64       `json:"value,omitempty"`          // Dynamic telemetry state (hydrated on-the-fly)
	HasObservation []Observation `json:"hasObservation,omitempty"` // Time-series observations in Centralized KG
	Status         string        `json:"status,omitempty"`         // Synthesized/inferred semantic state
	HasChild       []Thing       `json:"hasChild,omitempty"`       // Topological hierarchical relationships
}

// EphemeralGraph holds the hydrated in-memory graph and inferred triples during Stage 3 & 4
type EphemeralGraph struct {
	Roots            []Thing           // Hierarchical tree of Things
	SensorIndex      map[int]*Thing    // Pointers to articulated leaf Thing nodes inside the tree
	ParentComponent  map[int]*Thing    // Sensor ExternalID -> Parent Component Thing
	ParentTurbine    map[int]*Thing    // Sensor ExternalID -> Grandparent Turbine Thing
	FaultyComponents map[string]string // Component Name -> Inferred Fault Type
	TurbineAlerts    map[string]int    // Turbine Name -> Count of Critical Subsystem Faults
	MatchedCount     int
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
	Stage3HydrMs   float64 // Stage 3: Semantic Hydration in ephemeral graph (ms)
	Stage4QueryMs  float64 // Stage 4: End-to-End Semantic Query / Inference Execution (ms)
	SuccessRate    float64 // Data Integrity percentage (0-100%)
	MemAllocMB     float64 // RAM allocated per query (MB)
}

type stateRecord struct {
	ID  int
	Val float64
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

// executeCentralizedQuery traverses the multi-hop hasChild relationships in the monolithic KG
// and resolves the Last Known Value (LKV) via `hasObservation(orderdesc: timestamp, first: 1)`,
// which is the exact graph equivalent of `SELECT DISTINCT ON (sensor_id) ... ORDER BY time DESC`.
func executeCentralizedQuery(queryType string, expected map[int]ExpectedSensor) (s1, s2, s3, s4, integrity float64) {
	ctx := context.Background()
	t0 := time.Now()

	var q string
	switch queryType {
	case "Q1_Selective":
		// Multi-hop path traversal starting from topological Zone_0 Thing down to Sensor Things via hasChild
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
		// Traverse from Gearbox Component Things through hasChild to Sensor Things and get LKV
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
		// Full multi-hop hierarchy traversal from WindFarm root Thing down to Sensor Things with LKV
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

	// Stage 4: Build index & run semantic query / fault propagation over the retrieved hierarchy
	t4 := time.Now()
	eph := indexHierarchy(decode.Roots)
	runStage4Execution(queryType, &eph)
	s4 = sinceMs(t4)

	integrity = verifyIntegrity(queryType, eph, expected)
	return s1, 0, 0, s4, integrity
}

// executeFederatedQuery implements the 4-Stage Orchestration Sequence over multi-hop Thing hierarchies
func executeFederatedQuery(numISUs int, queryType string, expected map[int]ExpectedSensor) (s1, s2, s3, s4, integrity float64) {
	ctx := context.Background()

	// ----------------------------------------------------------------------------------
	// STAGE 1: Topological Traversal & Data Anchor Resolution (Semantic Plane)
	// Traverses white topological Thing nodes via hasChild to discover black IoT Thing nodes
	// ----------------------------------------------------------------------------------
	t0 := time.Now()
	var q string
	switch queryType {
	case "Q1_Selective":
		// Selective subgraph resolution starting at Zone_0 Thing and following hasChild edges
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
		// Symmetric topological resolution for Gearbox Component Things and their child Sensor Things
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
		// Full Digital Twin hierarchy traversal: WindFarm -> Zone -> Turbine -> Component -> Sensor
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

	// Index the topological tree and extract Data Anchors from articulated Sensor nodes
	eph := indexHierarchy(decode.Roots)
	isuBatches := make(map[int][]int32, numISUs)
	for extID, sensorPtr := range eph.SensorIndex {
		isuBatches[sensorPtr.IsuID] = append(isuBatches[sensorPtr.IsuID], int32(extID))
	}
	s1 = sinceMs(t0)

	// ----------------------------------------------------------------------------------
	// STAGE 2: Parallel Batch State Retrieval across Independent Storage Units (Data Plane)
	// ----------------------------------------------------------------------------------
	t1 := time.Now()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var resultsChunks [][]stateRecord

	for isuID, anchorIDs := range isuBatches {
		if len(anchorIDs) == 0 {
			continue
		}
		// Sub-batch parallel retrieval per ISU to avoid single-query array bottleneck
		for start := 0; start < len(anchorIDs); start += ISUSubBatch {
			end := start + ISUSubBatch
			if end > len(anchorIDs) {
				end = len(anchorIDs)
			}
			chunk := anchorIDs[start:end]
			wg.Add(1)
			go func(targetISU int, ids []int32) {
				defer wg.Done()
				recs, err := fetchISUBatch(pgDBs[targetISU], ids)
				if err != nil {
					log.Printf("Stage 2 ISU %d Retrieval Error: %v", targetISU, err)
					return
				}
				mu.Lock()
				resultsChunks = append(resultsChunks, recs)
				mu.Unlock()
			}(isuID, chunk)
		}
	}
	wg.Wait()
	s2 = sinceMs(t1)

	// ----------------------------------------------------------------------------------
	// STAGE 3: Semantic Hydration (In-Memory Ephemeral Graph Enrichment)
	// Injects dynamic state triples directly into the articulated Thing nodes in memory
	// ----------------------------------------------------------------------------------
	t2 := time.Now()
	for _, recs := range resultsChunks {
		for _, rec := range recs {
			if sensorNode, exists := eph.SensorIndex[rec.ID]; exists {
				sensorNode.Value = rec.Val
				sensorNode.Status = "Hydrated"
			}
		}
	}
	s3 = sinceMs(t2)

	// ----------------------------------------------------------------------------------
	// STAGE 4: Semantic Query & Inference Execution over Hydrated Ephemeral Graph
	// ----------------------------------------------------------------------------------
	t3 := time.Now()
	runStage4Execution(queryType, &eph)
	s4 = sinceMs(t3)

	integrity = verifyIntegrity(queryType, eph, expected)
	return s1, s2, s3, s4, integrity
}

// indexHierarchy recursively walks the `hasChild` tree of Thing nodes, indexing articulated
// leaf Sensor Things (which hold Data Anchors) and linking their parent Component and Turbine Things.
func indexHierarchy(roots []Thing) EphemeralGraph {
	eph := EphemeralGraph{
		Roots:            roots,
		SensorIndex:      make(map[int]*Thing, 1024),
		ParentComponent:  make(map[int]*Thing, 1024),
		ParentTurbine:    make(map[int]*Thing, 1024),
		FaultyComponents: make(map[string]string),
		TurbineAlerts:    make(map[string]int),
	}

	var walk func(node *Thing, curTurbine *Thing, curComponent *Thing)
	walk = func(node *Thing, curTurbine *Thing, curComponent *Thing) {
		switch node.AssetCategory {
		case "Turbine":
			curTurbine = node
		case "Component":
			curComponent = node
		case "Sensor":
			if node.ExternalID > 0 {
				// If populated via Centralized hasObservation LKV query, extract latest observation value
				if len(node.HasObservation) > 0 {
					node.Value = node.HasObservation[0].Value
				}
				eph.SensorIndex[node.ExternalID] = node
				if curComponent != nil {
					eph.ParentComponent[node.ExternalID] = curComponent
				}
				if curTurbine != nil {
					eph.ParentTurbine[node.ExternalID] = curTurbine
				}
			}
		}
		for i := range node.HasChild {
			walk(&node.HasChild[i], curTurbine, curComponent)
		}
	}

	for i := range eph.Roots {
		walk(&eph.Roots[i], nil, nil)
	}
	return eph
}

// runStage4Execution executes path-based filtering or upward fault-propagation reasoning over the Thing tree
func runStage4Execution(queryType string, eph *EphemeralGraph) {
	switch queryType {
	case "Q1_Selective":
		// Count all hydrated Sensor Things discovered under Zone_0's hasChild hierarchy
		for _, s := range eph.SensorIndex {
			if s.ExternalID > 0 {
				eph.MatchedCount++
			}
		}

	case "Q2_Filtering":
		// Multi-hop relational + state filtering:
		// Select Sensor Things whose parent Component Thing is a "Gearbox" and dynamic Value > 75.0
		for extID, s := range eph.SensorIndex {
			comp := eph.ParentComponent[extID]
			if comp != nil && comp.SubType == "Gearbox" && s.Value > 75.0 {
				s.Status = "ThresholdExceeded"
				eph.MatchedCount++
			}
		}

	case "Q3_Inference":
		// Upward Semantic Rule Reasoning across `hasChild` hierarchy:
		// Rule: Thing(s, Sensor) ^ value(s) > max_threshold(s) ^ hasChild(comp, s) ^ hasChild(turb, comp)
		//       => Status(comp) = "Faulty" ^ Status(turb) = "RequiresMaintenance"
		for extID, s := range eph.SensorIndex {
			if s.Value > s.MaxThreshold {
				s.Status = "InferredAnomaly"
				if comp := eph.ParentComponent[extID]; comp != nil {
					comp.Status = "FaultyComponent"
					eph.FaultyComponents[comp.Name] = "Critical_" + s.SubType
				}
				if turb := eph.ParentTurbine[extID]; turb != nil {
					turb.Status = "RequiresMaintenance"
					eph.TurbineAlerts[turb.Name]++
				}
				eph.MatchedCount++
			} else {
				s.Status = "Nominal"
			}
		}
	}
}

func verifyIntegrity(queryType string, eph EphemeralGraph, expected map[int]ExpectedSensor) float64 {
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
		if eph.MatchedCount == 0 {
			return 100.0
		}
		return 0.0
	}

	diff := eph.MatchedCount - expectedMatches
	if diff < 0 {
		diff = -diff
	}
	acc := (1.0 - float64(diff)/float64(expectedMatches)) * 100.0
	if acc < 0 {
		acc = 0
	}
	return acc
}

// Executes the Last Known Value (LKV) query on the specific ISU TSDB
func fetchISUBatch(db *sql.DB, ids []int32) ([]stateRecord, error) {
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

	localResults := make([]stateRecord, 0, len(ids))
	for rows.Next() {
		var rec stateRecord
		if err := rows.Scan(&rec.ID, &rec.Val); err == nil {
			localResults = append(localResults, rec)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return localResults, nil
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
		// 50% of sensors routed to ISU 0, remaining 50% distributed across ISUs 1..N-1
		if sensorIdx <= totalSensors/2 {
			return 0
		}
		return 1 + (sensorIdx % (numISUs - 1))
	case "HotspotSkew":
		// 80% of sensors concentrated in ISU 0 (extreme straggler hotspot)
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

	// Build sensors grouped by component index [0..199]
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

		// Deterministic synthetic telemetry value in [20.0, 100.0]
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

	// Construct the hierarchical tree of topological Thing nodes (WindFarm -> Zone -> Turbine -> Component -> Sensor)
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

// performUpdates injects a realistic time-series window (HistorySamples = 20 observations per sensor)
// into either the Centralized KG (as timestamped Observation nodes) or the Federated TSDB ISUs.
// Both systems must then resolve the exact same Last Known Value (LKV) query at runtime.
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
				// Latest observation at the most recent timestamp
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
		batchSize := 250
		for i := 0; i < len(mutations); i += batchSize {
			end := i + batchSize
			if end > len(mutations) {
				end = len(mutations)
			}
			jsonBytes, _ := json.Marshal(mutations[i:end])
			dgClient.NewTxn().Mutate(ctx, &api.Mutation{SetJson: jsonBytes, CommitNow: true})
		}
	} else {
		// Group updates by target ISU and bulk insert the exact same 20 timestamped observations per sensor
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
					// Latest record at most recent timestamp
					stmtIns.Exec(baseTime.Add(time.Duration(HistorySamples)*time.Second), rec.ExternalID, rec.Value)
				}
				if _, err := stmtIns.Exec(); err != nil {
					log.Fatalf("Failed to flush CopyIn on ISU %d: %v", targetISU, err)
				}
				stmtIns.Close()
				tx.Commit()
				// Update PostgreSQL statistics so index scan on (sensor_id, time DESC) is always chosen
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
