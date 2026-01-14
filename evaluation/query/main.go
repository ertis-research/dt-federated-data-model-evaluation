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
	OutputCSV      = "output/query_evaluation_results.csv"
	RunsPerTest    = 10                     // Total valid measurement runs per scenario
	WarmUpRuns     = 3                      // Warm-up runs to stabilize connection pools/JIT
	BatchSize      = 500                    // Batch size for parallel ISU retrieval
	SimulatedDelay = 100 * time.Millisecond // Inter-run delay to prevent CPU/IO saturation
)

// Global Shared State
var (
	cfg      config.Env
	pgDB     *sql.DB     // Data Plane
	dgClient *dgo.Dgraph // Semantic Plane
)

// Domain Entity: Represents a node in the KG
type Thing struct {
	Uid        string   `json:"uid,omitempty"`
	DType      []string `json:"dgraph.type,omitempty"`
	Name       string   `json:"name,omitempty"`
	ExternalID int      `json:"external_id,omitempty"` // The "Data Anchor" linking Graph to TSDB
	Value      string   `json:"value,omitempty"`       // The dynamic state
}

type QueryMetrics struct {
	RunID        int
	TotalLatency float64 // End-to-end execution time (ms)
	IoWaitTime   float64 // Time spent waiting for Database I/O (ms)
	CpuTime      float64 // Time spent on Orchestration/Hydration logic (ms)
	SuccessRate  float64 // Data Integrity percentage (0-100%)
}

// ======================================================================================
// SECTION I: EXPERIMENT CONTROL (MAIN LOOP)
// ======================================================================================

func main() {
	log.SetFlags(0)
	cfg = config.LoadEnv()

	initInfrastructure()
	defer pgDB.Close()

	writer := initCSVWriter()
	defer writer.Flush()

	// Scenarios
	sensorCounts := []int{100, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000}
	architectures := []string{"Centralized", "Federated"}

	fmt.Printf("Starting QUERY EVALUATION (WarmUp: %d, Runs: %d)...\n", WarmUpRuns, RunsPerTest)

	for _, count := range sensorCounts {
		for _, arch := range architectures {
			evaluateScenario(writer, arch, count)
		}
	}
	fmt.Println("\nEvaluation Complete. Raw data saved to", OutputCSV)
}

func evaluateScenario(writer *csv.Writer, arch string, count int) {
	fmt.Printf("\n--- Testing Architecture: %s | Sensors: %d ---\n", arch, count)

	// Prepare the environment state
	fmt.Print("   > Setting up environment... ")
	expectedValues := setupEnvironment(count)
	performUpdates(arch, expectedValues)
	fmt.Println("Ready.")

	// Execute runs and log data immediately
	performBenchmarkSeries(writer, arch, count, expectedValues)
}

// executes the warm-up and measurement loops, logging every run.
func performBenchmarkSeries(writer *csv.Writer, arch string, count int, expectedValues map[int]string) {

	// --- PHASE 1: WARM-UP ---
	// Executes queries without logging to stabilize system caches and connection pools.
	if WarmUpRuns > 0 {
		fmt.Print("   > Warming up: ")
		for w := 0; w < WarmUpRuns; w++ {
			fetchDigitalTwinState(arch) // Discard result
			fmt.Print("~")
			time.Sleep(SimulatedDelay)
		}
		fmt.Println(" Done.")
	}

	// --- PHASE 2: MEASUREMENT ---
	fmt.Println("   > Measuring runs:")

	for r := 1; r <= RunsPerTest; r++ {
		time.Sleep(SimulatedDelay)

		startTotal := time.Now()

		// === ARCHITECTURAL ENTRY POINT ===
		// Retrieves the reconstructed DT state
		graph, ioTime, cpuTime := fetchDigitalTwinState(arch)

		totalDuration := time.Since(startTotal).Seconds() * 1000

		// Verification of data consistency
		correctCount := countCorrectNodes(expectedValues, graph)
		successRate := 100.0
		if len(expectedValues) > 0 {
			successRate = (float64(correctCount) / float64(len(expectedValues))) * 100.0
		}

		if correctCount != len(expectedValues) {
			log.Printf("     [!] Consistency Violation in Run %d: Got %d/%d", r, correctCount, len(expectedValues))
		}

		metrics := QueryMetrics{
			RunID:        r,
			TotalLatency: totalDuration,
			IoWaitTime:   ioTime,
			CpuTime:      cpuTime,
			SuccessRate:  successRate,
		}

		fmt.Printf("     Run %d: Latency=%.2fms | I/O=%.2fms | Overhead=%.2fms | Success=%.2f%%\n",
			r, metrics.TotalLatency, metrics.IoWaitTime, metrics.CpuTime, metrics.SuccessRate)

		logResult(writer, count, arch, metrics)
	}
}

// ======================================================================================
// SECTION II: FEDERATED ORCHESTRATION ENGINE
// ======================================================================================

// orchestrates the retrieval process based on the architecture.
func fetchDigitalTwinState(arch string) ([]Thing, float64, float64) {
	var ioDuration, cpuDuration float64

	// Step 1: Data Anchor Resolution
	// The orchestrator queries the KG to understand the asset structure
	t0 := time.Now()
	topology, err := resolveTopology()
	if err != nil {
		log.Fatalf("Topology Resolution Failed: %v", err)
	}
	ioDuration += time.Since(t0).Seconds() * 1000

	// Architectural Decision Point
	if arch == "Centralized" {
		// In the monolithic approach, state is co-located with topology.
		// Retrieval is a single I/O operation. Overhead is negligible.
		return topology, ioDuration, 0
	}

	// Step 2 & 3: State Retrieval & Semantic Hydration (Federated)
	t1 := time.Now()

	// enrichFederatedStateMetrics handles the parallel retrieval and in-memory fusion.
	enrichedGraph, maxBatchIOTime := enrichFederatedStateMetrics(topology)

	totalFederatedTime := time.Since(t1).Seconds() * 1000

	// Overhead Calculation:
	// Total Time = (Waiting for slowest DB batch) + (CPU time for Hydration/Marshalling)
	ioDuration += maxBatchIOTime

	cpuDuration = totalFederatedTime - maxBatchIOTime
	if cpuDuration < 0 {
		cpuDuration = 0
	}

	return enrichedGraph, ioDuration, cpuDuration
}

// queries the Dgraph KG to retrieve active assets and Data Anchors.
func resolveTopology() ([]Thing, error) {
	ctx := context.Background()
	q := `
	{
		things(func: type(Thing)) {
			external_id
			value
			dgraph.type
			uid
			name
		}
	}`
	resp, err := dgClient.NewTxn().Query(ctx, q)
	if err != nil {
		return nil, err
	}

	var decode struct {
		Things []Thing `json:"things"`
	}
	if err := json.Unmarshal(resp.Json, &decode); err != nil {
		return nil, err
	}
	return decode.Things, nil
}

// implements the lazy evaluation strategy
// It performs parallel batch retrieval and Semantic Hydration
func enrichFederatedStateMetrics(topology []Thing) ([]Thing, float64) {
	var mu sync.Mutex
	var wg sync.WaitGroup
	var maxIOTime float64 // Tracks the bottleneck batch

	// Graph Indexing
	// Simulates loading the KG topology into an efficient memory structure for hydration
	nodeIndex := make(map[int]*Thing, len(topology))
	ids := make([]int32, len(topology))
	for i := range topology {
		nodeIndex[topology[i].ExternalID] = &topology[i]
		ids[i] = int32(topology[i].ExternalID)
	}

	// Parallel Batch
	for i := 0; i < len(ids); i += BatchSize {
		end := i + BatchSize
		if end > len(ids) {
			end = len(ids)
		}

		chunkIDs := ids[i:end]
		wg.Add(1)

		go func(batch []int32) {
			defer wg.Done()

			tStartDB := time.Now()
			batchResults, err := fetchISUBatch(batch)
			durDB := time.Since(tStartDB).Seconds() * 1000

			if err != nil {
				log.Printf("ISU Batch Retrieval Error: %v", err)
				return
			}

			mu.Lock()
			if durDB > maxIOTime {
				maxIOTime = durDB
			}

			// Semantic Hydration
			// Injection of operational state into the semantic graph nodes
			for id, val := range batchResults {
				if node, exists := nodeIndex[id]; exists {
					node.Value = val
				}
			}
			mu.Unlock()
		}(chunkIDs)
	}

	wg.Wait()
	return topology, maxIOTime
}

// Executes the last known value query on the TSDB
// This simulates the semantic wrapper abstraction layer
func fetchISUBatch(ids []int32) (map[int]string, error) {
	query := `
		SELECT DISTINCT ON (sensor_id) sensor_id, payload
		FROM query_test
		WHERE sensor_id = ANY($1)
		ORDER BY sensor_id, time DESC
	`
	rows, err := pgDB.Query(query, pq.Array(ids))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	localResults := make(map[int]string)
	for rows.Next() {
		var id int
		var val string
		if err := rows.Scan(&id, &val); err == nil {
			localResults[id] = val
		}
	}
	return localResults, nil
}

// ======================================================================================
// SECTION III: INFRASTRUCTURE & HELPERS
// ======================================================================================

func initInfrastructure() {
	// Dgraph Connection Setup
	conn, err := grpc.Dial(cfg.DgraphURI, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Semantic Plane Init Failed: %v", err)
	}
	dgClient = dgo.NewDgraphClient(api.NewDgraphClient(conn))

	// Postgres Connection Setup
	pgDB, err = sql.Open("postgres", cfg.PostgresConn())
	if err != nil {
		log.Fatalf("Data Plane Init Failed: %v", err)
	}

	// Connection Pool Tuning for High Concurrency
	pgDB.SetMaxOpenConns(50)
	pgDB.SetMaxIdleConns(25)
	pgDB.SetConnMaxLifetime(5 * time.Minute)

	initDBSchema()
}

func initDBSchema() {
	pgDB.Exec("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
	pgDB.Exec("DROP TABLE IF EXISTS query_test;")
	pgDB.Exec(`CREATE TABLE query_test (
		time TIMESTAMPTZ NOT NULL,
		sensor_id INT,
		payload TEXT
	);`)
	pgDB.Exec("SELECT create_hypertable('query_test', 'time');")
	pgDB.Exec("CREATE INDEX ON query_test (sensor_id, time DESC);")
}

func initCSVWriter() *csv.Writer {
	file, err := os.Create(OutputCSV)
	if err != nil {
		log.Fatalf("Failed to create CSV: %v", err)
	}
	writer := csv.NewWriter(file)
	writer.Write([]string{
		"Num_Sensors",
		"Architecture",
		"Run_ID",
		"Total_Latency_ms",
		"IO_Wait_ms",
		"Orchestration_Overhead_ms",
		"Success_Rate_Pct",
	})
	writer.Flush()
	return writer
}

func logResult(w *csv.Writer, count int, arch string, m QueryMetrics) {
	w.Write([]string{
		strconv.Itoa(count),
		arch,
		strconv.Itoa(m.RunID),
		fmt.Sprintf("%.4f", m.TotalLatency),
		fmt.Sprintf("%.4f", m.IoWaitTime),
		fmt.Sprintf("%.4f", m.CpuTime),
		fmt.Sprintf("%.2f", m.SuccessRate),
	})
	w.Flush()
}

// prepares the topological structure for the experiment
func setupEnvironment(count int) map[int]string {
	ctx := context.Background()
	dgClient.Alter(ctx, &api.Operation{DropAll: true})
	pgDB.Exec("TRUNCATE TABLE query_test")

	// Define Schema
	dgClient.Alter(ctx, &api.Operation{Schema: `
		external_id: int @index(int) .
		value: string .
		type Thing {
			external_id
			value
		}
	`})

	expected := make(map[int]string)
	var nodes []Thing

	// Generate synthetic data
	for i := 1; i <= count; i++ {
		val := fmt.Sprintf("val_%d_%d", i, rand.Intn(9999))
		expected[i] = val
		node := Thing{
			Uid:        fmt.Sprintf("_:n%d", i),
			DType:      []string{"Thing"},
			Name:       fmt.Sprintf("Sensor %d", i),
			ExternalID: i,
		}
		nodes = append(nodes, node)
	}

	// Topology injection
	batchSize := 1000
	for i := 0; i < len(nodes); i += batchSize {
		end := i + batchSize
		if end > len(nodes) {
			end = len(nodes)
		}
		jsonBytes, _ := json.Marshal(nodes[i:end])
		dgClient.NewTxn().Mutate(ctx, &api.Mutation{SetJson: jsonBytes, CommitNow: true})
	}
	return expected
}

// injects the state values into the appropriate data store based on architecture
func performUpdates(arch string, data map[int]string) {
	ctx := context.Background()
	if arch == "Centralized" {
		// Centralized: Update KG directly
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
			if val, ok := data[node.ExternalID]; ok {
				mutations = append(mutations, Thing{Uid: node.Uid, Value: val})
			}
		}
		// Batch mutation
		batchSize := 1000
		for i := 0; i < len(mutations); i += batchSize {
			end := i + batchSize
			if end > len(mutations) {
				end = len(mutations)
			}
			jsonBytes, _ := json.Marshal(mutations[i:end])
			dgClient.NewTxn().Mutate(ctx, &api.Mutation{SetJson: jsonBytes, CommitNow: true})
		}

	} else {
		// Federated: Insert into TSDB
		tx, _ := pgDB.Begin()
		stmtIns, _ := tx.Prepare("INSERT INTO query_test (time, sensor_id, payload) VALUES (NOW(), $1, $2)")
		defer stmtIns.Close()

		for id, val := range data {
			stmtIns.Exec(id, val)
		}
		tx.Commit()
	}
}

// validates the integrity of the hydrated graph against expected values
func countCorrectNodes(expected map[int]string, graph []Thing) int {
	correct := 0
	for _, node := range graph {
		if expVal, ok := expected[node.ExternalID]; ok {
			if node.Value == expVal {
				correct++
			}
		}
	}
	return correct
}
