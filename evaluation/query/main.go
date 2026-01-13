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
	RunsPerTest    = 10
	BatchSize      = 1000                   // Batch size for parallel ISU retrieval
	SimulatedDelay = 100 * time.Millisecond // Throttle to prevent CPU saturation
)

// Global Shared State
var (
	cfg      config.Env
	pgDB     *sql.DB     // Data Plane (ISU)
	dgClient *dgo.Dgraph // Semantic Plane (Topology)
)

// Domain Entity
type Thing struct {
	Uid        string   `json:"uid,omitempty"`
	DType      []string `json:"dgraph.type,omitempty"`
	Name       string   `json:"name,omitempty"`
	ExternalID int      `json:"external_id,omitempty"` // The "Data Anchor"
	Value      string   `json:"value,omitempty"`
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

	// Scalability Scenarios
	sensorCounts := []int{100, 1000, 5000, 10000, 25000, 50000}
	architectures := []string{"Centralized", "Federated"}

	fmt.Printf("Starting QUERY EVALUATION (%d runs average)...\n", RunsPerTest)

	for _, count := range sensorCounts {
		for _, arch := range architectures {
			evaluateScenario(writer, arch, count)
		}
	}
	fmt.Println("\nEvaluation Complete. Data saved to", OutputCSV)
}

// evaluateScenario handles the lifecycle of a single test case: Setup, Execution, and Logging.
func evaluateScenario(writer *csv.Writer, arch string, count int) {
	fmt.Printf("\n--- Testing Architecture: %s | Sensors: %d ---\n", arch, count)

	// A. Setup Phase
	fmt.Print("   > Setting up environment... ")
	expectedValues := setupEnvironment(count)
	performUpdates(arch, expectedValues)
	fmt.Println("Ready.")

	// B. Measurement Phase
	avgTime, consistent := measurePerformance(arch, expectedValues)

	// C. Reporting Phase
	fmt.Printf("   > AVG Time: %.4f ms | Consistent: %v\n", avgTime, consistent)
	logResult(writer, count, arch, avgTime, consistent)
}

func measurePerformance(arch string, expectedValues map[int]string) (float64, bool) {
	var totalDuration float64
	consistent := true

	fmt.Print("   > Running queries: ")
	for r := 0; r < RunsPerTest; r++ {
		time.Sleep(SimulatedDelay)

		start := time.Now()

		// === ARCHITECTURAL ENTRY POINT ===
		retrievedValues := fetchDigitalTwinState(arch)

		totalDuration += time.Since(start).Seconds() * 1000
		fmt.Printf(".")

		if !verifyData(expectedValues, retrievedValues) {
			consistent = false
			log.Printf(" ! Consistency Violation in Run %d", r+1)
		}
	}
	fmt.Println(" Done.")

	return totalDuration / float64(RunsPerTest), consistent
}

// ======================================================================================
// SECTION II: FEDERATED ORCHESTRATION ENGINE
// This section implements the "Lazy Evaluation" and "Graph Enrichment" strategies.
// ======================================================================================

// fetchDigitalTwinState orchestrates the reconstruction of the global DT state.
func fetchDigitalTwinState(arch string) map[int]string {
	// Phase 1: Data Anchor Resolution (Topology Retrieval)
	topology, err := resolveTopology()
	if err != nil {
		log.Fatalf("Topology Resolution Failed: %v", err)
	}

	// Architectural Decision Point
	if arch == "Centralized" {
		return extractStateFromTopology(topology)
	}

	// Phase 2 & 3: State Retrieval & Semantic Hydration (Federated)
	return enrichFederatedState(topology)
}

// resolveTopology Queries the KG to identify active assets and their Data Anchors.
// Corresponds to: "The orchestrator loads the DT KG... accessing the associated Data Anchors."
func resolveTopology() ([]Thing, error) {
	ctx := context.Background()
	q := `
	{
		things(func: type(Thing)) {
			external_id
			value
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

// extractStateFromTopology represents the monolithic approach where state resides in the graph.
func extractStateFromTopology(topology []Thing) map[int]string {
	results := make(map[int]string)
	for _, t := range topology {
		results[t.ExternalID] = t.Value
	}
	return results
}

// enrichFederatedState implements Parallel Batch Retrieval and Semantic Hydration.
func enrichFederatedState(topology []Thing) map[int]string {
	results := make(map[int]string)
	var mu sync.Mutex // Guards the "Semantic Hydration" into the map
	var wg sync.WaitGroup

	// Extract Data Anchors
	ids := make([]int32, len(topology))
	for i, t := range topology {
		ids[i] = int32(t.ExternalID)
	}

	// Batch Processing Loop
	for i := 0; i < len(ids); i += BatchSize {
		end := i + BatchSize
		if end > len(ids) {
			end = len(ids)
		}

		chunkIDs := ids[i:end]
		wg.Add(1)

		// Execute "Semantic Wrapper" retrieval in a goroutine
		go func(batch []int32) {
			defer wg.Done()

			// Phase 2: State Retrieval (ISU)
			batchResults, err := fetchISUBatch(batch)
			if err != nil {
				log.Printf("ISU Batch Retrieval Error: %v", err)
				return
			}

			// Phase 3: Semantic Hydration (Merging into Global State)
			mu.Lock()
			for k, v := range batchResults {
				results[k] = v
			}
			mu.Unlock()
		}(chunkIDs)
	}

	wg.Wait()
	return results
}

// fetchISUBatch executes the SQL Logic for "Last Known Value" retrieval.
// Represents the "Semantic Wrapper" abstraction layer.
func fetchISUBatch(ids []int32) (map[int]string, error) {
	// Query implements: "Select most recent record such that t_data <= t"
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
	// Dgraph Connection
	conn, err := grpc.Dial(cfg.DgraphURI, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Semantic Plane Init Failed: %v", err)
	}
	dgClient = dgo.NewDgraphClient(api.NewDgraphClient(conn))

	// Postgres Connection
	pgDB, err = sql.Open("postgres", cfg.PostgresConn())
	if err != nil {
		log.Fatalf("Data Plane Init Failed: %v", err)
	}
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
	writer.Write([]string{"Num_Sensors", "Architecture", "Avg_Read_Time_ms", "Runs_Count", "Consistent"})
	writer.Flush()
	return writer
}

func logResult(w *csv.Writer, count int, arch string, time float64, consistent bool) {
	w.Write([]string{
		strconv.Itoa(count),
		arch,
		fmt.Sprintf("%.4f", time),
		strconv.Itoa(RunsPerTest),
		strconv.FormatBool(consistent),
	})
	w.Flush()
}

// setupEnvironment prepares the topology. Note: It does not populate data values for Federated mode,
// as those are inserted via performUpdates.
func setupEnvironment(count int) map[int]string {
	ctx := context.Background()
	dgClient.Alter(ctx, &api.Operation{DropAll: true})
	pgDB.Exec("TRUNCATE TABLE query_test")

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

	// Bulk Mutation
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

func performUpdates(arch string, data map[int]string) {
	ctx := context.Background()
	if arch == "Centralized" {
		// Update KG directly (Anti-Pattern for high frequency)
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
		jsonBytes, _ := json.Marshal(mutations)
		dgClient.NewTxn().Mutate(ctx, &api.Mutation{SetJson: jsonBytes, CommitNow: true})

	} else {
		// Insert into TSDB (Preferred pattern)
		tx, _ := pgDB.Begin()
		stmtIns, _ := tx.Prepare("INSERT INTO query_test (time, sensor_id, payload) VALUES (NOW(), $1, $2)")
		defer stmtIns.Close()
		for id, val := range data {
			stmtIns.Exec(id, val)
		}
		tx.Commit()
	}
}

func verifyData(expected map[int]string, retrieved map[int]string) bool {
	if len(expected) != len(retrieved) {
		return false
	}
	for id, expVal := range expected {
		if gotVal, ok := retrieved[id]; !ok || gotVal != expVal {
			return false
		}
	}
	return true
}
