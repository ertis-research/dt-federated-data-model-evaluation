package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	_ "github.com/lib/pq" // Postgres driver
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// --- Configuration Constants ---
const (
	BrokerAddress = "aaa"
	TopicPrefix   = "dt/sensor/"
	TotalRuns     = 5
	OutputCSV     = "final_evaluation_results.csv"

	// DB Connection Strings
	PostgresConn = "aaa"
	DgraphTarget = "aaa"
)

// --- Structs ---

type Scenario struct {
	ID              int
	Name            string
	NumSensors      int
	FrequencyHz     float64
	GraphComplexity int // Total nodes in the Knowledge Graph
	DurationSec     int
}

type Result struct {
	ScenarioID       int
	Architecture     string
	RunID            int
	AvgLatencyMs     float64
	MessagesSent     int64
	MessagesLost     int64
	ThroughputMsgSec float64
}

// Helper struct for Dgraph JSON mutation
type SensorNode struct {
	Uid   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
	Name  string   `json:"name,omitempty"`
	Value string   `json:"value,omitempty"` // The dynamic value to update
}

// --- Global Variables ---
var (
	pgDB          *sql.DB
	stmtTimescale *sql.Stmt // Prepared statement for fast SQL
	dgClient      *dgo.Dgraph

	// Map SensorID (int) -> Dgraph UID (string)
	// Used to simulate that we know exactly which node to update in the graph
	sensorUIDMap map[int]string
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// 1. Initialize DB Connections
	initTimescale()
	initDgraph()

	// 2. Prepare CSV Output
	file, err := os.Create(OutputCSV)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	// Header in Academic English
	writer.Write([]string{
		"Scenario_ID", "Architecture", "Run_ID", "Num_Sensors",
		"Frequency_Hz", "Graph_Complexity", "Avg_Latency_ms",
		"Packet_Loss_Count", "Throughput_msg_sec",
	})
	writer.Flush()

	// 3. Define Scenarios (Incremental Stress)
	scenarios := []Scenario{
		// Scenario 1: Baseline (Low load, Simple Graph)
		{ID: 1, Name: "Baseline", NumSensors: 50, FrequencyHz: 1.0, GraphComplexity: 1000, DurationSec: 10},

		// Scenario 2: High Frequency (Stress on Ingestion Rate)
		{ID: 2, Name: "High Frequency", NumSensors: 50, FrequencyHz: 20.0, GraphComplexity: 1000, DurationSec: 10},

		// Scenario 3: High Concurrency (Stress on DB Locks/Threads)
		{ID: 3, Name: "High Concurrency", NumSensors: 300, FrequencyHz: 1.0, GraphComplexity: 1000, DurationSec: 10},

		// Scenario 4: Complex Graph (Stress on Graph Traversal/Indexing)
		{ID: 4, Name: "Complex Graph A", NumSensors: 100, FrequencyHz: 5.0, GraphComplexity: 10000, DurationSec: 10},

		// Scenario 5: Massive Graph (Extreme Semantic Overhead)
		{ID: 5, Name: "Complex Graph B", NumSensors: 100, FrequencyHz: 5.0, GraphComplexity: 50000, DurationSec: 10},
	}

	architectures := []string{"Federated", "Centralized"}

	fmt.Println("Starting DIGITAL TWIN EVALUATION (Go + Dgraph + TimescaleDB)...")

	for _, sc := range scenarios {
		// --- PRE-RUN SETUP ---
		// We prepare the graph topology ONCE per scenario to ensure consistency
		fmt.Printf("\n>>> Preparing Scenario %d: Complexity %d Nodes <<<\n", sc.ID, sc.GraphComplexity)
		prepareGraph(sc.GraphComplexity, sc.NumSensors)

		for _, arch := range architectures {
			fmt.Printf("--- Arch: %s ---\n", arch)

			for run := 1; run <= TotalRuns; run++ {
				fmt.Printf("  Run %d/%d... ", run, TotalRuns)

				// Force Garbage Collection to start fresh
				// runtime.GC()

				res := executeTestRun(sc, arch, run)

				// Save to CSV
				writer.Write([]string{
					strconv.Itoa(sc.ID),
					arch,
					strconv.Itoa(run),
					strconv.Itoa(sc.NumSensors),
					fmt.Sprintf("%.1f", sc.FrequencyHz),
					strconv.Itoa(sc.GraphComplexity),
					fmt.Sprintf("%.4f", res.AvgLatencyMs),
					strconv.FormatInt(res.MessagesLost, 10),
					fmt.Sprintf("%.2f", res.ThroughputMsgSec),
				})
				writer.Flush()
				fmt.Println("Done.")

				// Cool down between runs to let DB buffers drain
				time.Sleep(2 * time.Second)
			}
		}
	}
	fmt.Println("\nEvaluation Complete. Data saved to", OutputCSV)
}

func executeTestRun(sc Scenario, arch string, runID int) Result {
	// Metrics (Thread-safe)
	var totalLatency int64 = 0
	var msgReceivedCount int64 = 0
	var msgSentCount int64 = 0
	var muMetrics sync.Mutex

	// Done channel
	done := make(chan bool)

	// 1. Start SERVER (Subscriber)
	opts := mqtt.NewClientOptions().AddBroker(BrokerAddress).SetClientID(fmt.Sprintf("srv_%s_%d_%d", arch, sc.ID, runID))
	opts.SetCleanSession(true)
	// High performance settings
	opts.SetWriteTimeout(5 * time.Second)
	opts.SetKeepAlive(60 * time.Second)

	// The Ingestion Handler
	opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {

		// Parse topic to get Sensor ID "dt/sensor/123" -> 123
		topic := msg.Topic()
		sensorIDStr := topic[len(TopicPrefix):]
		sensorID, _ := strconv.Atoi(sensorIDStr)

		// Payload is the sent timestamp
		sentTimeNano, _ := strconv.ParseInt(string(msg.Payload()), 10, 64)
		sentTime := time.Unix(0, sentTimeNano)

		// --- ARCHITECTURE WRITE LOGIC ---
		if arch == "Centralized" {
			// Write to Dgraph: Find the specific node and update it
			writeToDgraph(sensorID, string(msg.Payload()))
		} else {
			// Write to TimescaleDB: Append to hypertable
			writeToTimescale(string(msg.Payload()))
		}

		// Calculate End-to-End Latency
		latencyMicro := time.Since(sentTime).Microseconds()

		muMetrics.Lock()
		totalLatency += latencyMicro
		msgReceivedCount++
		muMetrics.Unlock()
	})

	serverClient := mqtt.NewClient(opts)
	if token := serverClient.Connect(); token.Wait() && token.Error() != nil {
		log.Printf("Error connecting server: %v", token.Error())
		return Result{}
	}
	serverClient.Subscribe(TopicPrefix+"#", 0, nil)

	// 2. Start SENSORS (Publishers)
	var wgSensors sync.WaitGroup
	testDuration := time.Duration(sc.DurationSec) * time.Second

	// Pre-create clients to avoid connection overhead during measurement
	// NOTE: In a real simulation of 1000s devices, we might reuse connections or use a load generator pattern.
	// For <500, goroutines are fine.

	for i := 0; i < sc.NumSensors; i++ {
		wgSensors.Add(1)
		go func(id int) {
			defer wgSensors.Done()

			// Unique Client ID
			cOpts := mqtt.NewClientOptions().AddBroker(BrokerAddress).SetClientID(fmt.Sprintf("s_%d_%d_%d", sc.ID, runID, id))
			cOpts.SetCleanSession(true)
			c := mqtt.NewClient(cOpts)
			if token := c.Connect(); token.Wait() && token.Error() != nil {
				return
			}
			defer c.Disconnect(0)

			// Update Loop
			ticker := time.NewTicker(time.Duration(1000.0/sc.FrequencyHz) * time.Millisecond)
			defer ticker.Stop()

			endTime := time.Now().Add(testDuration)

			for range ticker.C {
				if time.Now().After(endTime) {
					return
				}

				// Payload: Timestamp only
				payload := fmt.Sprintf("%d", time.Now().UnixNano())

				token := c.Publish(TopicPrefix+strconv.Itoa(id), 0, false, payload)
				token.Wait() // Wait for network ack (QoS 0 is fast, but wait ensures it left)

				muMetrics.Lock()
				msgSentCount++
				muMetrics.Unlock()
			}
		}(i)
	}

	wgSensors.Wait()

	// Wait a bit for pending messages in the broker/db queue
	time.Sleep(1 * time.Second)
	serverClient.Disconnect(0)
	close(done)

	// Calc Stats
	avgLat := 0.0
	if msgReceivedCount > 0 {
		avgLat = float64(totalLatency) / float64(msgReceivedCount) / 1000.0 // us to ms
	}

	lost := msgSentCount - msgReceivedCount
	if lost < 0 {
		lost = 0
	}

	throughput := float64(msgReceivedCount) / float64(sc.DurationSec)

	return Result{
		ScenarioID:       sc.ID,
		Architecture:     arch,
		RunID:            runID,
		AvgLatencyMs:     avgLat,
		MessagesSent:     msgSentCount,
		MessagesLost:     lost,
		ThroughputMsgSec: throughput,
	}
}

// ---------------------------------------------------------
// DATABASE FUNCTIONS
// ---------------------------------------------------------

func initTimescale() {
	var err error
	pgDB, err = sql.Open("postgres", PostgresConn)
	if err != nil {
		log.Fatal("Could not connect to Postgres:", err)
	}

	// Setup Schema
	pgDB.Exec("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
	pgDB.Exec("DROP TABLE IF EXISTS sensor_data;")
	pgDB.Exec(`CREATE TABLE sensor_data (
		time TIMESTAMPTZ NOT NULL,
		payload TEXT
	);`)
	pgDB.Exec("SELECT create_hypertable('sensor_data', 'time');")

	// PREPARE STATEMENT (Critical for performance)
	stmtTimescale, err = pgDB.Prepare("INSERT INTO sensor_data (time, payload) VALUES (NOW(), $1)")
	if err != nil {
		log.Fatal("Could not prepare statement:", err)
	}
}

func initDgraph() {
	conn, err := grpc.Dial(DgraphTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	dgClient = dgo.NewDgraphClient(api.NewDgraphClient(conn))
}

// prepareGraph sets up the initial state in Dgraph
// It creates 'totalNodes' and maps the first 'activeSensors' to our ID map.
func prepareGraph(totalNodes int, activeSensors int) {
	ctx := context.Background()

	// 1. Drop All (Start Clean)
	op := &api.Operation{DropAll: true}
	if err := dgClient.Alter(ctx, op); err != nil {
		log.Printf("Warning clearing Dgraph: %v", err)
	}

	// 2. Set Schema
	op = &api.Operation{Schema: `
		name: string @index(hash) .
		value: string .
		type Thing {
			name
			value
		}
	`}
	if err := dgClient.Alter(ctx, op); err != nil {
		log.Fatal(err)
	}

	// 3. Create Nodes in Batches
	sensorUIDMap = make(map[int]string)

	// To avoid sending 50,000 requests, we batch them
	batchSize := 1000

	var allNodes []SensorNode

	for i := 0; i < totalNodes; i++ {
		nodeName := fmt.Sprintf("Thing_%d", i)
		node := SensorNode{
			Uid:   fmt.Sprintf("_:node%d", i), // Blank Node
			DType: []string{"Thing"},
			Name:  nodeName,
			Value: "init",
		}
		allNodes = append(allNodes, node)
	}

	// Mutate in chunks
	for i := 0; i < len(allNodes); i += batchSize {
		end := i + batchSize
		if end > len(allNodes) {
			end = len(allNodes)
		}

		batch := allNodes[i:end]
		jsonBytes, _ := json.Marshal(batch)

		mu := &api.Mutation{SetJson: jsonBytes, CommitNow: true}
		resp, err := dgClient.NewTxn().Mutate(ctx, mu)
		if err != nil {
			log.Fatal("Failed to populate graph:", err)
		}

		// 4. Map UIDs for the Active Sensors
		// We need to know the REAL UIDs generated by Dgraph to update them later
		for j := i; j < end; j++ {
			// If this node corresponds to an active sensor (0 to activeSensors-1)
			if j < activeSensors {
				blankNodeID := fmt.Sprintf("node%d", j)
				realUID := resp.Uids[blankNodeID]
				sensorUIDMap[j] = realUID
			}
		}
	}

	// Verify mapping
	if len(sensorUIDMap) < activeSensors {
		log.Printf("WARNING: Mapped %d sensors, expected %d", len(sensorUIDMap), activeSensors)
	}
}

// writeToDgraph simulates a semantic update (Centralized)
// It must locate the node and update a predicate.
func writeToDgraph(sensorID int, val string) {
	// Look up the UID (simulating "Knowing which node is which")
	uid, exists := sensorUIDMap[sensorID]
	if !exists {
		// If sensor not found in graph, we skip (should not happen in this test)
		return
	}

	// Create JSON mutation for existing UID
	// We update the "value" predicate
	d := SensorNode{
		Uid:   uid,
		Value: val,
	}
	pb, _ := json.Marshal(d)

	mu := &api.Mutation{SetJson: pb, CommitNow: true}
	_, err := dgClient.NewTxn().Mutate(context.Background(), mu)
	if err != nil {
		// Log error but don't crash, count as lost implicitly by not returning success?
		// Actually blocking here increases latency, which is what we want to measure.
		log.Println("Dgraph Write Error:", err)
	}
}

// writeToTimescale simulates raw telemetry ingestion (Federated)
// It uses prepared statements for max speed.
func writeToTimescale(val string) {
	// O(1) Append. No graph traversal. No locking of neighbor nodes.
	_, err := stmtTimescale.Exec(val)
	if err != nil {
		log.Println("Timescale Write Error:", err)
	}
}
