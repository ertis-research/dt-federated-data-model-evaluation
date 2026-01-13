package main

import (
	"context"
	"database/sql"
	"dt-real-bench/config"
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

// ======================================================================================
// SECTION I: INFRASTRUCTURE & POLYGLOT PERSISTENCE
//
// This section establishes the "Polyglot Persistence" foundation using externalized
// configuration. This decoupling allows for environmental flexibility while maintaining
// the logical separation between "Topology" (Knowledge Graph) and "State" (Time-Series DB).
// ======================================================================================

const (
	TopicPrefix = "dt/sensor/"
	OutputCSV   = "output/ingestion_evaluation_results.csv"
	TotalRuns   = 2
)

// Global Infrastructure Handles & Configuration
var (
	cfg           config.Env     // Global configuration struct loaded from environment
	pgDB          *sql.DB        // Handle for the Data Plane (ISU)
	stmtTimescale *sql.Stmt      // Prepared statement for high-frequency ingestion
	dgClient      *dgo.Dgraph    // Handle for the Semantic Plane (KG)
	sensorUIDMap  map[int]string // Logical articulation mapping (Data Anchor resolution)
)

// Experimental Parameters
type Scenario struct {
	ID              int
	Name            string
	NumSensors      int
	FrequencyHz     float64
	GraphComplexity int // Represents the "Relational Density" of the topology
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

// ======================================================================================
// SECTION II: THE SEMANTIC PLANE (TOPOLOGY)
//
// This layer represents the DT's logical structure. Initialization is driven by
// the configuration provided by the environment variables (DGRAPH_URI).
// ======================================================================================

type SensorNode struct {
	Uid   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
	Name  string   `json:"name,omitempty"`
	Value string   `json:"value,omitempty"`
}

func initDgraph() {
	// Connection target derived from external configuration
	target := cfg.DgraphURI
	conn, err := grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to initialize Semantic Plane connection at %s: %v", target, err)
	}
	dgClient = dgo.NewDgraphClient(api.NewDgraphClient(conn))
}

// prepareGraph initializes the Semantic Plane before experimental execution.
// It validates the premise that "Topology comprises structural relationships...
// and remains stable barring physical modifications."
func prepareGraph(totalNodes int, activeSensors int) {
	ctx := context.Background()

	pgDB.Exec("TRUNCATE TABLE ingestion_test")

	// 1. Reset State to ensure experimental consistency
	dgClient.Alter(ctx, &api.Operation{DropAll: true})
	op := &api.Operation{Schema: `
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

	// 2. Topology Generation (Simulating Semantic Overhead)
	sensorUIDMap = make(map[int]string)
	batchSize := 1000
	var allNodes []SensorNode

	for i := range totalNodes {
		node := SensorNode{
			Uid:   fmt.Sprintf("_:node%d", i),
			DType: []string{"Thing"},
			Name:  fmt.Sprintf("Thing_%d", i),
			Value: "init", // Initial static state
		}
		allNodes = append(allNodes, node)
	}

	// 3. Persist Topology to the KG
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
			log.Fatal("Failed to populate Semantic Plane:", err)
		}

		// 4. Establish Data Anchors
		for j := i; j < end; j++ {
			if j < activeSensors {
				blankNodeID := fmt.Sprintf("node%d", j)
				sensorUIDMap[j] = resp.Uids[blankNodeID]
			}
		}
	}
}

// writeToDgraph implements the CENTRALIZED ARCHITECTURE (The Baseline/Anti-Pattern).
func writeToDgraph(sensorID int, val string) {
	uid, exists := sensorUIDMap[sensorID]
	if !exists {
		return
	}

	d := SensorNode{Uid: uid, Value: val}
	pb, _ := json.Marshal(d)

	mu := &api.Mutation{SetJson: pb, CommitNow: true}
	_, err := dgClient.NewTxn().Mutate(context.Background(), mu)
	if err != nil {
		log.Println("Semantic Plane Write Error:", err)
	}
}

// ======================================================================================
// SECTION III: THE DATA PLANE (STATE / ISUs)
//
// This layer connects to the Independent Storage Units (ISUs) using credentials
// and endpoints defined in the configuration module.
// ======================================================================================

func initTimescale() {
	var err error
	// Connection string is constructed dynamically via the config method
	connStr := cfg.PostgresConn()
	pgDB, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to ISU (Postgres) using config: %v", err)
	}

	// Schema Definition for the Data Plane
	pgDB.Exec("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
	pgDB.Exec("DROP TABLE IF EXISTS ingestion_test;")
	pgDB.Exec(`CREATE TABLE ingestion_test (
        time TIMESTAMPTZ NOT NULL,
        payload TEXT
    );`)
	pgDB.Exec("SELECT create_hypertable('ingestion_test', 'time');")

	stmtTimescale, err = pgDB.Prepare("INSERT INTO ingestion_test (time, payload) VALUES (NOW(), $1)")
	if err != nil {
		log.Fatal(err)
	}
}

// writeToTimescale implements the PROPOSED FEDERATED ARCHITECTURE.
func writeToTimescale(val string) {
	_, err := stmtTimescale.Exec(val)
	if err != nil {
		log.Println("Data Plane (ISU) Write Error:", err)
	}
}

// ======================================================================================
// SECTION IV: EXPERIMENTAL ORCHESTRATION & EVALUATION
//
// Executes the empirical comparison. MQTT clients are instantiated using
// the broker credentials loaded from the environment variables.
// ======================================================================================

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// 0. Load Configuration
	cfg = config.LoadEnv()

	// 1. Initialize Polyglot Infrastructure
	initTimescale()
	initDgraph()

	// 2. Prepare Data Logging
	file, _ := os.Create(OutputCSV)
	defer file.Close()
	writer := csv.NewWriter(file)
	writer.Write([]string{
		"Scenario_ID", "Architecture", "Run_ID", "Num_Sensors",
		"Frequency_Hz", "Graph_Complexity", "Avg_Latency_ms",
		"Packet_Loss_Count", "Throughput_msg_sec",
	})
	writer.Flush()

	// 3. Define Scenarios
	scenarios := []Scenario{
		{ID: 1, Name: "Baseline", NumSensors: 50, FrequencyHz: 1.0, GraphComplexity: 1000, DurationSec: 10},
		{ID: 2, Name: "High Frequency", NumSensors: 50, FrequencyHz: 20.0, GraphComplexity: 1000, DurationSec: 10},
		{ID: 4, Name: "Complex Graph A", NumSensors: 100, FrequencyHz: 5.0, GraphComplexity: 10000, DurationSec: 10},
		{ID: 5, Name: "Complex Graph B", NumSensors: 100, FrequencyHz: 5.0, GraphComplexity: 50000, DurationSec: 10},
	}

	architectures := []string{"Federated", "Centralized"}

	fmt.Println("Starting INGESTION EVALUATION (Federated vs Centralized)...")

	for _, sc := range scenarios {
		fmt.Printf("\n>>> Initializing Topology for Scenario %d (%d Nodes) <<<\n", sc.ID, sc.GraphComplexity)
		prepareGraph(sc.GraphComplexity, sc.NumSensors)

		for _, arch := range architectures {
			fmt.Printf("--- Evaluating Architecture: %s ---\n", arch)
			for run := 1; run <= TotalRuns; run++ {
				res := executeTestRun(sc, arch, run)
				writer.Write([]string{
					strconv.Itoa(sc.ID), arch, strconv.Itoa(run),
					strconv.Itoa(sc.NumSensors), fmt.Sprintf("%.1f", sc.FrequencyHz),
					strconv.Itoa(sc.GraphComplexity), fmt.Sprintf("%.4f", res.AvgLatencyMs),
					strconv.FormatInt(res.MessagesLost, 10), fmt.Sprintf("%.2f", res.ThroughputMsgSec),
				})
				writer.Flush()
				time.Sleep(2 * time.Second)
			}
		}
	}
	fmt.Println("\nEvaluation Complete. Data saved to", OutputCSV)
}

// executeTestRun orchestrates the message flow using configured MQTT endpoints.
func executeTestRun(sc Scenario, arch string, runID int) Result {
	var totalLatency int64 = 0
	var msgReceivedCount int64 = 0
	var msgSentCount int64 = 0
	var muMetrics sync.Mutex
	done := make(chan bool)

	// Helper to create configured MQTT Client Options
	createMQTTOpts := func(clientID string) *mqtt.ClientOptions {
		brokerURI := fmt.Sprintf("tcp://%s:%s", cfg.MQTT.Broker, cfg.MQTT.Port)
		opts := mqtt.NewClientOptions().AddBroker(brokerURI).SetClientID(clientID)
		opts.SetCleanSession(true)
		// Inject Credentials from Config
		opts.SetUsername(cfg.MQTT.User)
		opts.SetPassword(cfg.MQTT.Pass)
		return opts
	}

	// --- SERVER SIDE (Ingestion Component) ---
	opts := createMQTTOpts(fmt.Sprintf("srv_%s_%d_%d", arch, sc.ID, runID))

	// Ingestion Handler
	opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		topic := msg.Topic()
		sensorIDStr := topic[len(TopicPrefix):]
		sensorID, _ := strconv.Atoi(sensorIDStr)

		sentTimeNano, _ := strconv.ParseInt(string(msg.Payload()), 10, 64)
		sentTime := time.Unix(0, sentTimeNano)

		// === ARCHITECTURAL DECISION POINT ===
		if arch == "Centralized" {
			writeToDgraph(sensorID, string(msg.Payload()))
		} else {
			writeToTimescale(string(msg.Payload()))
		}

		latencyMicro := time.Since(sentTime).Microseconds()
		muMetrics.Lock()
		totalLatency += latencyMicro
		msgReceivedCount++
		muMetrics.Unlock()
	})

	serverClient := mqtt.NewClient(opts)
	if token := serverClient.Connect(); token.Wait() && token.Error() != nil {
		log.Printf("Server Connection Error: %v", token.Error())
		return Result{}
	}
	serverClient.Subscribe(TopicPrefix+"#", 0, nil)

	// --- CLIENT SIDE (Sensors / Load Generators) ---
	var wgSensors sync.WaitGroup
	testDuration := time.Duration(sc.DurationSec) * time.Second

	for i := 0; i < sc.NumSensors; i++ {
		wgSensors.Add(1)
		go func(id int) {
			defer wgSensors.Done()
			// Use the helper to ensure clients also use correct credentials
			cOpts := createMQTTOpts(fmt.Sprintf("s_%d_%d_%d", sc.ID, runID, id))
			c := mqtt.NewClient(cOpts)
			if token := c.Connect(); token.Wait() && token.Error() != nil {
				return
			}
			defer c.Disconnect(0)

			ticker := time.NewTicker(time.Duration(1000.0/sc.FrequencyHz) * time.Millisecond)
			defer ticker.Stop()
			endTime := time.Now().Add(testDuration)

			for range ticker.C {
				if time.Now().After(endTime) {
					return
				}

				payload := fmt.Sprintf("%d", time.Now().UnixNano())
				token := c.Publish(TopicPrefix+strconv.Itoa(id), 0, false, payload)
				token.Wait()

				muMetrics.Lock()
				msgSentCount++
				muMetrics.Unlock()
			}
		}(i)
	}

	wgSensors.Wait()
	time.Sleep(1 * time.Second)
	serverClient.Disconnect(0)
	close(done)

	avgLat := 0.0
	if msgReceivedCount > 0 {
		avgLat = float64(totalLatency) / float64(msgReceivedCount) / 1000.0
	}

	return Result{
		ScenarioID: sc.ID, Architecture: arch, RunID: runID,
		AvgLatencyMs: avgLat, MessagesSent: msgSentCount,
		MessagesLost:     msgSentCount - msgReceivedCount,
		ThroughputMsgSec: float64(msgReceivedCount) / float64(sc.DurationSec),
	}
}
