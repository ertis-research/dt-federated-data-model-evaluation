// Package main implements the end-to-end telemetry ingestion evaluation benchmark
// comparing Monolithic/Centralized Knowledge Graph ingestion (1-to-1 and Batched)
// against the Federated Digital Twin Data Model across Independent Storage Units (ISUs).
package main

import (
	"database/sql"
	"dt-real-bench/config"
	"time"

	"github.com/dgraph-io/dgo/v2"
)

// ======================================================================================
// BENCHMARK CONFIGURATION & CONSTANTS
// ======================================================================================

const (
	// TopicPrefix defines the base MQTT topic namespace used for sensor telemetry publishing.
	TopicPrefix = "dt/sensor/"

	// IngestionCSVFileName defines the output CSV filename created inside each timestamped run directory.
	IngestionCSVFileName = "ingestion_evaluation_results.csv"

	// TotalRuns defines the number of measurement iterations executed per scenario and architecture.
	TotalRuns = 5

	// TestDurationSec defines the steady-state telemetry publishing duration per measurement run (seconds).
	TestDurationSec = 30

	// WarmupDurationSec defines the duration of the initial unrecorded system warm-up phase (seconds).
	WarmupDurationSec = 15

	// DBDrainTimeoutSec defines the maximum wait time allowed for in-flight database writes
	// to drain after all simulated sensors finish publishing (seconds).
	DBDrainTimeoutSec = 20

	// PromScrapeWaitSec defines the post-run wait window to ensure the last kubelet cAdvisor
	// samples of the run (10 s housekeeping + 5 s scrape interval) are ingested by Prometheus
	// before querying resource consumption (seconds).
	PromScrapeWaitSec = 15

	// MinPromWindowSec defines a safety floor for the Prometheus evaluation window: four scrape
	// intervals (5 s each), so rate() always has several samples. Measured runs (>= 30 s) never
	// hit it, so the window always matches the run's own activity period.
	MinPromWindowSec = 20

	// QoSLevel defines the MQTT Quality of Service level (1 = At Least Once delivery).
	QoSLevel = 1

	// NumServerSubscribers defines the number of parallel MQTT subscriber clients used by
	// the ingestion server to partition incoming telemetry topics and prevent single-session
	// broker mailbox overflow under high sensor concurrency (Steps 5 and 6).
	NumServerSubscribers = 16

	// BatchMaxSize defines the maximum number of events accumulated before flushing a batch
	// in the Centralized_Batched baseline architecture.
	BatchMaxSize = 100

	// BatchMaxFlush defines the maximum time interval before flushing a partial batch
	// in the Centralized_Batched baseline architecture.
	BatchMaxFlush = 100 * time.Millisecond

	// SensorValueMin and SensorValueMax bound the synthetic telemetry reading carried by each message.
	SensorValueMin = 20.0
	SensorValueMax = 100.0

	// insertTimescaleSQL defines the Federated write: one telemetry row per reading.
	insertTimescaleSQL = "INSERT INTO ingestion_test (time, sensor_id, payload) VALUES (NOW(), $1, $2)"
)

// Global shared infrastructure state across the ingestion benchmark lifecycle.
var (
	cfg            config.Env
	pgDBs          []*sql.DB      // Data Plane: Connections to Independent Storage Units (TimescaleDB/PostgreSQL)
	stmtsTimescale []*sql.Stmt    // Pre-compiled SQL INSERT statements per ISU
	dgClient       *dgo.Dgraph    // Semantic Plane: Distributed Knowledge Graph client (Dgraph)
	sensorUIDMap   map[int]string // Mapping from logical Sensor ID to Dgraph internal UID
)

// ======================================================================================
// DOMAIN, WORKLOAD & TELEMETRY METRICS MODELS
// ======================================================================================

// PromResponse represents the JSON structure returned by the Prometheus HTTP query API.
type PromResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  []interface{}     `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

// Scenario defines a single workload scaling step characterized by the number of active
// IoT sensors and their publishing frequency (Hz). The KG size follows from the shared
// Wind Farm topology (twin.TotalNodes).
type Scenario struct {
	StepID      int
	NumSensors  int
	FrequencyHz float64
}

// ArchConfig defines a target ingestion architecture configuration under evaluation.
type ArchConfig struct {
	Name    string
	Mode    string // "Centralized_1to1", "Centralized_Batched", "Federated"
	NumISUs int    // 0 for centralized modes; 1, 2, 4, 8 for federated ISU configurations
}

// Result encapsulates the end-to-end ingestion latency distribution, delivery reliability,
// throughput, and Kubernetes container resource utilization for a single benchmark run.
type Result struct {
	StepID            int
	NumSensors        int
	Architecture      string
	NumISUs           int
	RunID             int
	FrequencyHz       float64
	AvgLatencyMs      float64
	P50LatencyMs      float64
	P95LatencyMs      float64
	P99LatencyMs      float64
	MessagesSent      int64
	MessagesLost      int64 // Undelivered messages (not persisted before the drain cutoff)
	DbErrors          int64
	ThroughputMsgSec  float64 // Sustained throughput: messages persisted during the publishing window / window length
	TargetCPUAvgCores float64
	TargetRAMMaxMB    float64
}

// batchedEvent represents an individual telemetry message queued for micro-batch mutation
// in the Centralized_Batched ingestion mode.
type batchedEvent struct {
	sensorID int
	value    float64
	sentTime time.Time
	recvTime time.Time
}

// msgKey uniquely identifies a published telemetry message (sensor and publish timestamp),
// used to reconcile published messages against persisted ones.
type msgKey struct {
	sensorID int
	sentNano int64
}
