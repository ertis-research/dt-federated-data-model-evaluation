// Package main implements the end-to-end semantic query and inference evaluation
// benchmark comparing a Monolithic/Centralized Knowledge Graph against the proposed
// 4-Stage Federated Digital Twin Data Model across Independent Storage Units (ISUs).
package main

import (
	"database/sql"
	"dt-real-bench/config"
	"dt-real-bench/twin"
	"time"

	"github.com/dgraph-io/dgo/v2"
)

// ======================================================================================
// BENCHMARK CONFIGURATION & CONSTANTS
// ======================================================================================

const (
	// RunsPerTest defines the number of measurement iterations executed per scenario
	// to obtain statistically robust P50, P95, and P99 tail latency distributions.
	RunsPerTest = 30

	// WarmUpRuns defines the number of unrecorded initial executions per scenario
	// used to stabilize database connection pools, caches, and query plans.
	WarmUpRuns = 5

	// ISUSubBatch defines the maximum number of sensor external IDs dispatched per
	// parallel worker goroutine targeting an individual Independent Storage Unit (ISU).
	// It is the orchestrator's default (full system) configuration.
	ISUSubBatch = 250

	// NoSubBatch disables intra-ISU sub-batching: each ISU receives a single wrapper call with
	// all of its Data Anchors. Used as the ablation that isolates horizontal data-plane scaling
	// from intra-ISU parallelism. Also recorded for Centralized rows, where it does not apply.
	NoSubBatch = 0

	// HistorySamples defines the number of historical time-series observations injected
	// per sensor into both the Centralized KG and the Federated TSDB ISUs.
	HistorySamples = 100

	// SimulatedDelay defines the inter-run pause between consecutive query evaluations.
	SimulatedDelay = 150 * time.Millisecond

	// IdleCPUThresholdCores defines the per-pod CPU usage (cores) below which a storage pod
	// (Dgraph Alpha or a TimescaleDB ISU) is considered idle, i.e. free of background
	// compactions, rollups, or DropAll cleanup left by the previous scenario's data load.
	IdleCPUThresholdCores = 0.10

	// IdleConsecutiveChecks defines how many consecutive idle readings are required.
	IdleConsecutiveChecks = 3

	// IdlePollInterval defines the pause between idle readings. Readings use irate() over a
	// 1-minute window (last two cAdvisor samples, ~10-15 s apart), so three consecutive idle
	// readings cover at least one full cAdvisor update interval.
	IdlePollInterval = 5 * time.Second

	// IdleMaxFailedChecks defines how many consecutive failed Prometheus readings are tolerated
	// before the idle wait gives up and the scenario proceeds with a warning.
	IdleMaxFailedChecks = 6

	// IdleMaxWait bounds the idle wait; on timeout the scenario proceeds with a warning.
	IdleMaxWait = 5 * time.Minute
)

// Global shared infrastructure state across the benchmark lifecycle.
var (
	cfg      config.Env
	pgDBs    []*sql.DB   // Data Plane: Multiple Independent Storage Units (TimescaleDB/PostgreSQL ISUs)
	dgClient *dgo.Dgraph // Semantic Plane: Distributed Knowledge Graph (Dgraph)
)

// ======================================================================================
// SEMANTIC PLANE ENTITY MODEL (Topological White Nodes & Articulated Black Nodes)
// ======================================================================================

// Observation and Thing are the shared Wind Farm Digital Twin entities (see package twin),
// so the Centralized baseline uses the exact same KG model as the ingestion benchmark.
type (
	Observation = twin.Observation
	Thing       = twin.Thing
)

// ======================================================================================
// DATA PLANE & GROUND-TRUTH VERIFICATION MODELS
// ======================================================================================

// wrapperRecord represents the canonical JSON exchange payload emitted by an ISU
// Semantic Wrapper during Stage 2 (Parallel Batch State Retrieval).
type wrapperRecord struct {
	ID      int     `json:"asset_id"`
	Reading float64 `json:"reading"`
	Unit    string  `json:"unit"`
}

// ExpectedSensor holds the deterministic ground-truth state and topological context
// of a simulated sensor to verify end-to-end semantic query accuracy.
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

// QueryMetrics encapsulates the fine-grained latency breakdown, data integrity accuracy,
// and memory allocation overhead recorded for a single benchmark iteration.
type QueryMetrics struct {
	ExperimentType string
	NumSensors     int
	Architecture   string
	NumISUs        int
	PartitionMode  string  // "Uniform", "ModerateSkew", "HotspotSkew"
	QueryType      string  // "Q1_Selective", "Q2_Filtering", "Q3_Inference"
	Concurrency    int     // Number of concurrent query streams (C)
	SubBatch       int     // Intra-ISU sub-batch size (ISUSubBatch) or NoSubBatch (0)
	RunID          int     // Iteration index (1..RunsPerTest)
	TotalLatency   float64 // End-to-end execution latency (ms): Stage 1 + Stage 2 + Stage 3 + Stage 4
	Stage1AnchorMs float64 // Stage 1: Topological Traversal & Data Anchor Resolution in KG (ms)
	Stage2ISUIOMs  float64 // Stage 2: Parallel State Retrieval from ISUs via Semantic Wrappers (ms)
	Stage3HydrMs   float64 // Stage 3: Semantic Hydration & RDF Triple Synthesis in Ephemeral Graph (ms)
	Stage4QueryMs  float64 // Stage 4: Embedded Inference & BGP Query Execution (ms)
	SuccessRate    float64 // End-to-end Data Integrity percentage (0.0 - 100.0%)
	MemAllocMB     float64 // Heap memory allocated per query execution (MB)
}
