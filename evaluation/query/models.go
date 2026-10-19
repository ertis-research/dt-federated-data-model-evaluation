// Package main implements the end-to-end semantic query and inference evaluation
// benchmark comparing a Monolithic/Centralized Knowledge Graph against the proposed
// 4-Stage Federated Digital Twin Data Model across Independent Storage Units (ISUs).
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
	// RunsPerTest defines the number of measurement iterations executed per scenario
	// to obtain statistically robust P50, P95, and P99 tail latency distributions.
	RunsPerTest = 30

	// WarmUpRuns defines the number of unrecorded initial executions per scenario
	// used to stabilize database connection pools, caches, and query plans.
	WarmUpRuns = 5

	// ISUSubBatch defines the maximum number of sensor external IDs dispatched per
	// parallel worker goroutine targeting an individual Independent Storage Unit (ISU).
	ISUSubBatch = 250

	// HistorySamples defines the number of historical time-series observations injected
	// per sensor into both the Centralized KG and the Federated TSDB ISUs.
	HistorySamples = 100

	// SimulatedDelay defines the inter-run pause between consecutive query evaluations.
	SimulatedDelay = 150 * time.Millisecond
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

// Observation represents a timestamped telemetry state node stored directly within
// the Centralized Knowledge Graph baseline.
type Observation struct {
	Uid       string    `json:"uid,omitempty"`
	DType     []string  `json:"dgraph.type,omitempty"`
	Timestamp time.Time `json:"timestamp,omitempty"`
	Value     float64   `json:"value,omitempty"`
}

// Thing represents a canonical ontological entity in the Knowledge Graph
// (e.g., WindFarm, Zone, Turbine, Component, or Sensor).
// Purely topological entities (white nodes: WindFarm, Zone, Turbine, Component)
// maintain hierarchical `hasChild` relationships without external Data Anchors.
// IoT-linked entities (articulated black nodes: Sensor) encapsulate a Data Anchor
// composed of `(IsuID, ExternalID)` pointing to the Federated Data Plane.
type Thing struct {
	Uid            string        `json:"uid,omitempty"`
	DType          []string      `json:"dgraph.type,omitempty"`
	Name           string        `json:"name,omitempty"`
	AssetCategory  string        `json:"asset_category,omitempty"` // "WindFarm", "Zone", "Turbine", "Component", "Sensor"
	SubType        string        `json:"sub_type,omitempty"`       // e.g., "Gearbox", "Generator", "Temperature", "Vibration"
	MaxThreshold   float64       `json:"max_threshold,omitempty"`
	IsuID          int           `json:"isu_id,omitempty"`         // Data Anchor: Target ISU index
	ExternalID     int           `json:"external_id,omitempty"`    // Data Anchor: Local TSDB sensor identifier
	Value          float64       `json:"value,omitempty"`          // Dynamic telemetry state
	HasObservation []Observation `json:"hasObservation,omitempty"` // Historical observations in Centralized KG mode
	HasChild       []Thing       `json:"hasChild,omitempty"`       // Topological hierarchical relationships
}

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
	RunID          int     // Iteration index (1..RunsPerTest)
	TotalLatency   float64 // End-to-end execution latency (ms): Stage 1 + Stage 2 + Stage 3 + Stage 4
	Stage1AnchorMs float64 // Stage 1: Topological Traversal & Data Anchor Resolution in KG (ms)
	Stage2ISUIOMs  float64 // Stage 2: Parallel State Retrieval from ISUs via Semantic Wrappers (ms)
	Stage3HydrMs   float64 // Stage 3: Semantic Hydration & RDF Triple Synthesis in Ephemeral Graph (ms)
	Stage4QueryMs  float64 // Stage 4: Embedded Inference & BGP Query Execution (ms)
	SuccessRate    float64 // End-to-end Data Integrity percentage (0.0 - 100.0%)
	MemAllocMB     float64 // Heap memory allocated per query execution (MB)
}

