// Package twin defines the single Wind Farm Digital Twin data model shared by the ingestion
// and query benchmarks: the Dgraph schema, the hierarchical topology, the Data Anchor
// partitioning, and the canonical Centralized write path (append Observation + materialize LKV).
package twin

import (
	"fmt"
	"time"
)

// ======================================================================================
// SEMANTIC PLANE SCHEMA (shared by Centralized and Federated deployments)
// ======================================================================================

// Schema is the Dgraph schema used by every architecture. In Centralized mode, articulated
// Sensor Things hold both the full observation history (`hasObservation`) and the materialized
// Last Known Value (`last_value`, `last_timestamp`). In Federated mode, the same Things only hold
// their Data Anchors (`isu_id`, `external_id`) and the history lives in the ISUs.
const Schema = `
	name: string @index(exact) .
	asset_category: string @index(exact) .
	sub_type: string @index(exact) .
	external_id: int @index(int) .
	isu_id: int @index(int) .
	max_threshold: float .
	last_value: float .
	last_timestamp: datetime .
	value: float .
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
		last_value
		last_timestamp
		hasObservation
		hasChild
	}
`

// ======================================================================================
// WIND FARM TOPOLOGY
// ======================================================================================

const (
	NumZones        = 5
	TurbinesPerZone = 10 // 50 turbines total
	CompsPerTurbine = 4  // 200 components total

	// DefaultMaxThreshold is the anomaly threshold assigned to every Sensor Thing.
	DefaultMaxThreshold = 80.0
)

var (
	ComponentTypes = []string{"Gearbox", "Generator", "Rotor", "PitchControl"}
	SensorTypes    = []string{"Temperature", "Vibration", "ActivePower", "RotationalSpeed"}
)

// Observation represents a timestamped telemetry node stored in the Centralized KG.
type Observation struct {
	Uid       string    `json:"uid,omitempty"`
	DType     []string  `json:"dgraph.type,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
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
	LastValue      *float64      `json:"last_value,omitempty"`     // Materialized LKV (Centralized mode only)
	LastTimestamp  *time.Time    `json:"last_timestamp,omitempty"` // Timestamp of the materialized LKV
	HasObservation []Observation `json:"hasObservation,omitempty"` // Historical observations (Centralized mode only)
	HasChild       []Thing       `json:"hasChild,omitempty"`       // Topological hierarchical relationships
}

// SensorSpec describes the topological placement and Data Anchor of a generated Sensor Thing.
type SensorSpec struct {
	ExternalID    int
	IsuID         int
	ZoneName      string
	TurbineName   string
	ComponentType string
	SensorType    string
	MaxThreshold  float64
}

// TotalNodes returns the number of Thing nodes in a topology with `numSensors` sensors.
func TotalNodes(numSensors int) int {
	numTurbines := NumZones * TurbinesPerZone
	return 1 + NumZones + numTurbines + numTurbines*CompsPerTurbine + numSensors
}

// SensorBlankNode returns the blank-node label of a Sensor Thing, used to resolve its UID
// from the mutation response (`resp.Uids[SensorBlankNode(id)]`).
func SensorBlankNode(externalID int) string {
	return fmt.Sprintf("sensor_%d", externalID)
}

// UniformISU hash-partitions external IDs across the active ISUs (splitmix64(id) mod N).
// Round-robin (`(id-1) mod N`) would align with the topology's own modular structure
// (e.g. every Gearbox sensor has id ≡ 1 mod 4 and would land on only 2 of 8 ISUs), turning
// selective queries into an unintended skew scenario; hashing keeps every subset balanced.
func UniformISU(externalID int, numISUs int) int {
	if numISUs <= 1 {
		return 0
	}
	return int(splitmix64(uint64(externalID)) % uint64(numISUs))
}

// splitmix64 is a fixed, well-distributed 64-bit mixing function (deterministic across runs).
func splitmix64(x uint64) uint64 {
	x += 0x9E3779B97F4A7C15
	x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9
	x = (x ^ (x >> 27)) * 0x94D049BB133111EB
	return x ^ (x >> 31)
}

// BuildWindFarm constructs the multi-hop hierarchy of Thing nodes linked via `hasChild`:
// WindFarm -> 5 Zones -> 50 Turbines -> 200 Components -> N Sensors (external IDs 1..N),
// assigning each sensor's Data Anchor with `isuOf`.
func BuildWindFarm(numSensors int, isuOf func(externalID int) int) (Thing, []SensorSpec) {
	numComps := NumZones * TurbinesPerZone * CompsPerTurbine
	compSensors := make(map[int][]Thing, numComps)
	specs := make([]SensorSpec, 0, numSensors)

	for i := 1; i <= numSensors; i++ {
		compGlobalIdx := (i - 1) % numComps
		zoneIdx := compGlobalIdx / (TurbinesPerZone * CompsPerTurbine)
		turbGlobalIdx := compGlobalIdx / CompsPerTurbine
		compLocalIdx := compGlobalIdx % CompsPerTurbine
		sType := SensorTypes[i%len(SensorTypes)]
		isuID := isuOf(i)

		compSensors[compGlobalIdx] = append(compSensors[compGlobalIdx], Thing{
			Uid:           "_:" + SensorBlankNode(i),
			DType:         []string{"Thing"},
			Name:          fmt.Sprintf("Sensor_%d", i),
			AssetCategory: "Sensor",
			SubType:       sType,
			ExternalID:    i,
			IsuID:         isuID,
			MaxThreshold:  DefaultMaxThreshold,
		})

		specs = append(specs, SensorSpec{
			ExternalID:    i,
			IsuID:         isuID,
			ZoneName:      fmt.Sprintf("Zone_%d", zoneIdx),
			TurbineName:   fmt.Sprintf("Turbine_%d", turbGlobalIdx),
			ComponentType: ComponentTypes[compLocalIdx],
			SensorType:    sType,
			MaxThreshold:  DefaultMaxThreshold,
		})
	}

	zones := make([]Thing, 0, NumZones)
	for z := 0; z < NumZones; z++ {
		turbines := make([]Thing, 0, TurbinesPerZone)
		for t := 0; t < TurbinesPerZone; t++ {
			turbGlobalIdx := z*TurbinesPerZone + t
			comps := make([]Thing, 0, CompsPerTurbine)
			for c := 0; c < CompsPerTurbine; c++ {
				compGlobalIdx := turbGlobalIdx*CompsPerTurbine + c
				comps = append(comps, Thing{
					Uid:           fmt.Sprintf("_:comp_%d", compGlobalIdx),
					DType:         []string{"Thing"},
					Name:          fmt.Sprintf("Comp_%d_%s", compGlobalIdx, ComponentTypes[c]),
					AssetCategory: "Component",
					SubType:       ComponentTypes[c],
					HasChild:      compSensors[compGlobalIdx],
				})
			}
			turbines = append(turbines, Thing{
				Uid:           fmt.Sprintf("_:turb_%d", turbGlobalIdx),
				DType:         []string{"Thing"},
				Name:          fmt.Sprintf("Turbine_%d", turbGlobalIdx),
				AssetCategory: "Turbine",
				HasChild:      comps,
			})
		}
		zones = append(zones, Thing{
			Uid:           fmt.Sprintf("_:zone_%d", z),
			DType:         []string{"Thing"},
			Name:          fmt.Sprintf("Zone_%d", z),
			AssetCategory: "Zone",
			HasChild:      turbines,
		})
	}

	root := Thing{
		Uid:           "_:windfarm_0",
		DType:         []string{"Thing"},
		Name:          "WindFarm_Alpha",
		AssetCategory: "WindFarm",
		HasChild:      zones,
	}
	return root, specs
}

// ======================================================================================
// CANONICAL CENTRALIZED WRITE PATH
// ======================================================================================

// ObservationUpdate builds the canonical Centralized mutation for a Sensor Thing: it appends
// every observation as a new `hasObservation` node and materializes the most recent one as the
// sensor's Last Known Value. Both benchmarks write the Centralized KG exclusively through it.
func ObservationUpdate(sensorUID string, obs []Observation) Thing {
	latest := 0
	for i := range obs {
		obs[i].DType = []string{"Observation"}
		if obs[i].Timestamp.After(obs[latest].Timestamp) {
			latest = i
		}
	}
	v := obs[latest].Value
	ts := obs[latest].Timestamp
	return Thing{
		Uid:            sensorUID,
		LastValue:      &v,
		LastTimestamp:  &ts,
		HasObservation: obs,
	}
}
