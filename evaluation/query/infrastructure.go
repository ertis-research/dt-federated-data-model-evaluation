package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
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
// INFRASTRUCTURE INITIALIZATION & HIERARCHICAL TOPOLOGY BUILDER
// ======================================================================================

// initInfrastructure establishes connections to the Semantic Plane (Dgraph) and all
// configured Independent Storage Units (TimescaleDB/PostgreSQL instances) in the Data Plane,
// initializing the hypertables and composite B-tree indexes required for LKV retrieval.
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

// closeInfrastructure cleanly releases all open database connections across the Data Plane ISUs.
func closeInfrastructure() {
	for _, db := range pgDBs {
		if db != nil {
			db.Close()
		}
	}
}

// assignTargetISU computes the target Independent Storage Unit index for a given sensor
// according to the active partition distribution strategy:
//   - "Uniform": Round-robin uniform distribution across all active ISUs.
//   - "ModerateSkew": 50% of sensors routed to ISU 0, remaining 50% distributed across ISUs 1..N-1.
//   - "HotspotSkew": 80% of sensors concentrated in ISU 0 (straggler hotspot), remaining 20% across ISUs 1..N-1.
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

// setupEnvironment constructs a multi-hop hierarchy of ontological Thing nodes linked via `hasChild`:
// WindFarm (Thing) -> 5 Zones (Thing) -> 50 Turbines (Thing) -> 200 Components (Thing) -> N Sensors (Thing).
// Only leaf Sensor Things hold Data Anchors (`isu_id`, `external_id`); upper levels are purely topological Things.
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

// performUpdates injects a realistic historical time-series window (`HistorySamples` observations
// per sensor) into either the Centralized KG (as timestamped Observation nodes) or the Federated
// TSDB ISUs (via bulk `COPY` protocol), ensuring both architectures resolve identical LKV queries.
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
				// Update PostgreSQL statistics so the index scan on (sensor_id, time DESC) is consistently chosen
				pgDBs[targetISU].Exec("ANALYZE query_test;")
			}(isuID, items)
		}
		wg.Wait()
	}
}

