package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"dt-real-bench/twin"
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
		return twin.UniformISU(sensorIdx, numISUs)
	}
}

// setupEnvironment loads the shared Wind Farm topology (see twin.BuildWindFarm):
// WindFarm (Thing) -> 5 Zones (Thing) -> 50 Turbines (Thing) -> 200 Components (Thing) -> N Sensors (Thing).
// Only leaf Sensor Things hold Data Anchors (`isu_id`, `external_id`); upper levels are purely topological Things.
func setupEnvironment(count int, numISUs int, partMode string) map[int]ExpectedSensor {
	ctx := context.Background()
	dgClient.Alter(ctx, &api.Operation{DropAll: true})
	for _, db := range pgDBs {
		db.Exec("TRUNCATE TABLE query_test")
	}

	dgClient.Alter(ctx, &api.Operation{Schema: twin.Schema})

	windFarmRoot, specs := twin.BuildWindFarm(count, func(externalID int) int {
		return assignTargetISU(externalID, count, numISUs, partMode)
	})

	expected := make(map[int]ExpectedSensor, count)
	for _, spec := range specs {
		i := spec.ExternalID
		expected[i] = ExpectedSensor{
			ExternalID:    i,
			IsuID:         spec.IsuID,
			ZoneName:      spec.ZoneName,
			TurbineName:   spec.TurbineName,
			ComponentType: spec.ComponentType,
			SensorType:    spec.SensorType,
			MaxThreshold:  spec.MaxThreshold,
			// Deterministic synthetic telemetry value in [20.0, 100.0]
			Value: 20.0 + float64((i*37)%800)/10.0,
		}
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
// per sensor) into either the Centralized KG (as timestamped Observation nodes plus the materialized
// `last_value`, via twin.ObservationUpdate) or the Federated
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
						Timestamp: baseTime.Add(time.Duration(h) * time.Second),
						Value:     item.Value - 1.0,
					}
				}
				obsList[HistorySamples-1] = Observation{
					Timestamp: baseTime.Add(time.Duration(HistorySamples) * time.Second),
					Value:     item.Value,
				}
				// Same write path as Centralized ingestion: append history + materialize LKV
				mutations = append(mutations, twin.ObservationUpdate(node.Uid, obsList))
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
