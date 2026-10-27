package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"dt-real-bench/twin"
)

// ======================================================================================
// INFRASTRUCTURE INITIALIZATION, ENVIRONMENT SETUP & PERSISTENCE LAYER
// ======================================================================================

// initInfrastructure establishes connections to the Semantic Plane (Dgraph) and all
// configured Independent Storage Units (TimescaleDB/PostgreSQL instances) in the Data Plane,
// initializing hypertables (with the same LKV index used by the query benchmark) and
// pre-compiling SQL insert statements.
func initInfrastructure() {
	conn, err := grpc.Dial(cfg.DgraphURI, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to Dgraph: %v", err)
	}
	dgClient = dgo.NewDgraphClient(api.NewDgraphClient(conn))

	numISUs := len(cfg.Timescale.Ports)
	pgDBs = make([]*sql.DB, numISUs)
	stmtsTimescale = make([]*sql.Stmt, numISUs)

	for i := 0; i < numISUs; i++ {
		db, err := sql.Open("postgres", cfg.PostgresConnForISU(i))
		if err != nil {
			log.Fatalf("Failed to open ISU %d: %v", i, err)
		}
		db.SetMaxOpenConns(50)
		db.SetMaxIdleConns(50)

		db.Exec("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
		db.Exec("DROP TABLE IF EXISTS ingestion_test;")
		db.Exec(`CREATE TABLE ingestion_test (
			time TIMESTAMPTZ NOT NULL,
			sensor_id INT,
			payload DOUBLE PRECISION
		);`)
		db.Exec("SELECT create_hypertable('ingestion_test', 'time');")
		db.Exec("CREATE INDEX ON ingestion_test (sensor_id, time DESC);")

		stmt, err := db.Prepare(insertTimescaleSQL)
		if err != nil {
			log.Fatalf("Failed to prepare statement on ISU %d: %v", i, err)
		}
		pgDBs[i] = db
		stmtsTimescale[i] = stmt
	}
}

// closeInfrastructure cleanly closes all prepared SQL statements and database connections.
func closeInfrastructure() {
	for _, stmt := range stmtsTimescale {
		if stmt != nil {
			stmt.Close()
		}
	}
	for _, db := range pgDBs {
		if db != nil {
			db.Close()
		}
	}
}

// createMQTTOpts builds an MQTT client configuration with standard timeouts and credentials.
func createMQTTOpts(clientID string) *mqtt.ClientOptions {
	brokerURI := fmt.Sprintf("tcp://%s:%s", cfg.MQTT.Broker, cfg.MQTT.Port)
	opts := mqtt.NewClientOptions().AddBroker(brokerURI).SetClientID(clientID)
	opts.SetCleanSession(true)
	opts.SetUsername(cfg.MQTT.User)
	opts.SetPassword(cfg.MQTT.Pass)
	opts.SetKeepAlive(60 * time.Second)
	opts.SetWriteTimeout(5 * time.Second)
	opts.SetConnectTimeout(5 * time.Second)
	return opts
}

// prepareEnvironment resets both Dgraph and all TimescaleDB ISUs, pre-allocates the
// active hypertable time chunk, applies the shared Dgraph schema, and loads the shared
// Wind Farm topology (see twin.BuildWindFarm) with `numSensors` articulated Sensor Things
// whose Data Anchors are partitioned across `numISUs`, recording the UIDs of the sensors.
func prepareEnvironment(numSensors int, numISUs int) {
	ctx := context.Background()
	dgClient.Alter(ctx, &api.Operation{DropAll: true})
	for i, db := range pgDBs {
		if stmtsTimescale[i] != nil {
			stmtsTimescale[i].Close()
		}
		db.Exec("TRUNCATE TABLE ingestion_test")
		db.SetMaxIdleConns(0)
		db.SetMaxIdleConns(50)
		stmt, err := db.Prepare(insertTimescaleSQL)
		if err != nil {
			log.Fatalf("Failed to re-prepare statement on ISU %d: %v", i, err)
		}
		stmtsTimescale[i] = stmt
		// Insert 1 seed row so TimescaleDB pre-allocates the current time chunk before Run 1 starts
		stmtsTimescale[i].Exec(0, 0.0)
	}

	dgClient.Alter(ctx, &api.Operation{Schema: twin.Schema})

	windFarmRoot, specs := twin.BuildWindFarm(numSensors, func(externalID int) int {
		return twin.UniformISU(externalID, numISUs)
	})
	jsonBytes, err := json.Marshal(windFarmRoot)
	if err != nil {
		log.Fatalf("Failed to marshal hierarchical Thing tree: %v", err)
	}
	resp, err := dgClient.NewTxn().Mutate(ctx, &api.Mutation{SetJson: jsonBytes, CommitNow: true})
	if err != nil {
		log.Fatalf("Failed to mutate hierarchical Thing tree in Dgraph: %v", err)
	}

	sensorUIDMap = make(map[int]string, numSensors)
	for _, spec := range specs {
		sensorUIDMap[spec.ExternalID] = resp.Uids[twin.SensorBlankNode(spec.ExternalID)]
	}
}

// writeToDgraph executes an immediate 1-to-1 Centralized mutation for a single sensor reading:
// it appends a new Observation node and updates the sensor's materialized Last Known Value.
func writeToDgraph(ctx context.Context, sensorID int, value float64) error {
	uid, exists := sensorUIDMap[sensorID]
	if !exists {
		return fmt.Errorf("uid not found")
	}

	d := twin.ObservationUpdate(uid, []twin.Observation{{Timestamp: time.Now(), Value: value}})
	pb, _ := json.Marshal(d)

	mu := &api.Mutation{SetJson: pb, CommitNow: true}
	_, err := dgClient.NewTxn().Mutate(ctx, mu)
	return err
}

// writeBatchToDgraph commits a micro-batch of sensor readings in a single Dgraph transaction.
// Readings are grouped per sensor so that each sensor's Last Known Value is materialized once
// from its most recent reading in the batch, while every reading is appended as an Observation.
func writeBatchToDgraph(ctx context.Context, events []batchedEvent) error {
	order := make([]int, 0, len(events))
	grouped := make(map[int][]twin.Observation, len(events))
	for _, ev := range events {
		if _, exists := sensorUIDMap[ev.sensorID]; !exists {
			continue
		}
		if _, seen := grouped[ev.sensorID]; !seen {
			order = append(order, ev.sensorID)
		}
		grouped[ev.sensorID] = append(grouped[ev.sensorID], twin.Observation{Timestamp: ev.recvTime, Value: ev.value})
	}
	if len(order) == 0 {
		return nil
	}

	nodes := make([]twin.Thing, 0, len(order))
	for _, sensorID := range order {
		nodes = append(nodes, twin.ObservationUpdate(sensorUIDMap[sensorID], grouped[sensorID]))
	}
	pb, _ := json.Marshal(nodes)
	mu := &api.Mutation{SetJson: pb, CommitNow: true}
	_, err := dgClient.NewTxn().Mutate(ctx, mu)
	return err
}

// writeToTimescaleISU inserts a single sensor telemetry reading into the assigned TimescaleDB ISU
// using its pre-compiled SQL statement.
func writeToTimescaleISU(ctx context.Context, isuIdx int, sensorID int, value float64) error {
	_, err := stmtsTimescale[isuIdx].ExecContext(ctx, sensorID, value)
	return err
}
