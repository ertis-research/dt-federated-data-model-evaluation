package main

import (
	"context"
	"database/sql"
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
	_ "github.com/lib/pq" // Postgres driver
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// --- CONFIG ---
const (
	OutputCSV   = "query_evaluation_results.csv"
	RunsPerTest = 10 // Número de repeticiones para la media
)

// --- STRUCTS ---
type Thing struct {
	Uid        string   `json:"uid,omitempty"`
	DType      []string `json:"dgraph.type,omitempty"`
	Name       string   `json:"name,omitempty"`
	ExternalID int      `json:"external_id,omitempty"`
	Value      string   `json:"value,omitempty"`
}

// --- GLOBALS ---
var (
	pgDB         *sql.DB
	dgClient     *dgo.Dgraph
	PostgresConn = "aaa"
	DgraphTarget = "aaa"
)

func main() {
	log.SetFlags(0)

	// 1. Init Connections
	initDBs()
	defer pgDB.Close()

	// 2. Setup CSV
	file, _ := os.Create(OutputCSV)
	defer file.Close()
	writer := csv.NewWriter(file)
	// Cambiamos la cabecera para reflejar que es la media
	writer.Write([]string{"Num_Sensors", "Architecture", "Avg_Read_Time_ms", "Runs_Count", "Consistent"})
	writer.Flush()

	// 3. Test Scenarios
	sensorCounts := []int{10, 50, 100, 500, 1000, 2000}
	architectures := []string{"Centralized", "Federated"}

	fmt.Printf("Starting READ EVALUATION (%d runs average)...\n", RunsPerTest)

	for _, count := range sensorCounts {
		for _, arch := range architectures {
			fmt.Printf("\n--- Testing %s with %d Sensors ---\n", arch, count)

			// A. SETUP (Se hace UNA vez por escenario para preparar los datos)
			fmt.Print("   > Setting up environment... ")
			expectedValues := setupEnvironment(arch, count)
			performUpdates(arch, expectedValues)
			fmt.Println("Ready.")

			// B. MEASURE (Bucle de 10 ejecuciones)
			var totalDuration float64
			consistent := true

			fmt.Print("   > Running queries: ")
			for r := 0; r < RunsPerTest; r++ {
				// Pequeña pausa para no saturar CPU irrealmente entre queries
				time.Sleep(50 * time.Millisecond)

				start := time.Now()

				// La función que medimos
				retrievedValues := fetchDigitalTwinState(arch)

				duration := time.Since(start).Seconds() * 1000 // ms
				totalDuration += duration

				fmt.Printf(".") // Barra de progreso visual

				// Verificamos consistencia en cada run por seguridad
				if !verifyData(expectedValues, retrievedValues) {
					consistent = false
					log.Printf(" ! Consistency Error in Run %d", r+1)
				}
			}
			fmt.Println(" Done.")

			// C. CALCULAR MEDIA
			avgTime := totalDuration / float64(RunsPerTest)
			fmt.Printf("   > AVG Time: %.4f ms | Consistent: %v\n", avgTime, consistent)

			// Save
			writer.Write([]string{
				strconv.Itoa(count),
				arch,
				fmt.Sprintf("%.4f", avgTime),
				strconv.Itoa(RunsPerTest),
				strconv.FormatBool(consistent),
			})
			writer.Flush()
		}
	}
	fmt.Println("\nAverage Read Evaluation Complete. Saved to", OutputCSV)
}

// ---------------------------------------------------------
// CORE LOGIC: FETCH STATE (Optimized)
// ---------------------------------------------------------

func fetchDigitalTwinState(arch string) map[int]string {
	results := make(map[int]string)
	ctx := context.Background()

	// 1. Dgraph (Igual que siempre)
	q := `
	{
		things(func: type(Thing)) {
			external_id
			value
		}
	}`
	resp, err := dgClient.NewTxn().Query(ctx, q)
	if err != nil {
		log.Fatal(err)
	}

	var decode struct {
		Things []Thing `json:"things"`
	}
	json.Unmarshal(resp.Json, &decode)

	if arch == "Centralized" {
		for _, t := range decode.Things {
			results[t.ExternalID] = t.Value
		}
	} else {
		// --- FEDERATED: CONCURRENT BATCHING ---

		// Recopilar todos los IDs
		var allSensorIDs []int32
		for _, t := range decode.Things {
			allSensorIDs = append(allSensorIDs, int32(t.ExternalID))
		}

		batchSize := 100
		var wg sync.WaitGroup // Para esperar a que acaben las goroutines
		var mu sync.Mutex     // Para proteger la escritura en el mapa 'results'

		for i := 0; i < len(allSensorIDs); i += batchSize {
			end := i + batchSize
			if end > len(allSensorIDs) {
				end = len(allSensorIDs)
			}

			// Copiamos el slice para pasarlo a la goroutine
			chunkIDs := allSensorIDs[i:end]

			if len(chunkIDs) > 0 {
				wg.Add(1) // Avisamos que lanzamos una tarea

				// LANZAMOS LA CONSULTA EN PARALELO
				go func(ids []int32) {
					defer wg.Done() // Al terminar, restamos 1 al contador

					query := `
						SELECT DISTINCT ON (sensor_id) sensor_id, payload
						FROM sensor_read_test
						WHERE sensor_id = ANY($1)
						ORDER BY sensor_id, time DESC
					`

					// La query bloquea SOLO a esta goroutine, no al resto
					rows, err := pgDB.Query(query, pq.Array(ids))
					if err != nil {
						log.Println("Error Async Batch:", err)
						return
					}
					defer rows.Close()

					// Leemos los datos en variables locales primero
					// para no bloquear el mapa principal durante el escaneo
					localResults := make(map[int]string)
					for rows.Next() {
						var id int
						var val string
						if err := rows.Scan(&id, &val); err == nil {
							localResults[id] = val
						}
					}

					// Sección Crítica: Escribir en el mapa global
					// Bloqueamos solo el tiempo justo de copiar los datos
					mu.Lock()
					for k, v := range localResults {
						results[k] = v
					}
					mu.Unlock()

				}(chunkIDs)
			}
		}

		// El programa principal se detiene aquí hasta que todas las consultas terminen
		wg.Wait()
	}
	return results
}

// ---------------------------------------------------------
// HELPER FUNCTIONS
// ---------------------------------------------------------

func initDBs() {
	conn, _ := grpc.Dial(DgraphTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	dgClient = dgo.NewDgraphClient(api.NewDgraphClient(conn))

	var err error
	pgDB, err = sql.Open("postgres", PostgresConn)
	if err != nil {
		log.Fatal(err)
	}

	// Setup Schema & Index
	pgDB.Exec("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
	pgDB.Exec("DROP TABLE IF EXISTS sensor_read_test;")
	pgDB.Exec(`CREATE TABLE sensor_read_test (
		time TIMESTAMPTZ NOT NULL,
		sensor_id INT,
		payload TEXT
	);`)
	pgDB.Exec("SELECT create_hypertable('sensor_read_test', 'time');")

	// Índice crucial para que el DISTINCT ON sea rápido
	pgDB.Exec("CREATE INDEX ON sensor_read_test (sensor_id, time DESC);")
}

func setupEnvironment(arch string, count int) map[int]string {
	ctx := context.Background()
	dgClient.Alter(ctx, &api.Operation{DropAll: true})
	pgDB.Exec("TRUNCATE TABLE sensor_read_test")

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
		tx, _ := pgDB.Begin()
		stmtIns, _ := tx.Prepare("INSERT INTO sensor_read_test (time, sensor_id, payload) VALUES (NOW(), $1, $2)")
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
