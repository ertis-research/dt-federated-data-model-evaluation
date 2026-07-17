package main

import (
	"context"
	"database/sql"
	"dt-real-bench/config"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ======================================================================================
// CONFIGURATION & CONSTANTS
// ======================================================================================

const (
	TopicPrefix       = "dt/sensor/"
	OutputCSV         = "output/ingestion_evaluation_results2.csv"
	TotalRuns         = 5
	TestDurationSec   = 120 // Test duration
	WarmupDurationSec = 60  // Warm-up duration
	QoSLevel          = 1   // At least once
)

type PromResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  []interface{}     `json:"value"` // Ej: [1678886400, "1.245"]
		} `json:"result"`
	} `json:"data"`
}

// Global Infrastructure
var (
	cfg           config.Env
	pgDB          *sql.DB
	stmtTimescale *sql.Stmt
	dgClient      *dgo.Dgraph
	sensorUIDMap  map[int]string
)

// Structs
type Scenario struct {
	StepID          int
	NumSensors      int
	FrequencyHz     float64
	GraphComplexity int
}

type Result struct {
	StepID            int
	NumSensors        int
	Architecture      string
	RunID             int
	FrequencyHz       float64
	AvgLatencyMs      float64
	MessagesSent      int64
	MessagesLost      int64
	DbErrors          int64
	ThroughputMsgSec  float64
	TargetCPUAvgCores float64
	TargetRAMMaxMB    float64
}

type SensorNode struct {
	Uid   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
	Name  string   `json:"name,omitempty"`
	Value string   `json:"value,omitempty"`
}

// ======================================================================================
// SECTION I: EXPERIMENT CONTROL
// ======================================================================================

func main() {
	log.SetFlags(0)
	rand.Seed(time.Now().UnixNano())
	cfg = config.LoadEnv()

	initInfrastructure()
	defer pgDB.Close()

	writer := initCSVWriter()
	defer writer.Flush()

	scenarios := []Scenario{
		{StepID: 1, NumSensors: 100, FrequencyHz: 0.1, GraphComplexity: 200},
		{StepID: 2, NumSensors: 500, FrequencyHz: 0.1, GraphComplexity: 500},
		{StepID: 3, NumSensors: 1000, FrequencyHz: 0.1, GraphComplexity: 1000},
		{StepID: 4, NumSensors: 2000, FrequencyHz: 0.2, GraphComplexity: 2000},
		{StepID: 5, NumSensors: 5000, FrequencyHz: 0.2, GraphComplexity: 5000},
	}

	architectures := []string{"Federated", "Centralized"}

	fmt.Println("Starting INGESTION EVALUATION...")
	fmt.Printf("QoS: %d | Duration: %d sec\n", QoSLevel, TestDurationSec)

	samplePayload := fmt.Sprintf("%d", time.Now().UnixNano())
	fmt.Printf("Application Payload Size: %d bytes (Sample: %s)\n", len(samplePayload), samplePayload)

	fmt.Println("\n>>> SYSTEM WARM-UP (60 seconds) <<<")
	warmupSc := Scenario{StepID: 0, NumSensors: 500, FrequencyHz: 1.0, GraphComplexity: 1000}
	prepareEnvironment(warmupSc.GraphComplexity, warmupSc.NumSensors)
	executeTestRun(warmupSc, "Centralized", 0, true)
	fmt.Println(">>> Warm-up Complete. Cooling down... <<<")
	time.Sleep(3 * time.Second)

	for _, sc := range scenarios {
		fmt.Printf("\n>>> STEP %d: %d Sensors @ %.2f Hz (%d Nodes) <<<\n",
			sc.StepID, sc.NumSensors, sc.FrequencyHz, sc.GraphComplexity)

		prepareEnvironment(sc.GraphComplexity, sc.NumSensors)

		for _, arch := range architectures {
			fmt.Printf("--- Arch: %s ---\n", arch)
			for run := 1; run <= TotalRuns; run++ {

				res := executeTestRun(sc, arch, run, false)

				writer.Write([]string{
					strconv.Itoa(res.StepID),
					strconv.Itoa(res.NumSensors),
					fmt.Sprintf("%.2f", res.FrequencyHz),
					arch,
					strconv.Itoa(run),
					strconv.Itoa(sc.GraphComplexity),
					fmt.Sprintf("%.4f", res.AvgLatencyMs),
					strconv.FormatInt(res.MessagesLost, 10),
					strconv.FormatInt(res.DbErrors, 10),
					fmt.Sprintf("%.2f", res.ThroughputMsgSec),
					fmt.Sprintf("%.3f", res.TargetCPUAvgCores),
					fmt.Sprintf("%.2f", res.TargetRAMMaxMB),
				})
				writer.Flush()

				fmt.Printf("   > Run %d: Sent=%d | Lost=%d | DB_Err=%d | Lat=%.2fms | CPU=%.3fc | RAM=%.2fMB\n",
					run, res.MessagesSent, res.MessagesLost, res.DbErrors, res.AvgLatencyMs, res.TargetCPUAvgCores, res.TargetRAMMaxMB)

				time.Sleep(2 * time.Second)
			}
		}
	}
	fmt.Println("\nIngestion Evaluation Complete.")
}

// ======================================================================================
// SECTION II: ORCHESTRATION & MEASUREMENT
// ======================================================================================

func executeTestRun(sc Scenario, arch string, runID int, isWarmup bool) Result {
	var totalLatency int64 = 0
	var msgReceivedCount int64 = 0
	var msgSentCount int64 = 0
	var dbErrorCount int64 = 0

	var muMetrics sync.Mutex
	var wgServer sync.WaitGroup

	duration := TestDurationSec
	if isWarmup {
		duration = WarmupDurationSec
	}

	done := make(chan bool)

	// --- A. SERVER SIDE ---
	serverOpts := createMQTTOpts(fmt.Sprintf("srv_%s_%d_%d", arch, sc.NumSensors, runID))
	serverOpts.SetOrderMatters(false) // Permite procesamiento asíncrono real
	serverOpts.SetMessageChannelDepth(10000)

	serverOpts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {

		// Extraer los datos de la red lo más rápido posible
		payloadStr := string(msg.Payload())
		topic := msg.Topic()

		// Procesar asíncronamente para no bloquear el router de MQTT
		wgServer.Add(1)
		go func(pStr, tStr string) {
			defer wgServer.Done()
			sentTimeNano, _ := strconv.ParseInt(pStr, 10, 64)
			sentTime := time.Unix(0, sentTimeNano)

			parts := strings.Split(tStr, "/")
			sensorID, _ := strconv.Atoi(parts[len(parts)-1])

			var err error
			if arch == "Centralized" {
				err = writeToDgraph(sensorID, pStr)
			} else {
				err = writeToTimescale(pStr)
			}

			if err != nil {
				atomic.AddInt64(&dbErrorCount, 1)
				// Opcional: log.Printf("DB Error: %v", err)
				return
			}

			latencyMicro := time.Since(sentTime).Microseconds()

			muMetrics.Lock()
			totalLatency += latencyMicro
			msgReceivedCount++
			muMetrics.Unlock()
		}(payloadStr, topic) // Pasar variables por valor a la gorutina
	})

	srvClient := mqtt.NewClient(serverOpts)
	if token := srvClient.Connect(); token.Wait() && token.Error() != nil {
		log.Printf("Server Connect Error: %v", token.Error())
		return Result{}
	}
	runTopicPrefix := fmt.Sprintf("%s%s/step%d/run%d/", TopicPrefix, arch, sc.StepID, runID)
	srvClient.Subscribe(runTopicPrefix+"#", QoSLevel, nil)
	time.Sleep(1 * time.Second)

	// --- B. CLIENT SIDE ---
	var wgSensors sync.WaitGroup

	tickerInterval := time.Duration(1000.0/sc.FrequencyHz) * time.Millisecond

	startTime := time.Now()

	for i := 0; i < sc.NumSensors; i++ {
		wgSensors.Add(1)
		go func(id int) {
			defer wgSensors.Done()

			cOpts := createMQTTOpts(fmt.Sprintf("s_%d_%d_%d_%d", sc.StepID, sc.NumSensors, runID, id))
			c := mqtt.NewClient(cOpts)
			if token := c.Connect(); token.Wait() && token.Error() != nil {
				log.Printf("Sensor %d fails connection: %v", id, token.Error())
				return
			}
			defer c.Disconnect(0)

			time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

			ticker := time.NewTicker(tickerInterval)
			timeout := time.After(time.Duration(duration) * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-timeout:
					return
				case <-ticker.C:
					payload := fmt.Sprintf("%d", time.Now().UnixNano())
					runTopicPrefix := fmt.Sprintf("%s%s/step%d/run%d/", TopicPrefix, arch, sc.StepID, runID)
					t := c.Publish(runTopicPrefix+strconv.Itoa(id), QoSLevel, false, payload)
					t.Wait()

					if t.Error() == nil {
						muMetrics.Lock()
						msgSentCount++
						muMetrics.Unlock()
					}
				}
			}
		}(i)
	}

	wgSensors.Wait()
	time.Sleep(2 * time.Second)

	fmt.Println("   > Sensors finished. Waiting for database to securely process queue...")
	serverDone := make(chan struct{})
	go func() {
		wgServer.Wait() // Espera a que todas las inserciones en BD terminen
		close(serverDone)
	}()

	// Damos un margen amplio (hasta 4 minutos) para evitar que las gorutinas se fuguen al siguiente run.
	select {
	case <-serverDone:
		fmt.Println("   > All messages successfully written to DB.")
	case <-time.After(4 * time.Minute):
		fmt.Println("   > [WARNING] DB drain timeout reached (4 min). DB is strictly bottlenecked.")
	}

	endTime := time.Now() // Calculamos el tiempo total de estrés de la BD
	srvClient.Disconnect(250)
	close(done)

	var cpuAvg, ramMaxMB float64
	if !isWarmup {
		fmt.Println("   > Waiting for Prometheus scraping (15s)...")
		time.Sleep(15 * time.Second)

		promURL := cfg.PrometheusURI
		myNamespace := cfg.KubernetesNamespace

		podRegex := "timescaledb-.*"
		if arch == "Centralized" {
			podRegex = "dgraph-dgraph-alpha-.*"
		}

		windowSec := int(endTime.Sub(startTime).Seconds())

		cpuQuery := fmt.Sprintf(`sum(rate(container_cpu_usage_seconds_total{namespace="%s", pod=~"%s", container!="POD", container!=""}[%ds]))`, myNamespace, podRegex, windowSec)
		ramQuery := fmt.Sprintf(`sum(max_over_time(container_memory_working_set_bytes{namespace="%s", pod=~"%s", container!="POD", container!=""}[%ds])) / 1024 / 1024`, myNamespace, podRegex, windowSec)

		var errCPU, errRAM error
		cpuAvg, errCPU = queryPrometheus(promURL, cpuQuery, endTime)
		if errCPU != nil {
			fmt.Printf("   [!] Error Prometheus CPU: %v\n", errCPU)
		}

		ramMaxMB, errRAM = queryPrometheus(promURL, ramQuery, endTime)
		if errRAM != nil {
			fmt.Printf("   [!] Error Prometheus RAM: %v\n", errRAM)
		}
	}

	// --- C. STATS ---
	avgLat := 0.0
	if msgReceivedCount > 0 {
		avgLat = float64(totalLatency) / float64(msgReceivedCount) / 1000.0
	}

	throughput := float64(msgReceivedCount) / float64(duration)

	return Result{
		StepID:            sc.StepID,
		NumSensors:        sc.NumSensors,
		FrequencyHz:       sc.FrequencyHz,
		Architecture:      arch,
		RunID:             runID,
		AvgLatencyMs:      avgLat,
		MessagesSent:      msgSentCount,
		MessagesLost:      msgSentCount - msgReceivedCount - atomic.LoadInt64(&dbErrorCount),
		DbErrors:          atomic.LoadInt64(&dbErrorCount),
		ThroughputMsgSec:  throughput,
		TargetCPUAvgCores: cpuAvg,
		TargetRAMMaxMB:    ramMaxMB,
	}
}

// ======================================================================================
// SECTION III: PERSISTENCE LOGIC
// ======================================================================================

func writeToDgraph(sensorID int, val string) error {
	uid, exists := sensorUIDMap[sensorID]
	if !exists {
		return fmt.Errorf("uid not found")
	}

	d := SensorNode{Uid: uid, Value: val}
	pb, _ := json.Marshal(d)

	mu := &api.Mutation{SetJson: pb, CommitNow: true}
	_, err := dgClient.NewTxn().Mutate(context.Background(), mu)
	return err
}

func writeToTimescale(val string) error {
	_, err := stmtTimescale.Exec(val)
	return err
}

func prepareEnvironment(totalNodes int, activeSensors int) {
	ctx := context.Background()
	dgClient.Alter(ctx, &api.Operation{DropAll: true})
	pgDB.Exec("TRUNCATE TABLE ingestion_test")

	op := &api.Operation{Schema: `
		name: string @index(hash) .
		value: string .
		type Thing {
			name
			value
		}
	`}
	dgClient.Alter(ctx, op)

	sensorUIDMap = make(map[int]string)
	var allNodes []SensorNode

	for i := 0; i < totalNodes; i++ {
		allNodes = append(allNodes, SensorNode{
			Uid:   fmt.Sprintf("_:node%d", i),
			DType: []string{"Thing"},
			Name:  fmt.Sprintf("Thing_%d", i),
			Value: "init",
		})
	}

	batchSize := 2000
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
			log.Fatal(err)
		}
		for j := i; j < end; j++ {
			if j < activeSensors {
				sensorUIDMap[j] = resp.Uids[fmt.Sprintf("node%d", j)]
			}
		}
	}
}

func initInfrastructure() {
	conn, _ := grpc.Dial(cfg.DgraphURI, grpc.WithTransportCredentials(insecure.NewCredentials()))
	dgClient = dgo.NewDgraphClient(api.NewDgraphClient(conn))

	var err error
	pgDB, err = sql.Open("postgres", cfg.PostgresConn())
	if err != nil {
		log.Fatal(err)
	}

	pgDB.SetMaxOpenConns(100)
	pgDB.SetMaxIdleConns(20)

	pgDB.Exec("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
	pgDB.Exec("CREATE TABLE IF NOT EXISTS ingestion_test (time TIMESTAMPTZ NOT NULL, payload TEXT);")
	pgDB.Exec("SELECT create_hypertable('ingestion_test', 'time', if_not_exists => TRUE);")

	stmtTimescale, err = pgDB.Prepare("INSERT INTO ingestion_test (time, payload) VALUES (NOW(), $1)")
	if err != nil {
		log.Fatal(err)
	}
}

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

func initCSVWriter() *csv.Writer {
	file, _ := os.Create(OutputCSV)
	writer := csv.NewWriter(file)
	writer.Write([]string{
		"Step_ID", "Num_Sensors", "Freq_Hz", "Architecture", "Run_ID", "Graph_Complexity",
		"Avg_Latency_ms", "Packet_Loss_Network", "DB_Errors", "Throughput_msg_sec",
		"DB_CPU_Avg_Cores", "DB_RAM_Max_MB",
	})
	writer.Flush()
	return writer
}

func queryPrometheus(promURL string, query string, evalTime time.Time) (float64, error) {
	// Construir la URL con los parámetros
	reqURL := fmt.Sprintf("%s/api/v1/query?query=%s&time=%d",
		promURL,
		url.QueryEscape(query),
		evalTime.Unix())

	resp, err := http.Get(reqURL)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("error HTTP Prometheus: %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	var promData PromResponse
	if err := json.Unmarshal(body, &promData); err != nil {
		return 0, err
	}

	// Extraer el valor flotante del primer resultado
	if len(promData.Data.Result) > 0 && len(promData.Data.Result[0].Value) == 2 {
		valStr := fmt.Sprintf("%v", promData.Data.Result[0].Value[1])
		return strconv.ParseFloat(valStr, 64)
	}

	return 0, fmt.Errorf("no data returned from Prometheus")
}
