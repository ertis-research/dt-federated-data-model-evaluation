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
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"sort"
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
	OutputCSV         = "output/ingestion_evaluation_results_v2.csv"
	TotalRuns         = 3
	TestDurationSec   = 30 // 30s steady-state per run
	WarmupDurationSec = 15 // Warm-up duration
	DBDrainTimeoutSec = 20 // Max wait time for DB queue drain after sensors stop
	PromScrapeWaitSec = 10 // Wait time for kubelet cAdvisor & Prometheus scrape
	MinPromWindowSec  = 60 // Minimum Prometheus rate() window in seconds to guarantee >=2 kubelet samples
	QoSLevel          = 1  // At least once

	// Batching parameters for Centralized_Batched baseline
	BatchMaxSize  = 100
	BatchMaxFlush = 100 * time.Millisecond
)

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

// Global Infrastructure
var (
	cfg            config.Env
	pgDBs          []*sql.DB
	stmtsTimescale []*sql.Stmt
	dgClient       *dgo.Dgraph
	sensorUIDMap   map[int]string
)

// Structs
type Scenario struct {
	StepID          int
	NumSensors      int
	FrequencyHz     float64
	GraphComplexity int
}

type ArchConfig struct {
	Name    string
	Mode    string // "Centralized_1to1", "Centralized_Batched", "Federated"
	NumISUs int    // 0 for centralized, 1, 2, 4, 8 for federated
}

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

type batchedEvent struct {
	sensorID int
	payload  string
	sentTime time.Time
}

// ======================================================================================
// SECTION I: EXPERIMENT CONTROL
// ======================================================================================

func main() {
	log.SetFlags(0)
	rand.Seed(time.Now().UnixNano())
	cfg = config.LoadEnv()

	initInfrastructure()
	defer closeInfrastructure()

	writer := initCSVWriter()
	defer writer.Flush()

	scenarios := []Scenario{
		{StepID: 1, NumSensors: 100, FrequencyHz: 0.1, GraphComplexity: 200},
		{StepID: 2, NumSensors: 500, FrequencyHz: 0.1, GraphComplexity: 500},
		{StepID: 3, NumSensors: 1000, FrequencyHz: 0.1, GraphComplexity: 1000},
		{StepID: 4, NumSensors: 2000, FrequencyHz: 0.2, GraphComplexity: 2000},
		{StepID: 5, NumSensors: 5000, FrequencyHz: 0.2, GraphComplexity: 5000},
		{StepID: 6, NumSensors: 5000, FrequencyHz: 1.0, GraphComplexity: 5000},
	}

	architectures := []ArchConfig{
		{Name: "Centralized_1to1", Mode: "Centralized_1to1", NumISUs: 0},
		{Name: "Centralized_Batched", Mode: "Centralized_Batched", NumISUs: 0},
		{Name: "Federated_1ISU", Mode: "Federated", NumISUs: 1},
		{Name: "Federated_2ISU", Mode: "Federated", NumISUs: 2},
		{Name: "Federated_4ISU", Mode: "Federated", NumISUs: 4},
		{Name: "Federated_8ISU", Mode: "Federated", NumISUs: 8},
	}

	fmt.Println("Starting MULTI-ISU & ABLATION INGESTION EVALUATION...")
	fmt.Printf("QoS: %d | Duration: %d sec | Max Configured ISUs: %d\n", QoSLevel, TestDurationSec, len(pgDBs))

	samplePayload := fmt.Sprintf("%d", time.Now().UnixNano())
	fmt.Printf("Application Payload Size: %d bytes (Sample: %s)\n", len(samplePayload), samplePayload)

	fmt.Println("\n>>> SYSTEM WARM-UP (30 seconds) <<<")
	warmupSc := Scenario{StepID: 0, NumSensors: 500, FrequencyHz: 0.5, GraphComplexity: 1000}
	prepareEnvironment(warmupSc.GraphComplexity, warmupSc.NumSensors)
	executeTestRun(warmupSc, ArchConfig{Name: "Warmup", Mode: "Federated", NumISUs: len(pgDBs)}, 0, true)
	fmt.Println(">>> Warm-up Complete. Cooling down... <<<")
	time.Sleep(3 * time.Second)

	for _, sc := range scenarios {
		fmt.Printf("\n>>> STEP %d: %d Sensors @ %.2f Hz (%d Nodes) <<<\n",
			sc.StepID, sc.NumSensors, sc.FrequencyHz, sc.GraphComplexity)

		prepareEnvironment(sc.GraphComplexity, sc.NumSensors)

		for _, arch := range architectures {
			if arch.Mode == "Federated" && arch.NumISUs > len(pgDBs) {
				fmt.Printf("   [SKIP] %s requires %d ISUs, but only %d configured.\n", arch.Name, arch.NumISUs, len(pgDBs))
				continue
			}

			fmt.Printf("--- Arch: %s (ISUs: %d) ---\n", arch.Name, arch.NumISUs)
			for run := 1; run <= TotalRuns; run++ {

				res := executeTestRun(sc, arch, run, false)

				writer.Write([]string{
					strconv.Itoa(res.StepID),
					strconv.Itoa(res.NumSensors),
					fmt.Sprintf("%.2f", res.FrequencyHz),
					res.Architecture,
					strconv.Itoa(res.NumISUs),
					strconv.Itoa(run),
					strconv.Itoa(sc.GraphComplexity),
					fmt.Sprintf("%.4f", res.AvgLatencyMs),
					fmt.Sprintf("%.4f", res.P50LatencyMs),
					fmt.Sprintf("%.4f", res.P95LatencyMs),
					fmt.Sprintf("%.4f", res.P99LatencyMs),
					strconv.FormatInt(res.MessagesLost, 10),
					strconv.FormatInt(res.DbErrors, 10),
					fmt.Sprintf("%.2f", res.ThroughputMsgSec),
					fmt.Sprintf("%.3f", res.TargetCPUAvgCores),
					fmt.Sprintf("%.2f", res.TargetRAMMaxMB),
				})
				writer.Flush()

				fmt.Printf("   > Run %d: Sent=%d | Lost=%d | DB_Err=%d | Avg=%.2fms | P95=%.2fms | P99=%.2fms | Tput=%.1f msg/s | CPU=%.3fc | RAM=%.2fMB\n",
					run, res.MessagesSent, res.MessagesLost, res.DbErrors,
					res.AvgLatencyMs, res.P95LatencyMs, res.P99LatencyMs,
					res.ThroughputMsgSec, res.TargetCPUAvgCores, res.TargetRAMMaxMB)

				time.Sleep(2 * time.Second)
			}
		}
	}
	fmt.Println("\nIngestion Evaluation Complete.")
}

// ======================================================================================
// SECTION II: ORCHESTRATION & MEASUREMENT
// ======================================================================================

func executeTestRun(sc Scenario, arch ArchConfig, runID int, isWarmup bool) Result {
	var totalLatencyMicro int64 = 0
	var msgReceivedCount int64 = 0
	var msgSentCount int64 = 0
	var dbErrorCount int64 = 0

	expectedMessages := int(float64(sc.NumSensors)*sc.FrequencyHz*float64(TestDurationSec)) + 1000
	latenciesMs := make([]float64, 0, expectedMessages)

	var muMetrics sync.Mutex
	var wgServer sync.WaitGroup

	duration := TestDurationSec
	if isWarmup {
		duration = WarmupDurationSec
	}

 	// Setup batch channel if using Centralized_Batched
	var batchChan chan batchedEvent
	var wgBatcher sync.WaitGroup
	if arch.Mode == "Centralized_Batched" {
		batchChan = make(chan batchedEvent, 20000)
		numBatchWorkers := 4
		for w := 0; w < numBatchWorkers; w++ {
			wgBatcher.Add(1)
			go func() {
				defer wgBatcher.Done()
				buffer := make([]batchedEvent, 0, BatchMaxSize)
				flushTicker := time.NewTicker(BatchMaxFlush)
				defer flushTicker.Stop()

				flush := func() {
					if len(buffer) == 0 {
						return
					}
					err := writeBatchToDgraph(buffer)
					now := time.Now()
					if err != nil {
						atomic.AddInt64(&dbErrorCount, int64(len(buffer)))
					} else {
						muMetrics.Lock()
						for _, ev := range buffer {
							latMicro := now.Sub(ev.sentTime).Microseconds()
							totalLatencyMicro += latMicro
							msgReceivedCount++
							latenciesMs = append(latenciesMs, float64(latMicro)/1000.0)
						}
						muMetrics.Unlock()
					}
					buffer = buffer[:0]
				}

				for {
					select {
					case ev, ok := <-batchChan:
						if !ok {
							flush()
							return
						}
						buffer = append(buffer, ev)
						if len(buffer) >= BatchMaxSize {
							flush()
						}
					case <-flushTicker.C:
						flush()
					}
				}
			}()
		}
	}

	// --- A. SERVER SIDE ---
	serverOpts := createMQTTOpts(fmt.Sprintf("srv_%s_%d_%d", arch.Name, sc.NumSensors, runID))
	serverOpts.SetOrderMatters(false)
	serverOpts.SetMessageChannelDepth(20000)

	serverOpts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		payloadStr := string(msg.Payload())
		topic := msg.Topic()

		wgServer.Add(1)
		go func(pStr, tStr string) {
			defer wgServer.Done()
			sentTimeNano, _ := strconv.ParseInt(pStr, 10, 64)
			sentTime := time.Unix(0, sentTimeNano)

			parts := strings.Split(tStr, "/")
			sensorID, _ := strconv.Atoi(parts[len(parts)-1])

			if arch.Mode == "Centralized_Batched" {
				batchChan <- batchedEvent{
					sensorID: sensorID,
					payload:  pStr,
					sentTime: sentTime,
				}
				return
			}

			var err error
			if arch.Mode == "Centralized_1to1" {
				err = writeToDgraph(sensorID, pStr)
			} else {
				isuIdx := sensorID % arch.NumISUs
				err = writeToTimescaleISU(isuIdx, sensorID, pStr)
			}

			if err != nil {
				atomic.AddInt64(&dbErrorCount, 1)
				return
			}

			latencyMicro := time.Since(sentTime).Microseconds()

			muMetrics.Lock()
			totalLatencyMicro += latencyMicro
			msgReceivedCount++
			latenciesMs = append(latenciesMs, float64(latencyMicro)/1000.0)
			muMetrics.Unlock()
		}(payloadStr, topic)
	})

	srvClient := mqtt.NewClient(serverOpts)
	if token := srvClient.Connect(); token.Wait() && token.Error() != nil {
		log.Printf("Server Connect Error: %v", token.Error())
		return Result{}
	}
	runTopicPrefix := fmt.Sprintf("%s%s/step%d/run%d/", TopicPrefix, arch.Name, sc.StepID, runID)
	if subToken := srvClient.Subscribe(runTopicPrefix+"#", QoSLevel, nil); subToken.Wait() && subToken.Error() != nil {
		log.Printf("Server Subscribe Error: %v", subToken.Error())
		return Result{}
	}
	time.Sleep(500 * time.Millisecond)

	// --- B. CLIENT SIDE ---
	var wgReady sync.WaitGroup
	var wgSensors sync.WaitGroup
	startSignal := make(chan struct{})
	tickerInterval := time.Duration(1000.0/sc.FrequencyHz) * time.Millisecond

	for i := 0; i < sc.NumSensors; i++ {
		wgReady.Add(1)
		wgSensors.Add(1)
		go func(id int) {
			defer wgSensors.Done()

			cOpts := createMQTTOpts(fmt.Sprintf("s_%d_%d_%d_%d", sc.StepID, sc.NumSensors, runID, id))
			c := mqtt.NewClient(cOpts)
			if token := c.Connect(); token.Wait() && token.Error() != nil {
				log.Printf("Sensor %d fails connection: %v", id, token.Error())
				wgReady.Done()
				return
			}
			defer c.Disconnect(0)

			wgReady.Done()
			<-startSignal // Wait until all sensors are connected and server subscription is active

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
					t := c.Publish(runTopicPrefix+strconv.Itoa(id), QoSLevel, false, payload)
					t.Wait()

					if t.Error() == nil {
						atomic.AddInt64(&msgSentCount, 1)
					}
				}
			}
		}(i)
	}

	// Wait for all sensor MQTT clients to finish TCP/MQTT handshake before starting the timer
	wgReady.Wait()
	startTime := time.Now()
	close(startSignal)

	wgSensors.Wait()
	time.Sleep(2 * time.Second)

	fmt.Println("   > Sensors finished. Waiting for database to securely process queue...")
	serverDone := make(chan struct{})
	go func() {
		wgServer.Wait()
		if arch.Mode == "Centralized_Batched" {
			close(batchChan)
			wgBatcher.Wait()
		}
		close(serverDone)
	}()

	select {
	case <-serverDone:
		fmt.Println("   > All messages successfully written to DB.")
	case <-time.After(time.Duration(DBDrainTimeoutSec) * time.Second):
		fmt.Printf("   > [WARNING] DB drain timeout reached (%ds). DB is strictly bottlenecked.\n", DBDrainTimeoutSec)
	}

	endTime := time.Now()
	srvClient.Disconnect(250)

	var cpuAvg, ramMaxMB float64
	if !isWarmup {
		fmt.Printf("   > Waiting for Prometheus scraping (%ds)...\n", PromScrapeWaitSec)
		time.Sleep(time.Duration(PromScrapeWaitSec) * time.Second)

		promURL := cfg.PrometheusURI
		myNamespace := cfg.KubernetesNamespace

		podRegex := buildPodRegex(arch)
		windowSec := int(endTime.Sub(startTime).Seconds())
		if windowSec < MinPromWindowSec {
			windowSec = MinPromWindowSec
		}

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

	// --- C. STATS & PERCENTILES ---
	avgLat, p50Lat, p95Lat, p99Lat := computeLatencyStats(latenciesMs, totalLatencyMicro, msgReceivedCount)
	throughput := float64(msgReceivedCount) / float64(duration)
	sent := atomic.LoadInt64(&msgSentCount)
	dbErrs := atomic.LoadInt64(&dbErrorCount)
	lost := sent - msgReceivedCount - dbErrs
	if lost < 0 {
		lost = 0
	}

	return Result{
		StepID:            sc.StepID,
		NumSensors:        sc.NumSensors,
		FrequencyHz:       sc.FrequencyHz,
		Architecture:      arch.Name,
		NumISUs:           arch.NumISUs,
		RunID:             runID,
		AvgLatencyMs:      avgLat,
		P50LatencyMs:      p50Lat,
		P95LatencyMs:      p95Lat,
		P99LatencyMs:      p99Lat,
		MessagesSent:      sent,
		MessagesLost:      lost,
		DbErrors:          dbErrs,
		ThroughputMsgSec:  throughput,
		TargetCPUAvgCores: cpuAvg,
		TargetRAMMaxMB:    ramMaxMB,
	}
}

func computeLatencyStats(samples []float64, totalMicro int64, count int64) (avg, p50, p95, p99 float64) {
	if count == 0 || len(samples) == 0 {
		return 0, 0, 0, 0
	}
	avg = float64(totalMicro) / float64(count) / 1000.0
	sort.Float64s(samples)
	n := len(samples)
	p50 = samples[int(math.Min(float64(n-1), float64(n)*0.50))]
	p95 = samples[int(math.Min(float64(n-1), float64(n)*0.95))]
	p99 = samples[int(math.Min(float64(n-1), float64(n)*0.99))]
	return
}

func buildPodRegex(arch ArchConfig) string {
	if arch.Mode != "Federated" {
		return "dgraph-dgraph-alpha-.*"
	}
	var pods []string
	for i := 0; i < arch.NumISUs; i++ {
		pods = append(pods, fmt.Sprintf("timescaledb-%d", i))
	}
	return "^(" + strings.Join(pods, "|") + ")$"
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

func writeBatchToDgraph(events []batchedEvent) error {
	nodes := make([]SensorNode, 0, len(events))
	for _, ev := range events {
		if uid, exists := sensorUIDMap[ev.sensorID]; exists {
			nodes = append(nodes, SensorNode{Uid: uid, Value: ev.payload})
		}
	}
	if len(nodes) == 0 {
		return nil
	}
	pb, _ := json.Marshal(nodes)
	mu := &api.Mutation{SetJson: pb, CommitNow: true}
	_, err := dgClient.NewTxn().Mutate(context.Background(), mu)
	return err
}

func writeToTimescaleISU(isuIdx int, sensorID int, val string) error {
	_, err := pgDBs[isuIdx].Exec("INSERT INTO ingestion_test (time, sensor_id, payload) VALUES (NOW(), $1, $2)", sensorID, val)
	return err
}

func prepareEnvironment(totalNodes int, activeSensors int) {
	ctx := context.Background()
	dgClient.Alter(ctx, &api.Operation{DropAll: true})
	for i, db := range pgDBs {
		db.Exec("TRUNCATE TABLE ingestion_test")
		// Insert 1 seed row so TimescaleDB pre-allocates the current time chunk before Run 1 starts
		stmtsTimescale[i].Exec(0, "chunk_init")
	}

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
		db.SetMaxOpenConns(100)
		db.SetMaxIdleConns(20)

		db.Exec("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
		db.Exec("CREATE TABLE IF NOT EXISTS ingestion_test (time TIMESTAMPTZ NOT NULL, sensor_id INT, payload TEXT);")
		db.Exec("SELECT create_hypertable('ingestion_test', 'time', if_not_exists => TRUE);")

		stmt, err := db.Prepare("INSERT INTO ingestion_test (time, sensor_id, payload) VALUES (NOW(), $1, $2)")
		if err != nil {
			log.Fatalf("Failed to prepare statement on ISU %d: %v", i, err)
		}
		pgDBs[i] = db
		stmtsTimescale[i] = stmt
	}
}

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
	os.MkdirAll("output", 0755)
	file, err := os.Create(OutputCSV)
	if err != nil {
		log.Fatalf("Cannot create output CSV: %v", err)
	}
	writer := csv.NewWriter(file)
	writer.Write([]string{
		"Step_ID", "Num_Sensors", "Freq_Hz", "Architecture", "Num_ISUs", "Run_ID", "Graph_Complexity",
		"Avg_Latency_ms", "P50_Latency_ms", "P95_Latency_ms", "P99_Latency_ms",
		"Packet_Loss_Network", "DB_Errors", "Throughput_msg_sec",
		"DB_CPU_Avg_Cores", "DB_RAM_Max_MB",
	})
	writer.Flush()
	return writer
}

func queryPrometheus(promURL string, query string, evalTime time.Time) (float64, error) {
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

	if len(promData.Data.Result) > 0 && len(promData.Data.Result[0].Value) == 2 {
		valStr := fmt.Sprintf("%v", promData.Data.Result[0].Value[1])
		return strconv.ParseFloat(valStr, 64)
	}

	return 0, fmt.Errorf("no data returned from Prometheus")
}