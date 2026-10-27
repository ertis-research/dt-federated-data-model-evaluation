package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"dt-real-bench/twin"
)

// ======================================================================================
// INGESTION ORCHESTRATION, LATENCY MEASUREMENT & PROMETHEUS TELEMETRY ENGINE
// ======================================================================================

// executeTestRun orchestrates a single ingestion evaluation run (or warm-up phase):
//  1. Initializes background micro-batch workers (when in Centralized_Batched mode).
//  2. Starts the server-side MQTT subscriber pool that routes incoming sensor payloads to
//     Dgraph or the partitioned TimescaleDB ISUs.
//  3. Launches `sc.NumSensors` concurrent MQTT publisher goroutines at `sc.FrequencyHz`.
//  4. Waits for in-flight database writes to drain and queries Prometheus for container CPU/RAM.
//
// Every published message is reconciled against the persisted ones. Messages that are not
// persisted before the drain cutoff are counted as undelivered and kept in the latency
// distribution with a right-censored latency (cutoff - sent time), so tail percentiles of a
// saturated architecture are not computed only over the messages that survived.
func executeTestRun(sc Scenario, arch ArchConfig, runID int, isWarmup bool) Result {
	var msgSentCount int64 = 0
	var dbErrorCount int64 = 0

	expectedMessages := int(float64(sc.NumSensors)*sc.FrequencyHz*float64(TestDurationSec)) + 1000
	latenciesMs := make([]float64, 0, expectedMessages)
	persisted := make(map[msgKey]int64, expectedMessages) // message -> persist completion time (UnixNano)
	failed := make(map[msgKey]struct{})
	sentLog := make([][]int64, sc.NumSensors+1) // sensorID (1..N) -> publish timestamps (UnixNano)

	var muMetrics sync.Mutex
	var wgServer sync.WaitGroup

	recordPersisted := func(key msgKey, sentTime, now time.Time) {
		muMetrics.Lock()
		persisted[key] = now.UnixNano()
		latenciesMs = append(latenciesMs, float64(now.Sub(sentTime).Microseconds())/1000.0)
		muMetrics.Unlock()
	}
	recordFailed := func(key msgKey) {
		atomic.AddInt64(&dbErrorCount, 1)
		muMetrics.Lock()
		failed[key] = struct{}{}
		muMetrics.Unlock()
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	// Bound concurrent in-flight 1-to-1 Dgraph gRPC mutations so extreme bursts queue cleanly in Go
	dgraphSem := make(chan struct{}, 256)

	duration := TestDurationSec
	if isWarmup {
		duration = WarmupDurationSec
	}

	// Setup batch channel and worker pool if using Centralized_Batched
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
					if len(buffer) == 0 || runCtx.Err() != nil {
						buffer = buffer[:0]
						return
					}
					err := writeBatchToDgraph(runCtx, buffer)
					if runCtx.Err() != nil {
						buffer = buffer[:0]
						return
					}
					now := time.Now()
					for _, ev := range buffer {
						key := msgKey{sensorID: ev.sensorID, sentNano: ev.sentTime.UnixNano()}
						if err != nil {
							recordFailed(key)
						} else {
							recordPersisted(key, ev.sentTime, now)
						}
					}
					buffer = buffer[:0]
				}

				for {
					select {
					case <-runCtx.Done():
						return
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

	// --- A. SERVER SIDE (Partitioned MQTT Subscribers & Storage Dispatcher) ---
	runTopicPrefix := fmt.Sprintf("%s%s/step%d/run%d/", TopicPrefix, arch.Name, sc.StepID, runID)

	messageHandler := func(client mqtt.Client, msg mqtt.Message) {
		if runCtx.Err() != nil {
			return
		}
		payloadStr := string(msg.Payload())
		topic := msg.Topic()

		wgServer.Add(1)
		go func(pStr, tStr string) {
			defer wgServer.Done()
			if runCtx.Err() != nil {
				return
			}
			// Payload format: "<sent UnixNano>,<reading>"
			sentStr, valueStr, _ := strings.Cut(pStr, ",")
			sentTimeNano, _ := strconv.ParseInt(sentStr, 10, 64)
			value, _ := strconv.ParseFloat(valueStr, 64)
			sentTime := time.Unix(0, sentTimeNano)

			parts := strings.Split(tStr, "/")
			sensorID, _ := strconv.Atoi(parts[len(parts)-1])
			key := msgKey{sensorID: sensorID, sentNano: sentTimeNano}

			if arch.Mode == "Centralized_Batched" {
				select {
				case batchChan <- batchedEvent{
					sensorID: sensorID,
					value:    value,
					sentTime: sentTime,
					recvTime: time.Now(),
				}:
				case <-runCtx.Done():
				}
				return
			}

			var err error
			if arch.Mode == "Centralized_1to1" {
				select {
				case dgraphSem <- struct{}{}:
					err = writeToDgraph(runCtx, sensorID, value)
					<-dgraphSem
				case <-runCtx.Done():
					return
				}
			} else {
				isuIdx := twin.UniformISU(sensorID, arch.NumISUs)
				err = writeToTimescaleISU(runCtx, isuIdx, sensorID, value)
			}

			if runCtx.Err() != nil {
				return
			}

			if err != nil {
				recordFailed(key)
				return
			}
			recordPersisted(key, sentTime, time.Now())
		}(payloadStr, topic)
	}

	srvClients := make([]mqtt.Client, NumServerSubscribers)
	var wgSubs sync.WaitGroup
	for subIdx := 0; subIdx < NumServerSubscribers; subIdx++ {
		wgSubs.Add(1)
		var subOnce sync.Once
		subTopic := fmt.Sprintf("%sp%d/#", runTopicPrefix, subIdx)

		serverOpts := createMQTTOpts(fmt.Sprintf("srv_%s_%d_%d_%d", arch.Name, sc.StepID, runID, subIdx))
		serverOpts.SetOrderMatters(false)
		serverOpts.SetMessageChannelDepth(20000)
		serverOpts.SetDefaultPublishHandler(messageHandler)
		serverOpts.SetOnConnectHandler(func(c mqtt.Client) {
			if subToken := c.Subscribe(subTopic, QoSLevel, nil); subToken.Wait() && subToken.Error() != nil {
				log.Printf("Server Subscribe Error (%s): %v", subTopic, subToken.Error())
			}
			subOnce.Do(wgSubs.Done)
		})

		srvClient := mqtt.NewClient(serverOpts)
		if token := srvClient.Connect(); token.Wait() && token.Error() != nil {
			log.Printf("Server Connect Error (sub %d): %v", subIdx, token.Error())
			return Result{}
		}
		srvClients[subIdx] = srvClient
	}
	wgSubs.Wait()
	time.Sleep(500 * time.Millisecond)

	// --- B. CLIENT SIDE (Simulated IoT Sensor Publishers, external IDs 1..N) ---
	var wgReady sync.WaitGroup
	var wgSensors sync.WaitGroup
	startSignal := make(chan struct{})
	tickerInterval := time.Duration(1000.0/sc.FrequencyHz) * time.Millisecond
	maxTicks := int(math.Round(float64(duration) * sc.FrequencyHz))
	if maxTicks < 1 {
		maxTicks = 1
	}
	connSem := make(chan struct{}, 250) // Pace concurrent TCP/MQTT handshakes

	for id := 1; id <= sc.NumSensors; id++ {
		wgReady.Add(1)
		wgSensors.Add(1)
		go func(id int) {
			defer wgSensors.Done()

			connSem <- struct{}{}
			cOpts := createMQTTOpts(fmt.Sprintf("s_%s_%d_%d_%d", arch.Name, sc.StepID, runID, id))
			c := mqtt.NewClient(cOpts)
			if token := c.Connect(); token.Wait() && token.Error() != nil {
				<-connSem
				log.Printf("Sensor %d fails connection: %v", id, token.Error())
				wgReady.Done()
				return
			}
			<-connSem
			defer c.Disconnect(0)

			sensorTopic := fmt.Sprintf("%sp%d/%d", runTopicPrefix, id%NumServerSubscribers, id)

			wgReady.Done()
			<-startSignal // Wait until all sensors are connected and server subscriptions are active

			runDeadline := time.After(time.Duration(duration) * time.Second)

			// Spread sensor phase offsets uniformly across 95% of tickerInterval to avoid
			// artificial 1-second synchronization stampedes and end-of-window deadline races.
			jitterWindowNano := int64(tickerInterval) * 95 / 100
			if jitterWindowNano > 0 {
				initialDelay := time.Duration(rand.Int63n(jitterWindowNano))
				select {
				case <-runDeadline:
					return
				case <-time.After(initialDelay):
				}
			}

			ticker := time.NewTicker(tickerInterval)
			defer ticker.Stop()

			publishMsg := func() {
				sentNano := time.Now().UnixNano()
				reading := SensorValueMin + rand.Float64()*(SensorValueMax-SensorValueMin)
				payload := fmt.Sprintf("%d,%.3f", sentNano, reading)
				t := c.Publish(sensorTopic, QoSLevel, false, payload)
				t.Wait()

				if t.Error() == nil {
					atomic.AddInt64(&msgSentCount, 1)
					sentLog[id] = append(sentLog[id], sentNano) // owned by this goroutine
				}
			}

			publishMsg()
			for tick := 1; tick < maxTicks; tick++ {
				select {
				case <-runDeadline:
					return
				case <-ticker.C:
					publishMsg()
				}
			}
		}(id)
	}

	// Wait for all sensor MQTT clients to finish TCP/MQTT handshake before starting the timer
	wgReady.Wait()
	startTime := time.Now()
	close(startSignal)

	wgSensors.Wait()
	publishEnd := time.Now()
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
		cancelRun()
		<-serverDone
	}

	cancelRun()
	endTime := time.Now() // Drain cutoff: nothing is persisted after this instant
	for _, srvClient := range srvClients {
		if srvClient != nil {
			srvClient.Disconnect(250)
		}
	}

	var cpuAvg, ramMaxMB float64
	if !isWarmup {
		fmt.Printf("   > Waiting for Prometheus scraping (%ds)...\n", PromScrapeWaitSec)
		time.Sleep(time.Duration(PromScrapeWaitSec) * time.Second)

		promURL := cfg.PrometheusURI
		myNamespace := cfg.KubernetesNamespace

		// The resource window covers exactly this run's activity: from the start of publishing
		// until the last write completed (or the drain cutoff). It excludes the idle pauses and
		// therefore never overlaps the previous run or the reset that precedes this one.
		workEnd := publishEnd
		muMetrics.Lock()
		for _, doneNano := range persisted {
			if done := time.Unix(0, doneNano); done.After(workEnd) {
				workEnd = done
			}
		}
		muMetrics.Unlock()
		windowMs := workEnd.Sub(startTime).Milliseconds()
		if windowMs < MinPromWindowSec*1000 {
			windowMs = MinPromWindowSec * 1000
		}

		podRegex := buildPodRegex(arch)
		cpuQuery := fmt.Sprintf(`sum(rate(container_cpu_usage_seconds_total{namespace="%s", pod=~"%s", container!="POD", container!=""}[%dms]))`, myNamespace, podRegex, windowMs)
		ramQuery := fmt.Sprintf(`sum(max_over_time(container_memory_working_set_bytes{namespace="%s", pod=~"%s", container!="POD", container!=""}[%dms])) / 1024 / 1024`, myNamespace, podRegex, windowMs)

		var errCPU, errRAM error
		cpuAvg, errCPU = queryPrometheus(promURL, cpuQuery, workEnd)
		if errCPU != nil {
			fmt.Printf("   [!] Error Prometheus CPU: %v\n", errCPU)
		}

		ramMaxMB, errRAM = queryPrometheus(promURL, ramQuery, workEnd)
		if errRAM != nil {
			fmt.Printf("   [!] Error Prometheus RAM: %v\n", errRAM)
		}
	}

	// --- C. RECONCILIATION, STATS & PERCENTILES ---
	// All server goroutines and batch workers have exited, so the maps are no longer written.
	// Undelivered messages keep a right-censored latency (lower bound) at the drain cutoff.
	var undelivered int64
	for id := 1; id <= sc.NumSensors; id++ {
		for _, sentNano := range sentLog[id] {
			key := msgKey{sensorID: id, sentNano: sentNano}
			if _, ok := persisted[key]; ok {
				continue
			}
			if _, ok := failed[key]; ok {
				continue
			}
			undelivered++
			latenciesMs = append(latenciesMs, float64(endTime.UnixNano()-sentNano)/1e6)
		}
	}

	avgLat, p50Lat, p95Lat, p99Lat := computeLatencyStats(latenciesMs)

	// Sustained throughput: only writes completed while the sensors were publishing count, so the
	// backlog drained afterwards does not inflate the rate the architecture actually sustained.
	var persistedInWindow int64
	publishEndNano := publishEnd.UnixNano()
	for _, doneNano := range persisted {
		if doneNano <= publishEndNano {
			persistedInWindow++
		}
	}
	throughput := 0.0
	if windowSec := publishEnd.Sub(startTime).Seconds(); windowSec > 0 {
		throughput = float64(persistedInWindow) / windowSec
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
		MessagesSent:      atomic.LoadInt64(&msgSentCount),
		MessagesLost:      undelivered,
		DbErrors:          atomic.LoadInt64(&dbErrorCount),
		ThroughputMsgSec:  throughput,
		TargetCPUAvgCores: cpuAvg,
		TargetRAMMaxMB:    ramMaxMB,
	}
}

// computeLatencyStats computes the arithmetic mean and P50, P95, and P99 percentiles
// from the recorded per-message end-to-end ingestion latencies (including censored ones).
func computeLatencyStats(samples []float64) (avg, p50, p95, p99 float64) {
	n := len(samples)
	if n == 0 {
		return 0, 0, 0, 0
	}
	sum := 0.0
	for _, v := range samples {
		sum += v
	}
	avg = sum / float64(n)
	sort.Float64s(samples)
	p50 = samples[int(math.Min(float64(n-1), float64(n)*0.50))]
	p95 = samples[int(math.Min(float64(n-1), float64(n)*0.95))]
	p99 = samples[int(math.Min(float64(n-1), float64(n)*0.99))]
	return
}

// buildPodRegex constructs the PromQL regular expression matching either the Dgraph Alpha
// pod(s) for Centralized modes or the active `timescaledb-0..N-1` pods for Federated modes.
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

// queryPrometheus executes an instant PromQL query against the Prometheus HTTP API at `evalTime`
// and parses the scalar floating-point metric value.
func queryPrometheus(promURL string, query string, evalTime time.Time) (float64, error) {
	reqURL := fmt.Sprintf("%s/api/v1/query?query=%s&time=%.3f",
		promURL,
		url.QueryEscape(query),
		float64(evalTime.UnixMilli())/1000.0)

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
