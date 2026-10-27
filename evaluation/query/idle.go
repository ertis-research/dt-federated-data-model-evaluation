package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// ======================================================================================
// STORAGE IDLE DETECTION (Prometheus / cAdvisor)
// ======================================================================================

// waitForStorageIdle blocks until every storage pod (Dgraph Alpha and all TimescaleDB ISUs)
// stays below IdleCPUThresholdCores for IdleConsecutiveChecks consecutive readings. It runs after
// each scenario's data load so that background work (Badger compactions and rollups, the cleanup
// of the previous scenario's DropAll, Postgres autovacuum) does not leak into the measured runs.
func waitForStorageIdle() {
	// irate() uses the last two samples inside the 1-minute window: it reflects the most recent
	// cAdvisor interval (~10-15 s) yet always finds two samples, unlike rate() over a short window.
	query := fmt.Sprintf(
		`max(sum by (pod) (irate(container_cpu_usage_seconds_total{namespace="%s", pod=~"dgraph-dgraph-alpha-.*|timescaledb-.*", container!="POD", container!=""}[1m])))`,
		cfg.KubernetesNamespace,
	)

	start := time.Now()
	idleReadings := 0
	failedReadings := 0
	for idleReadings < IdleConsecutiveChecks {
		if time.Since(start) > IdleMaxWait {
			fmt.Printf("[WARNING] storage not idle after %s, proceeding... ", IdleMaxWait)
			return
		}
		time.Sleep(IdlePollInterval)

		busiest, err := queryPrometheus(cfg.PrometheusURI, query)
		if err != nil {
			// Transient gaps (e.g. right after a scrape) are retried; persistent failures give up
			failedReadings++
			idleReadings = 0
			if failedReadings >= IdleMaxFailedChecks {
				fmt.Printf("[WARNING] Prometheus idle check failed %d times (%v), proceeding... ", failedReadings, err)
				return
			}
			continue
		}
		failedReadings = 0
		if busiest < IdleCPUThresholdCores {
			idleReadings++
		} else {
			idleReadings = 0
		}
	}
	fmt.Printf("idle after %.0fs... ", time.Since(start).Seconds())
}

// queryPrometheus executes an instant PromQL query against the Prometheus HTTP API
// and parses the scalar floating-point metric value.
func queryPrometheus(promURL string, query string) (float64, error) {
	reqURL := fmt.Sprintf("%s/api/v1/query?query=%s", promURL, url.QueryEscape(query))

	resp, err := http.Get(reqURL)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("error HTTP Prometheus: %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	var promData struct {
		Data struct {
			Result []struct {
				Value []interface{} `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &promData); err != nil {
		return 0, err
	}

	if len(promData.Data.Result) > 0 && len(promData.Data.Result[0].Value) == 2 {
		return strconv.ParseFloat(fmt.Sprintf("%v", promData.Data.Result[0].Value[1]), 64)
	}
	return 0, fmt.Errorf("no data returned from Prometheus")
}
