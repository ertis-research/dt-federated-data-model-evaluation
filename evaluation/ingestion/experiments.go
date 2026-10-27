package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"dt-real-bench/twin"
)

// ======================================================================================
// INGESTION EVALUATION SUITE & CSV TELEMETRY REPORTING
// ======================================================================================

// runIngestionEvaluation executes the initial system warm-up followed by the full matrix
// of workload scaling scenarios (Step 1..6) and architectures (Centralized 1-to-1,
// Centralized Batched, and Federated 1..8 ISUs), persisting results to a timestamped CSV.
func runIngestionEvaluation(outputDir string) {
	csvPath := filepath.Join(outputDir, IngestionCSVFileName)
	file, writer := createExperimentCSV(csvPath)
	defer func() {
		writer.Flush()
		file.Close()
	}()

	scenarios := []Scenario{
		{StepID: 1, NumSensors: 100, FrequencyHz: 0.1},
		{StepID: 2, NumSensors: 500, FrequencyHz: 0.1},
		{StepID: 3, NumSensors: 1000, FrequencyHz: 0.1},
		{StepID: 4, NumSensors: 2000, FrequencyHz: 0.2},
		{StepID: 5, NumSensors: 5000, FrequencyHz: 0.2},
		{StepID: 6, NumSensors: 5000, FrequencyHz: 1.0},
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
	fmt.Printf("Output CSV: %s\n", csvPath)

	samplePayload := fmt.Sprintf("%d,%.3f", time.Now().UnixNano(), SensorValueMax)
	fmt.Printf("Application Payload Size: %d bytes (Sample: %s)\n", len(samplePayload), samplePayload)

	for _, sc := range scenarios {
		graphNodes := twin.TotalNodes(sc.NumSensors)
		fmt.Printf("\n>>> STEP %d: %d Sensors @ %.2f Hz (%d Nodes) <<<\n",
			sc.StepID, sc.NumSensors, sc.FrequencyHz, graphNodes)

		for _, arch := range architectures {
			if arch.Mode == "Federated" && arch.NumISUs > len(pgDBs) {
				fmt.Printf("   [SKIP] %s requires %d ISUs, but only %d configured.\n", arch.Name, arch.NumISUs, len(pgDBs))
				continue
			}

			fmt.Printf("--- Arch: %s (ISUs: %d) ---\n", arch.Name, arch.NumISUs)

			// Unrecorded warm-up through this architecture's own write path at the step's load
			fmt.Printf("   > Warm-up (%d seconds)...\n", WarmupDurationSec)
			prepareEnvironment(sc.NumSensors, arch.NumISUs)
			executeTestRun(sc, arch, 0, true)
			time.Sleep(3 * time.Second)

			for run := 1; run <= TotalRuns; run++ {
				// Reset Dgraph and every ISU so each run starts from the same empty history
				prepareEnvironment(sc.NumSensors, arch.NumISUs)
				res := executeTestRun(sc, arch, run, false)

				logResult(writer, graphNodes, res)

				fmt.Printf("   > Run %d: Sent=%d | Lost=%d | DB_Err=%d | Avg=%.2fms | P95=%.2fms | P99=%.2fms | Tput=%.1f msg/s | CPU=%.3fc | RAM=%.2fMB\n",
					run, res.MessagesSent, res.MessagesLost, res.DbErrors,
					res.AvgLatencyMs, res.P95LatencyMs, res.P99LatencyMs,
					res.ThroughputMsgSec, res.TargetCPUAvgCores, res.TargetRAMMaxMB)

				time.Sleep(2 * time.Second)
			}
		}
	}

	fmt.Printf("\nIngestion Evaluation Complete. Results saved to: %s\n", csvPath)
}

// resolveOutputRunDir creates and returns a timestamped output directory located inside
// `ingestion/output/<YYYY-MM-DD_HH-MM-SS>` regardless of whether the binary is invoked from
// `evaluation/` or `evaluation/ingestion/`.
func resolveOutputRunDir() string {
	timestamp := time.Now().Format("2006-01-02_15-04-05")

	baseOutput := "output"
	if info, err := os.Stat("ingestion"); err == nil && info.IsDir() {
		baseOutput = filepath.Join("ingestion", "output")
	}

	runDir := filepath.Join(baseOutput, timestamp)
	if err := os.MkdirAll(runDir, 0755); err != nil {
		log.Fatalf("Failed to create timestamped output directory %s: %v", runDir, err)
	}
	return runDir
}

// createExperimentCSV initializes a new CSV file for the ingestion evaluation and writes
// the standard 16-column telemetry header.
func createExperimentCSV(csvPath string) (*os.File, *csv.Writer) {
	file, err := os.Create(csvPath)
	if err != nil {
		log.Fatalf("Cannot create output CSV %s: %v", csvPath, err)
	}
	writer := csv.NewWriter(file)
	writer.Write([]string{
		"Step_ID", "Num_Sensors", "Freq_Hz", "Architecture", "Num_ISUs", "Run_ID", "Graph_Complexity",
		"Avg_Latency_ms", "P50_Latency_ms", "P95_Latency_ms", "P99_Latency_ms",
		"Undelivered_Msgs", "DB_Errors", "Throughput_msg_sec",
		"DB_CPU_Avg_Cores", "DB_RAM_Max_MB",
	})
	writer.Flush()
	return file, writer
}

// logResult serializes a single ingestion Result observation into the active CSV file.
func logResult(writer *csv.Writer, graphComplexity int, res Result) {
	writer.Write([]string{
		strconv.Itoa(res.StepID),
		strconv.Itoa(res.NumSensors),
		fmt.Sprintf("%.2f", res.FrequencyHz),
		res.Architecture,
		strconv.Itoa(res.NumISUs),
		strconv.Itoa(res.RunID),
		strconv.Itoa(graphComplexity),
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
}
