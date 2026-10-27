package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// ======================================================================================
// SUB-EXPERIMENTS & CSV TELEMETRY REPORTING
// ======================================================================================

// Sub-experiment output CSV filenames stored within each timestamped execution directory.
const (
	Exp1CSVFileName = "exp1_scalability.csv"
	Exp2CSVFileName = "exp2_complexity_concurrency.csv"
	Exp3CSVFileName = "exp3_skew.csv"
)

// runSubExperiment1 evaluates horizontal scalability across varying numbers of
// Independent Storage Units (N = 1, 2, 4, 8 ISUs) and Knowledge Graph sizes
// (500 to 5,000 sensors) under the full-hierarchy Q3 inference workload. Every federated
// configuration is measured with intra-ISU sub-batching (full system) and without it
// (ablation), on the same loaded data, to separate horizontal scaling from intra-ISU parallelism.
func runSubExperiment1(outputDir string, maxISUs int) {
	csvPath := filepath.Join(outputDir, Exp1CSVFileName)
	file, writer := createExperimentCSV(csvPath)
	defer func() {
		writer.Flush()
		file.Close()
	}()

	fmt.Println("\n======================================================================")
	fmt.Println("SUB-EXPERIMENT 1: HORIZONTAL SCALABILITY (N = 1, 2, 4, 8 ISUs)")
	fmt.Println("======================================================================")

	sensorCounts := []int{500, 1000, 2000, 3000, 4000, 5000}
	isuConfigs := []int{0, 1, 2, 4, 8} // 0 = Centralized Dgraph Baseline

	for _, count := range sensorCounts {
		for _, numISUs := range isuConfigs {
			if numISUs > maxISUs {
				continue
			}
			archName := "Centralized"
			subBatches := []int{NoSubBatch}
			if numISUs > 0 {
				archName = fmt.Sprintf("Federated_%dISU", numISUs)
				subBatches = []int{ISUSubBatch, NoSubBatch}
			}
			runScenario(writer, "Exp1_Scalability", count, archName, numISUs, "Uniform", "Q3_Inference", 1, subBatches)
		}
	}

	fmt.Printf("\n[Sub-Experiment 1 Complete] Results saved to: %s\n", csvPath)
}

// runSubExperiment2 evaluates query complexity across selective subgraph traversal (Q1),
// multi-hop relational/numeric filtering (Q2), and forward-chaining fault inference (Q3)
// under increasing concurrent query streams (C = 1, 5, 10, 20).
func runSubExperiment2(outputDir string, maxISUs int) {
	csvPath := filepath.Join(outputDir, Exp2CSVFileName)
	file, writer := createExperimentCSV(csvPath)
	defer func() {
		writer.Flush()
		file.Close()
	}()

	fmt.Println("\n======================================================================")
	fmt.Println("SUB-EXPERIMENT 2: QUERY COMPLEXITY (Q1, Q2, Q3) & CONCURRENCY")
	fmt.Println("======================================================================")

	queries := []string{"Q1_Selective", "Q2_Filtering", "Q3_Inference"}
	concurrencies := []int{1, 5, 10, 20}
	targetISUs := 8
	if maxISUs < 8 {
		targetISUs = maxISUs
	}

	for _, qType := range queries {
		for _, conc := range concurrencies {
			// Centralized vs Federated (8 ISUs) at 5000 sensors
			runScenario(writer, "Exp2_Complexity_Concurrency", 5000, "Centralized", 0, "Uniform", qType, conc, []int{NoSubBatch})
			runScenario(writer, "Exp2_Complexity_Concurrency", 5000, fmt.Sprintf("Federated_%dISU", targetISUs), targetISUs, "Uniform", qType, conc, []int{ISUSubBatch})
		}
	}

	fmt.Printf("\n[Sub-Experiment 2 Complete] Results saved to: %s\n", csvPath)
}

// runSubExperiment3 evaluates sensitivity to data partition skew across ISUs by comparing
// Uniform, ModerateSkew (50% on ISU 0), and HotspotSkew (80% on ISU 0) distributions,
// each with and without intra-ISU sub-batching (the mechanism that splits the hotspot ISU's load).
func runSubExperiment3(outputDir string, maxISUs int) {
	csvPath := filepath.Join(outputDir, Exp3CSVFileName)
	file, writer := createExperimentCSV(csvPath)
	defer func() {
		writer.Flush()
		file.Close()
	}()

	fmt.Println("\n======================================================================")
	fmt.Println("SUB-EXPERIMENT 3: DATA PARTITION SKEW (UNIFORM vs SKEWED PARTITIONS)")
	fmt.Println("======================================================================")

	skewModes := []string{"Uniform", "ModerateSkew", "HotspotSkew"}
	skewISUs := 4
	if maxISUs >= 8 {
		skewISUs = 8
	} else if maxISUs < 4 {
		skewISUs = maxISUs
	}

	for _, skew := range skewModes {
		runScenario(writer, "Exp3_Skew", 5000, fmt.Sprintf("Federated_%dISU", skewISUs), skewISUs, skew, "Q3_Inference", 1, []int{ISUSubBatch, NoSubBatch})
	}

	fmt.Printf("\n[Sub-Experiment 3 Complete] Results saved to: %s\n", csvPath)
}

// runScenario provisions the multi-hop topology and historical time-series observations once,
// then, for each sub-batching variant, executes warm-up iterations, performs the measured
// benchmark runs, and flushes metrics to CSV. Variants therefore share the same loaded data.
func runScenario(writer *csv.Writer, expType string, count int, arch string, numISUs int, partMode string, queryType string, concurrency int, subBatches []int) {
	fmt.Printf("\n--- [%s] Arch: %s | Sensors: %d | Part: %s | Query: %s | Conc: %d ---\n",
		expType, arch, count, partMode, queryType, concurrency)

	fmt.Print("   > Setting up multi-hop Thing topology (hasChild) & state... ")
	expectedValues := setupEnvironment(count, numISUs, partMode)
	performUpdates(arch, numISUs, expectedValues)
	// Wait until Dgraph (compactions, rollups, previous DropAll cleanup) and the ISUs are idle
	waitForStorageIdle()
	fmt.Println("Ready.")

	for _, subBatch := range subBatches {
		if arch != "Centralized" {
			fmt.Printf("   > Sub-batch: %d (0 = one call per ISU)\n", subBatch)
		}
		runVariant(writer, expType, count, arch, numISUs, partMode, queryType, concurrency, subBatch, expectedValues)
	}
}

// runVariant executes the warm-up and measured runs of a single sub-batching variant.
func runVariant(writer *csv.Writer, expType string, count int, arch string, numISUs int, partMode string, queryType string, concurrency int, subBatch int, expectedValues map[int]ExpectedSensor) {
	// Warm-up phase
	for w := 0; w < WarmUpRuns; w++ {
		executeWorkload(arch, numISUs, queryType, concurrency, subBatch, expectedValues)
		time.Sleep(SimulatedDelay)
	}

	// Measurement phase
	for r := 1; r <= RunsPerTest; r++ {
		time.Sleep(SimulatedDelay)

		m := executeWorkload(arch, numISUs, queryType, concurrency, subBatch, expectedValues)
		m.ExperimentType = expType
		m.NumSensors = count
		m.Architecture = arch
		m.NumISUs = numISUs
		m.PartitionMode = partMode
		m.QueryType = queryType
		m.Concurrency = concurrency
		m.SubBatch = subBatch
		m.RunID = r

		if r == 1 || r == RunsPerTest || r%10 == 0 {
			fmt.Printf("     Run %2d/%d: Total=%.2fms (S1_Anchor=%.2fms | S2_IO=%.2fms | S3_Hydr=%.2fms | S4_Query=%.2fms) | Integrity=%.1f%% | Mem=%.2fMB\n",
				r, RunsPerTest, m.TotalLatency, m.Stage1AnchorMs, m.Stage2ISUIOMs, m.Stage3HydrMs, m.Stage4QueryMs, m.SuccessRate, m.MemAllocMB)
		}

		logResult(writer, m)
	}
}

// resolveOutputRunDir creates and returns a timestamped output directory located inside
// `query/output/<YYYY-MM-DD_HH-MM-SS>` regardless of whether the binary is invoked from
// `evaluation/` or `evaluation/query/`.
func resolveOutputRunDir() string {
	timestamp := time.Now().Format("2006-01-02_15-04-05")

	baseOutput := "output"
	if info, err := os.Stat("query"); err == nil && info.IsDir() {
		baseOutput = filepath.Join("query", "output")
	}

	runDir := filepath.Join(baseOutput, timestamp)
	if err := os.MkdirAll(runDir, 0755); err != nil {
		log.Fatalf("Failed to create timestamped output directory %s: %v", runDir, err)
	}
	return runDir
}

// createExperimentCSV initializes a new CSV file for an individual sub-experiment and writes
// the standard 16-column benchmark telemetry header.
func createExperimentCSV(csvPath string) (*os.File, *csv.Writer) {
	file, err := os.Create(csvPath)
	if err != nil {
		log.Fatalf("Failed to create CSV file %s: %v", csvPath, err)
	}
	writer := csv.NewWriter(file)
	writer.Write([]string{
		"Experiment_Type",
		"Num_Sensors",
		"Architecture",
		"Num_ISUs",
		"Partition_Mode",
		"Query_Type",
		"Concurrency",
		"Sub_Batch",
		"Run_ID",
		"Total_Latency_ms",
		"Stage1_Anchor_ms",
		"Stage2_ISU_IO_ms",
		"Stage3_Hydration_ms",
		"Stage4_QueryExec_ms",
		"Success_Rate_Pct",
		"Memory_Pressure_MB_per_Query",
	})
	writer.Flush()
	return file, writer
}

// logResult serializes a single QueryMetrics observation into the active sub-experiment CSV.
func logResult(w *csv.Writer, m QueryMetrics) {
	w.Write([]string{
		m.ExperimentType,
		strconv.Itoa(m.NumSensors),
		m.Architecture,
		strconv.Itoa(m.NumISUs),
		m.PartitionMode,
		m.QueryType,
		strconv.Itoa(m.Concurrency),
		strconv.Itoa(m.SubBatch),
		strconv.Itoa(m.RunID),
		fmt.Sprintf("%.4f", m.TotalLatency),
		fmt.Sprintf("%.4f", m.Stage1AnchorMs),
		fmt.Sprintf("%.4f", m.Stage2ISUIOMs),
		fmt.Sprintf("%.4f", m.Stage3HydrMs),
		fmt.Sprintf("%.4f", m.Stage4QueryMs),
		fmt.Sprintf("%.2f", m.SuccessRate),
		fmt.Sprintf("%.4f", m.MemAllocMB),
	})
	w.Flush()
}
