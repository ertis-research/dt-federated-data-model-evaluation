package main

import (
	"dt-real-bench/config"
	"flag"
	"fmt"
	"log"
	"strings"
)

// ======================================================================================
// BENCHMARK ENTRY POINT & CLI EXPERIMENT DISPATCHER
// ======================================================================================

func main() {
	expFlag := flag.String(
		"exp",
		"all",
		"Sub-experiment(s) to execute: 'all', '1' (Scalability), '2' (Complexity & Concurrency), '3' (Partition Skew), or comma-separated (e.g., '1,3')",
	)
	flag.Parse()

	runExp1, runExp2, runExp3, err := parseExperimentSelection(*expFlag)
	if err != nil {
		log.Fatalf("Invalid -exp argument: %v", err)
	}

	log.SetFlags(0)
	cfg = config.LoadEnv()

	initInfrastructure()
	defer closeInfrastructure()

	outputDir := resolveOutputRunDir()
	maxISUs := len(pgDBs)

	fmt.Printf("Starting END-TO-END SEMANTIC QUERY EVALUATION (WarmUp: %d, Runs: %d, Available ISUs: %d)...\n",
		WarmUpRuns, RunsPerTest, maxISUs)
	fmt.Printf("Output Directory: %s\n", outputDir)

	if runExp1 {
		runSubExperiment1(outputDir, maxISUs)
	}
	if runExp2 {
		runSubExperiment2(outputDir, maxISUs)
	}
	if runExp3 {
		runSubExperiment3(outputDir, maxISUs)
	}

	fmt.Printf("\nEvaluation Complete. All requested CSV results saved in: %s\n", outputDir)
}

// parseExperimentSelection parses the CLI `-exp` flag to determine which sub-experiments
// should be executed during the current benchmark run.
func parseExperimentSelection(selection string) (exp1, exp2, exp3 bool, err error) {
	normalized := strings.ToLower(strings.TrimSpace(selection))
	if normalized == "" || normalized == "all" {
		return true, true, true, nil
	}

	parts := strings.Split(normalized, ",")
	for _, part := range parts {
		token := strings.TrimSpace(part)
		switch token {
		case "1", "exp1", "scalability":
			exp1 = true
		case "2", "exp2", "complexity", "concurrency":
			exp2 = true
		case "3", "exp3", "skew":
			exp3 = true
		case "all":
			return true, true, true, nil
		default:
			return false, false, false, fmt.Errorf("unknown sub-experiment identifier %q (valid: all, 1, 2, 3)", token)
		}
	}

	if !exp1 && !exp2 && !exp3 {
		return false, false, false, fmt.Errorf("no valid sub-experiment selected")
	}
	return exp1, exp2, exp3, nil
}
