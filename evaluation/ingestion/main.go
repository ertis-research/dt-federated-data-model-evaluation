package main

import (
	"dt-real-bench/config"
	"log"
)

// ======================================================================================
// INGESTION BENCHMARK ENTRY POINT
// ======================================================================================

func main() {
	log.SetFlags(0)
	cfg = config.LoadEnv()

	initInfrastructure()
	defer closeInfrastructure()

	outputDir := resolveOutputRunDir()
	runIngestionEvaluation(outputDir)
}
