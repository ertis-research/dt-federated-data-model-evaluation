package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/lib/pq"
)

// ======================================================================================
// 4-STAGE END-TO-END FEDERATED & CENTRALIZED ORCHESTRATION ENGINE
// ======================================================================================

// sinceMs computes the elapsed duration in milliseconds since the provided timestamp.
func sinceMs(t time.Time) float64 {
	return float64(time.Since(t).Nanoseconds()) / 1e6
}

// executeWorkload dispatches single-stream (C = 1) or concurrent (C > 1) semantic query
// evaluations and records the per-stage latency breakdown, integrity score, and heap allocation.
func executeWorkload(arch string, numISUs int, queryType string, concurrency int, expected map[int]ExpectedSensor) QueryMetrics {
	var memBefore, memAfter runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	startWall := time.Now()

	if concurrency <= 1 {
		s1, s2, s3, s4, integrity := executeSingleQuery(arch, numISUs, queryType, expected)
		totalMs := sinceMs(startWall)
		runtime.ReadMemStats(&memAfter)
		memMB := float64(memAfter.TotalAlloc-memBefore.TotalAlloc) / 1024.0 / 1024.0

		return QueryMetrics{
			TotalLatency:   totalMs,
			Stage1AnchorMs: s1,
			Stage2ISUIOMs:  s2,
			Stage3HydrMs:   s3,
			Stage4QueryMs:  s4,
			SuccessRate:    integrity,
			MemAllocMB:     memMB,
		}
	}

	// Concurrent query execution (C > 1)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var sumTotal, sumS1, sumS2, sumS3, sumS4, sumIntegrity float64

	for c := 0; c < concurrency; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			qStart := time.Now()
			s1, s2, s3, s4, integrity := executeSingleQuery(arch, numISUs, queryType, expected)
			qTotal := sinceMs(qStart)

			mu.Lock()
			sumTotal += qTotal
			sumS1 += s1
			sumS2 += s2
			sumS3 += s3
			sumS4 += s4
			sumIntegrity += integrity
			mu.Unlock()
		}()
	}
	wg.Wait()

	runtime.ReadMemStats(&memAfter)
	memMB := (float64(memAfter.TotalAlloc-memBefore.TotalAlloc) / 1024.0 / 1024.0) / float64(concurrency)
	cFloat := float64(concurrency)

	return QueryMetrics{
		TotalLatency:   sumTotal / cFloat,
		Stage1AnchorMs: sumS1 / cFloat,
		Stage2ISUIOMs:  sumS2 / cFloat,
		Stage3HydrMs:   sumS3 / cFloat,
		Stage4QueryMs:  sumS4 / cFloat,
		SuccessRate:    sumIntegrity / cFloat,
		MemAllocMB:     memMB,
	}
}

// executeSingleQuery routes the query execution to either the Centralized Knowledge Graph
// baseline or the 4-Stage Federated Orchestration Engine.
func executeSingleQuery(arch string, numISUs int, queryType string, expected map[int]ExpectedSensor) (s1, s2, s3, s4, integrity float64) {
	if arch == "Centralized" {
		return executeCentralizedQuery(queryType, expected)
	}
	return executeFederatedQuery(numISUs, queryType, expected)
}

// executeCentralizedQuery traverses the multi-hop `hasChild` relationships in the monolithic KG,
// resolves the Last Known Value (LKV) via `hasObservation(orderdesc: timestamp, first: 1)`,
// and executes the same Stage 4 semantic evaluation over the resulting triples.
func executeCentralizedQuery(queryType string, expected map[int]ExpectedSensor) (s1, s2, s3, s4, integrity float64) {
	ctx := context.Background()
	t0 := time.Now()

	var q string
	switch queryType {
	case "Q1_Selective":
		q = `{
			roots(func: eq(name, "Zone_0")) {
				uid
				name
				asset_category
				hasChild {
					uid
					name
					asset_category
					hasChild {
						uid
						name
						asset_category
						sub_type
						hasChild {
							uid
							name
							asset_category
							sub_type
							external_id
							max_threshold
							hasObservation(orderdesc: timestamp, first: 1) {
								value
							}
						}
					}
				}
			}
		}`
	case "Q2_Filtering":
		q = `{
			roots(func: eq(sub_type, "Gearbox")) {
				uid
				name
				asset_category
				sub_type
				hasChild {
					uid
					name
					asset_category
					sub_type
					external_id
					max_threshold
					hasObservation(orderdesc: timestamp, first: 1) {
						value
					}
				}
			}
		}`
	case "Q3_Inference":
		q = `{
			roots(func: eq(asset_category, "WindFarm")) {
				uid
				name
				asset_category
				hasChild {
					uid
					name
					asset_category
					hasChild {
						uid
						name
						asset_category
						hasChild {
							uid
							name
							asset_category
							sub_type
							hasChild {
								uid
								name
								asset_category
								sub_type
								external_id
								max_threshold
								hasObservation(orderdesc: timestamp, first: 1) {
									value
								}
							}
						}
					}
				}
			}
		}`
	}

	resp, err := dgClient.NewTxn().Query(ctx, q)
	if err != nil {
		log.Printf("Centralized Dgraph Query Error: %v", err)
		return 0, 0, 0, 0, 0
	}

	var decode struct {
		Roots []Thing `json:"roots"`
	}
	json.Unmarshal(resp.Json, &decode)
	s1 = sinceMs(t0)

	// Stage 4: Index triples & run embedded BGP / inference engine over the retrieved hierarchy
	t4 := time.Now()
	triples := extractTopologicalTriples(decode.Roots, len(expected), nil)
	matched := runStage4SemanticEngine(queryType, triples, len(expected))
	s4 = sinceMs(t4)

	integrity = verifyIntegrity(queryType, matched, expected)
	return s1, 0, 0, s4, integrity
}

// executeFederatedQuery implements the 4-Stage Orchestration Sequence described in Section 4:
//   - Stage 1: Data Anchor Resolution (KG topological traversal)
//   - Stage 2: Parallel State Retrieval from ISUs via Canonical JSON Semantic Wrappers
//   - Stage 3: Semantic Hydration (JSON structural flattening & RDF triple synthesis into Ephemeral Graph)
//   - Stage 4: Query & Inference Execution (Triple indexing, SPARQL-like BGP matching & Forward-Chaining rules)
func executeFederatedQuery(numISUs int, queryType string, expected map[int]ExpectedSensor) (s1, s2, s3, s4, integrity float64) {
	ctx := context.Background()

	// ----------------------------------------------------------------------------------
	// STAGE 1: Topological Traversal & Data Anchor Resolution (Semantic Plane)
	// ----------------------------------------------------------------------------------
	t0 := time.Now()
	var q string
	switch queryType {
	case "Q1_Selective":
		q = `{
			roots(func: eq(name, "Zone_0")) {
				uid
				name
				asset_category
				hasChild {
					uid
					name
					asset_category
					hasChild {
						uid
						name
						asset_category
						sub_type
						hasChild {
							uid
							name
							asset_category
							sub_type
							external_id
							isu_id
							max_threshold
						}
					}
				}
			}
		}`
	case "Q2_Filtering":
		q = `{
			roots(func: eq(sub_type, "Gearbox")) {
				uid
				name
				asset_category
				sub_type
				hasChild {
					uid
					name
					asset_category
					sub_type
					external_id
					isu_id
					max_threshold
				}
			}
		}`
	default:
		q = `{
			roots(func: eq(asset_category, "WindFarm")) {
				uid
				name
				asset_category
				hasChild {
					uid
					name
					asset_category
					hasChild {
						uid
						name
						asset_category
						hasChild {
							uid
							name
							asset_category
							sub_type
							hasChild {
								uid
								name
								asset_category
								sub_type
								external_id
								isu_id
								max_threshold
							}
						}
					}
				}
			}
		}`
	}

	resp, err := dgClient.NewTxn().Query(ctx, q)
	if err != nil {
		log.Printf("Stage 1 Anchor Resolution Error: %v", err)
		return 0, 0, 0, 0, 0
	}

	var decode struct {
		Roots []Thing `json:"roots"`
	}
	json.Unmarshal(resp.Json, &decode)

	// Extract Data Anchors grouped by target ISU
	isuBatches := extractDataAnchors(decode.Roots, numISUs)
	s1 = sinceMs(t0)

	// ----------------------------------------------------------------------------------
	// STAGE 2: Parallel Batch State Retrieval across Independent Storage Units (Data Plane)
	// Semantic Wrappers return consolidated canonical JSON payloads per ISU batch.
	// ----------------------------------------------------------------------------------
	t1 := time.Now()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var wrapperPayloads [][]byte

	for isuID, anchorIDs := range isuBatches {
		if len(anchorIDs) == 0 {
			continue
		}
		for start := 0; start < len(anchorIDs); start += ISUSubBatch {
			end := start + ISUSubBatch
			if end > len(anchorIDs) {
				end = len(anchorIDs)
			}
			chunk := anchorIDs[start:end]
			wg.Add(1)
			go func(targetISU int, ids []int32) {
				defer wg.Done()
				jsonBatch, err := fetchISUWrapperJSON(pgDBs[targetISU], ids)
				if err != nil {
					log.Printf("Stage 2 ISU %d Retrieval Error: %v", targetISU, err)
					return
				}
				mu.Lock()
				wrapperPayloads = append(wrapperPayloads, jsonBatch)
				mu.Unlock()
			}(isuID, chunk)
		}
	}
	wg.Wait()
	s2 = sinceMs(t1)

	// ----------------------------------------------------------------------------------
	// STAGE 3: Semantic Hydration (JSON Flattening & Ephemeral RDF Triple Synthesis)
	// Flattens the received canonical JSON objects, synthesizes dynamic state predicates,
	// and merges them with the topological triples into the unified ephemeral triple set.
	// ----------------------------------------------------------------------------------
	t2 := time.Now()
	hydratedState := make(map[int]float64, len(expected))
	for _, rawJSON := range wrapperPayloads {
		var records []wrapperRecord
		if err := json.Unmarshal(rawJSON, &records); err != nil {
			continue
		}
		for _, rec := range records {
			hydratedState[rec.ID] = rec.Reading
		}
	}
	triples := extractTopologicalTriples(decode.Roots, len(expected), hydratedState)
	s3 = sinceMs(t2)

	// ----------------------------------------------------------------------------------
	// STAGE 4: Semantic Query & Inference Execution over Enriched Ephemeral Triple Graph
	// Indexes the unified RDF triples into SPO/POS/OSP tables and executes SPARQL-like
	// BGP pattern matching, numeric filtering, and Forward-Chaining inference rules.
	// ----------------------------------------------------------------------------------
	t3 := time.Now()
	matched := runStage4SemanticEngine(queryType, triples, len(expected))
	s4 = sinceMs(t3)

	integrity = verifyIntegrity(queryType, matched, expected)
	return s1, s2, s3, s4, integrity
}

// extractDataAnchors walks the topological response from Dgraph to group `(isu_id, external_id)`
// Data Anchors by their assigned Independent Storage Unit.
func extractDataAnchors(roots []Thing, numISUs int) map[int][]int32 {
	batches := make(map[int][]int32, numISUs)
	var walk func(n *Thing)
	walk = func(n *Thing) {
		if n.AssetCategory == "Sensor" && n.ExternalID > 0 {
			batches[n.IsuID] = append(batches[n.IsuID], int32(n.ExternalID))
		}
		for i := range n.HasChild {
			walk(&n.HasChild[i])
		}
	}
	for i := range roots {
		walk(&roots[i])
	}
	return batches
}

// fetchISUWrapperJSON executes the Last Known Value (LKV) query on the specified ISU TSDB
// and serializes the result into the canonical JSON exchange schema of the Semantic Wrapper.
func fetchISUWrapperJSON(db *sql.DB, ids []int32) ([]byte, error) {
	query := `
        SELECT DISTINCT ON (sensor_id) sensor_id, payload
        FROM query_test
        WHERE sensor_id = ANY($1)
        ORDER BY sensor_id, time DESC
    `
	rows, err := db.Query(query, pq.Array(ids))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	localResults := make([]wrapperRecord, 0, len(ids))
	for rows.Next() {
		var rec wrapperRecord
		rec.Unit = "degC_or_mm_s"
		if err := rows.Scan(&rec.ID, &rec.Reading); err == nil {
			localResults = append(localResults, rec)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return json.Marshal(localResults)
}

// verifyIntegrity compares the number of query bindings matched in Stage 4 against the
// deterministic ground-truth expectations to compute a percentage accuracy score (0-100%).
func verifyIntegrity(queryType string, matchedCount int, expected map[int]ExpectedSensor) float64 {
	if len(expected) == 0 {
		return 100.0
	}

	expectedMatches := 0
	for _, exp := range expected {
		switch queryType {
		case "Q1_Selective":
			if exp.ZoneName == "Zone_0" {
				expectedMatches++
			}
		case "Q2_Filtering":
			if exp.ComponentType == "Gearbox" && exp.Value > 75.0 {
				expectedMatches++
			}
		case "Q3_Inference":
			if exp.Value > exp.MaxThreshold {
				expectedMatches++
			}
		}
	}

	if expectedMatches == 0 {
		if matchedCount == 0 {
			return 100.0
		}
		return 0.0
	}

	diff := matchedCount - expectedMatches
	if diff < 0 {
		diff = -diff
	}
	acc := (1.0 - float64(diff)/float64(expectedMatches)) * 100.0
	if acc < 0 {
		acc = 0
	}
	return acc
}

