package main

// ======================================================================================
// EPHEMERAL IN-MEMORY RDF TRIPLE STORE & EMBEDDED INFERENCE / BGP ENGINE (STAGE 3 & 4)
// ======================================================================================

// RDFTriple represents a canonical semantic triple (Subject, Predicate, Object)
// along with an optional parsed numeric literal for fast range filter evaluation.
type RDFTriple struct {
	Subject   string
	Predicate string
	Object    string
	NumValue  float64 // Optional parsed numeric literal for fast range filter evaluation
}

// EphemeralTripleGraph implements an in-memory RDF graph with SPO, POS, and OSP indexes
// to support Stage 3 (Semantic Hydration) and Stage 4 (SPARQL-like BGP & Rule Inference).
type EphemeralTripleGraph struct {
	Triples    []RDFTriple
	SPO        map[string]map[string][]string // Subject -> Predicate -> []Object
	POS        map[string]map[string][]string // Predicate -> Object -> []Subject
	OSP        map[string]map[string][]string // Object -> Subject -> []Predicate (inverse traversal)
	NumericLit map[string]map[string]float64  // Subject -> Predicate -> float64 literal
	SensorURIs map[int]string                 // ExternalID -> Sensor URI (e.g., "dt:Sensor_1")
	Matched    int                            // Count of query bindings matched in Stage 4
}

// NewEphemeralTripleGraph allocates and initializes an EphemeralTripleGraph pre-sized
// according to the estimated number of sensors in the active subgraph.
func NewEphemeralTripleGraph(estimatedSensors int) *EphemeralTripleGraph {
	capHint := estimatedSensors * 6
	return &EphemeralTripleGraph{
		Triples:    make([]RDFTriple, 0, capHint),
		SPO:        make(map[string]map[string][]string, estimatedSensors*2),
		POS:        make(map[string]map[string][]string, 16),
		OSP:        make(map[string]map[string][]string, estimatedSensors*2),
		NumericLit: make(map[string]map[string]float64, estimatedSensors),
		SensorURIs: make(map[int]string, estimatedSensors),
	}
}

// InsertTriple synthesizes and indexes a semantic triple (s, p, o) into the ephemeral
// graph's SPO, POS, OSP, and numeric literal lookup tables.
func (g *EphemeralTripleGraph) InsertTriple(s, p, o string, numVal float64, isNumeric bool) {
	g.Triples = append(g.Triples, RDFTriple{Subject: s, Predicate: p, Object: o, NumValue: numVal})

	if _, ok := g.SPO[s]; !ok {
		g.SPO[s] = make(map[string][]string, 4)
	}
	g.SPO[s][p] = append(g.SPO[s][p], o)

	if _, ok := g.POS[p]; !ok {
		g.POS[p] = make(map[string][]string, 8)
	}
	g.POS[p][o] = append(g.POS[p][o], s)

	if _, ok := g.OSP[o]; !ok {
		g.OSP[o] = make(map[string][]string, 2)
	}
	g.OSP[o][s] = append(g.OSP[o][s], p)

	if isNumeric {
		if _, ok := g.NumericLit[s]; !ok {
			g.NumericLit[s] = make(map[string]float64, 2)
		}
		g.NumericLit[s][p] = numVal
	}
}

// extractTopologicalTriples synthesizes canonical RDF triples from the Knowledge Graph
// hierarchy and merges either hydrated dynamic state values (Federated Stage 3) or
// the materialized centralized LKV (Centralized baseline).
func extractTopologicalTriples(roots []Thing, estSensors int, hydratedState map[int]float64) []RDFTriple {
	triples := make([]RDFTriple, 0, estSensors*5)

	var walk func(n *Thing)
	walk = func(n *Thing) {
		uri := "dt:" + n.Name
		triples = append(triples, RDFTriple{Subject: uri, Predicate: "rdf:type", Object: "dt:" + n.AssetCategory})
		if n.SubType != "" {
			triples = append(triples, RDFTriple{Subject: uri, Predicate: "dt:hasSubType", Object: "dt:" + n.SubType})
		}
		if n.AssetCategory == "Sensor" && n.ExternalID > 0 {
			triples = append(triples, RDFTriple{Subject: uri, Predicate: "dt:maxThreshold", Object: "threshold", NumValue: n.MaxThreshold})
			if hydratedState != nil {
				if v, ok := hydratedState[n.ExternalID]; ok {
					triples = append(triples, RDFTriple{Subject: uri, Predicate: "dt:hasDynamicValue", Object: "hydrated", NumValue: v})
				}
			} else if n.LastValue != nil {
				v := *n.LastValue
				triples = append(triples, RDFTriple{Subject: uri, Predicate: "dt:hasDynamicValue", Object: "centralized", NumValue: v})
			}
		}
		for i := range n.HasChild {
			childURI := "dt:" + n.HasChild[i].Name
			triples = append(triples, RDFTriple{Subject: uri, Predicate: "dt:hasChild", Object: childURI})
			walk(&n.HasChild[i])
		}
	}

	for i := range roots {
		walk(&roots[i])
	}
	return triples
}

// runStage4SemanticEngine loads the synthesized RDF triples into the embedded inference
// engine's SPO/POS/OSP indexes and executes SPARQL-equivalent Basic Graph Pattern (BGP)
// joins, numeric range filters, and Forward-Chaining semantic inference rules.
func runStage4SemanticEngine(queryType string, triples []RDFTriple, estSensors int) int {
	g := NewEphemeralTripleGraph(estSensors)
	for i := range triples {
		t := &triples[i]
		isNum := t.Predicate == "dt:hasDynamicValue" || t.Predicate == "dt:maxThreshold"
		g.InsertTriple(t.Subject, t.Predicate, t.Object, t.NumValue, isNum)
	}

	switch queryType {
	case "Q1_Selective":
		// BGP Query:
		// SELECT ?sensor ?val WHERE {
		//   dt:Zone_0 dt:hasChild ?turb .
		//   ?turb     dt:hasChild ?comp .
		//   ?comp     dt:hasChild ?sensor .
		//   ?sensor   dt:hasDynamicValue ?val .
		// }
		turbs := g.SPO["dt:Zone_0"]["dt:hasChild"]
		for _, turbURI := range turbs {
			comps := g.SPO[turbURI]["dt:hasChild"]
			for _, compURI := range comps {
				sensors := g.SPO[compURI]["dt:hasChild"]
				for _, sensorURI := range sensors {
					if _, hasVal := g.NumericLit[sensorURI]["dt:hasDynamicValue"]; hasVal {
						g.Matched++
					}
				}
			}
		}

	case "Q2_Filtering":
		// BGP Query with Relational Join + Numeric Filter:
		// SELECT ?comp ?sensor ?val WHERE {
		//   ?comp   rdf:type dt:Component ;
		//           dt:hasSubType dt:Gearbox ;
		//           dt:hasChild ?sensor .
		//   ?sensor dt:hasDynamicValue ?val .
		//   FILTER (?val > 75.0)
		// }
		gearboxComps := g.POS["dt:hasSubType"]["dt:Gearbox"]
		for _, compURI := range gearboxComps {
			sensors := g.SPO[compURI]["dt:hasChild"]
			for _, sensorURI := range sensors {
				if val, ok := g.NumericLit[sensorURI]["dt:hasDynamicValue"]; ok && val > 75.0 {
					g.InsertTriple(sensorURI, "rdf:type", "dt:ThresholdExceededSensor", 0, false)
					g.Matched++
				}
			}
		}

	case "Q3_Inference":
		// Forward-Chaining Semantic Rule Reasoning & Fault Propagation:
		// Rule R1 (Anomaly Detection):
		//   (?s rdf:type dt:Sensor) ^ (?s dt:hasDynamicValue ?v) ^ (?s dt:maxThreshold ?th) ^ (?v > ?th)
		//   => (?s rdf:type dt:AnomalousSensor)
		// Rule R2 (Upward Component & Turbine Fault Propagation via inverse dt:hasChild):
		//   (?s rdf:type dt:AnomalousSensor) ^ (?comp dt:hasChild ?s) ^ (?turb dt:hasChild ?comp)
		//   => (?comp rdf:type dt:FaultyComponent) ^ (?turb rdf:type dt:MaintenanceRequiredTurbine)
		sensors := g.POS["rdf:type"]["dt:Sensor"]
		for _, sensorURI := range sensors {
			val, hasVal := g.NumericLit[sensorURI]["dt:hasDynamicValue"]
			th, hasTh := g.NumericLit[sensorURI]["dt:maxThreshold"]
			if hasVal && hasTh && val > th {
				g.InsertTriple(sensorURI, "rdf:type", "dt:AnomalousSensor", 0, false)
				g.Matched++

				for compURI := range g.OSP[sensorURI] {
					g.InsertTriple(compURI, "rdf:type", "dt:FaultyComponent", 0, false)
					for turbURI := range g.OSP[compURI] {
						g.InsertTriple(turbURI, "rdf:type", "dt:MaintenanceRequiredTurbine", 0, false)
					}
				}
			}
		}
	}
	return g.Matched
}
