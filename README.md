To replicate the results presented in the paper, please follow the steps outlined below.

## Infrastructure
#### Hardware Testbed
The evaluation uses two physically separate clusters, so the load generator never competes for resources with the systems under test:

- **Benchmark clients (load generator & query orchestrator):** the `evaluation/ingestion` and `evaluation/query` benchmarks run on a Kubernetes cluster consisting of seven high-performance servers, where each node is equipped with two Intel® Xeon® Gold 6230R processors, two NVIDIA® Tesla® V100 GPUs, and 384 GB of RAM.
- **Architecture components (systems under test):** Dgraph, the TimescaleDB ISUs, and the EMQX broker are deployed on a Kubernetes cluster consisting of two servers (`edge1`, `edge2`). Each server features an Intel® Xeon® D-2183IT processor, an NVIDIA® Tesla® T4 GPU, and 128 GB of RAM. Dgraph (Zero and Alpha) is pinned to `edge1` and EMQX to `edge2`, while the eight TimescaleDB ISUs are spread across `edge1` and `edge2` (preferred pod anti-affinity). Per-container CPU and memory limits are listed in `kubernetes/`.

#### Prerequisites
Ensure your environment meets the following technical requirements:

- **Kubernetes Cluster:** A running cluster.
- **Go:** Version 1.25
- **Helm:** For managing package installations.
- **kubectl:** Configured to interface with your cluster.

#### Deployment
Follow the instructions below to deploy the infrastructure using the exact chart versions used in the evaluation.

##### DGraph (Semantic Plane - KG Database)
- **Helm Chart:** `dgraph-24.1.4` (App Version: `v24.1.4`)
```bash
# Add the DGraph Helm repository
helm repo add dgraph https://charts.dgraph.io
helm repo update

# Install DGraph
helm install dgraph dgraph/dgraph --version 24.1.4 -f kubernetes/dgraph.yaml
```

##### TimescaleDB (Data Plane - 8 Independent Storage Units)
- **Image:** `timescale/timescaledb:latest-pg17`
```bash
# Deploys 8 TimescaleDB StatefulSet replicas (timescaledb-0..7)
# exposed on consecutive NodePorts (45521..45528)
kubectl apply -f kubernetes/timescaledb.yaml
```

##### EMQX (MQTT Broker)
- **Helm Chart:** `emqx-5.5.0` (App Version: `5.5.0`)
```bash
# Add the EMQX Helm repository
helm repo add emqx https://repos.emqx.io/charts
helm repo update

# Install EMQX
helm install emqx emqx/emqx --version 5.5.0 -f kubernetes/emqx.yaml
```

##### Prometheus (Resource Consumption Monitoring)
- **Helm Chart:** `prometheus-29.37.0` (App Version: `v3.15.0`)
```bash
# Add the Prometheus Community Helm repository
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

# Install Prometheus (exposed on NodePort 30090 with 5s scrape interval)
helm install prometheus prometheus-community/prometheus --version 29.37.0 -f kubernetes/prometheus.yaml
```

#### Verification
Verify the Helm releases and ensure all pods are in the `Running` state before starting the evaluation:

```bash
helm list
```
Expected Helm releases:
```text
NAME            NAMESPACE                       REVISION        STATUS          CHART                APP VERSION
dgraph          federated-data-model-test       1               deployed        dgraph-24.1.4        v24.1.4    
emqx            federated-data-model-test       1               deployed        emqx-5.5.0           5.5.0      
prometheus      federated-data-model-test       1               deployed        prometheus-29.37.0   v3.15.0    
```

```bash
kubectl get pods
```
Expected Pod status:
```text
NAME                                             READY   STATUS    RESTARTS   AGE
dgraph-dgraph-ratel-5b78bff566-pc7bl             1/1     Running   0          23m
emqx-0                                           1/1     Running   0          24m
dgraph-dgraph-zero-0                             1/1     Running   0          23m
dgraph-dgraph-alpha-0                            1/1     Running   0          23m
timescaledb-0                                    1/1     Running   0          16m
timescaledb-1                                    1/1     Running   0          16m
timescaledb-2                                    1/1     Running   0          16m
timescaledb-3                                    1/1     Running   0          16m
timescaledb-5                                    1/1     Running   0          16m
timescaledb-6                                    1/1     Running   0          16m
timescaledb-7                                    1/1     Running   0          16m
timescaledb-4                                    1/1     Running   0          16m
prometheus-prometheus-node-exporter-gtmjl        1/1     Running   0          39s
prometheus-kube-state-metrics-6b9d655759-4lsfz   1/1     Running   0          39s
prometheus-prometheus-node-exporter-cczlq        1/1     Running   0          39s
prometheus-server-856c7fb697-xft4c               2/2     Running   0          39s
```

## Experimental Design & Evaluation Scenarios

The evaluation suite is designed to validate the proposed **Federated Digital Twin Data Model**, which decouples structural topology (**Semantic Plane**, hosted in Dgraph as purely topological from high-frequency time-series telemetry (**Data Plane**, distributed across $N$ Independent Storage Units — ISUs — backed by TimescaleDB). Articulated nodes store lightweight **Data Anchors** (`isu_id`, `external_id`) instead of raw observation values, enabling polyglot persistence and on-demand semantic hydration.

The testbed is divided into two main experimental suites: **Experiment 1 (Telemetry Ingestion)** and **Experiment 2 (End-to-End Federated Query Orchestration)**, which is further structured into three targeted sub-experiments.

---

### Experiment 1: High-Frequency Telemetry Ingestion & Ablation Study (`evaluation/ingestion`)

#### What It Evaluates
This experiment measures end-to-end ingestion performance, tail latency stability, reliability (undelivered messages and database errors), and database resource footprint (CPU cores and working-set RAM scraped via Prometheus) as telemetry arrival rates and Knowledge Graph (KG) topological sizes scale up.

#### Evaluated Architectures & Rationale
To ensure a rigorous ablation study that isolates the architectural benefit of decoupling the Semantic and Data Planes from low-level client or storage engine optimizations, six architectures are evaluated:

1. **`Centralized_1to1` (Monolithic Baseline — Per-Event Transactions):** Every incoming MQTT telemetry payload triggers an immediate, synchronous RDF/JSON mutation in Dgraph (`CommitNow: true`) that appends a new `Observation` node to the sensor (`hasObservation`) and updates its materialized Last Known Value (`last_value`, `last_timestamp`).
   - *Why:* Represents standard unbatched event-driven ingestion into a monolithic Knowledge Graph, exposing Raft consensus, index maintenance, and predicate locking bottlenecks under high write concurrency.
2. **`Centralized_Batched` (Monolithic Ablation — Micro-Batched Transactions):** Incoming MQTT events are buffered in memory and committed to Dgraph in micro-batches (`BatchMaxSize = 100` events or `BatchMaxFlush = 100 ms` across 4 concurrent worker routines).
   - *Why:* Provides a fair, production-optimized monolithic KG baseline. By amortizing network round-trips and transaction commits in Dgraph, this scenario demonstrates that the performance degradation of a centralized KG under heavy telemetry is a fundamental consequence of indexing time-series in a graph structure rather than merely a lack of client-side batching.
3. **`Federated_1ISU` (Single-Node Federated Baseline):** The static KG topology resides in Dgraph (untouched during ingestion), while all telemetry streams are written (one row per reading) to a single TimescaleDB instance (`timescaledb-0`).
   - *Why:* Isolates the impact of **polyglot persistence** on a 1-to-1 basis before introducing horizontal sharding. Notably, a single TimescaleDB ISU is provisioned with half the CPU and a quarter of the RAM (`2 vCPUs, 4 GiB RAM`) of the Dgraph Alpha node (`4 vCPUs, 16 GiB RAM`), proving that specialized time-series storage outperforms a graph engine even under strict sub-resource allocation.
4. **`Federated_2ISU`, `Federated_4ISU`, `Federated_8ISU` (Horizontal Scale-Out):** Telemetry streams are partitioned deterministically across $N \in \{2, 4, 8\}$ independent TimescaleDB instances (hash partitioning, `splitmix64(external_id) mod N`, the same Data Anchor assignment stored in the KG), each running its own dedicated ingestion workers.
   - *Why:* Evaluates the horizontal write scalability of the Federated Data Plane, demonstrating how distributing Data Anchors across multiple ISUs linearly absorbs bursty IoT workloads and reduces per-node tail latency ($P_{95}$, $P_{99}$).

#### Shared Data Model
Both experiments use one Digital Twin model, defined once in `evaluation/twin`: the same Dgraph schema, the same Wind Farm topology (`1 WindFarm -> 5 Zones -> 50 Turbines -> 200 Components -> N Sensors`), and the same Centralized write path. In the Centralized KG every reading is appended as an `Observation` node **and** materialized as the sensor's `last_value`, so the KG keeps full history while serving LKV lookups directly. In the Federated deployment the KG holds only the topology and Data Anchors, and every reading is a hypertable row in its ISU, indexed by `(sensor_id, time DESC)` as in the query benchmark.

Within each step, every architecture first runs an unrecorded warm-up (`WarmupDurationSec`) through its own write path at the step's load. Dgraph and all ISUs are then reset (empty history, topology reloaded) before each measured run, so every run starts from the same state.

#### Metrics
- **Latency (Avg, P50, P95, P99):** publish-to-persist time over **all** published messages. Messages not persisted before the drain cutoff (`DBDrainTimeoutSec`) are kept with a right-censored latency (`cutoff - sent time`), so the percentiles of a saturated architecture are lower bounds instead of statistics over the surviving messages only.
- **`Undelivered_Msgs`:** published messages that were neither persisted nor rejected with a database error before the cutoff.
- **`Throughput_msg_sec` (sustained):** messages persisted *while the sensors were publishing*, divided by the length of that window. Backlog drained after publishing stops is not counted, so an architecture that falls behind the offered load reports a rate below it.
- **`DB_CPU_Avg_Cores` / `DB_RAM_Max_MB`:** average CPU (`rate` of `container_cpu_usage_seconds_total`) and peak working-set memory of the storage pods (Dgraph Alpha for `Centralized_*`, the active `timescaledb-0..N-1` pods for `Federated_*`), evaluated over exactly the run's activity window: from the start of publishing until the last write completes (or the drain cutoff), with millisecond precision. The window excludes idle pauses, so it never overlaps the previous run or the reset that precedes each run. Prometheus is queried after a `PromScrapeWaitSec = 15 s` wait so that the last cAdvisor samples of the run are ingested (5 s scrape interval).

#### Load Scenarios (Steps 1–6)
Each architecture is evaluated across 6 progressive load steps (5 independent 30-second runs per step):

| Step | Active Sensors | Emission Frequency | Target Throughput | Static KG Complexity | Scenario Rationale |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **Step 1** | 100 | 0.10 Hz | 10 msg/s | 356 Nodes | Low-load baseline to measure minimum end-to-end pipeline overhead. |
| **Step 2** | 500 | 0.10 Hz | 50 msg/s | 756 Nodes | Moderate scale representative of a small industrial cell. |
| **Step 3** | 1,000 | 0.10 Hz | 100 msg/s | 1,256 Nodes | Medium scale testing initial index contention in monolithic graphs. |
| **Step 4** | 2,000 | 0.20 Hz | 400 msg/s | 2,256 Nodes | High sensor density with doubled emission frequency. |
| **Step 5** | 5,000 | 0.20 Hz | 1,000 msg/s | 5,256 Nodes | Large-scale industrial twin under sustained telemetry load. |
| **Step 6** | 5,000 | 1.00 Hz | 5,000 msg/s | 5,256 Nodes | Extreme stress test designed to saturate transactional limits and evaluate backpressure and undelivered messages. |

---

### Experiment 2: End-to-End Federated Query Orchestration (`evaluation/query`)

#### What It Evaluates
While decoupling telemetry accelerates ingestion, it introduces the challenge of reconstructing unified semantic knowledge at query time. This benchmark evaluates the complete **4-Stage Federated Query Orchestration Mechanism** against a monolithic `Centralized` KG over a hierarchical **Wind Farm Digital Twin** (`1 WindFarm -> 5 Zones -> 50 Turbines -> 200 Components -> N Sensors`).

To guarantee a fair, symmetric comparison:
- Every sensor in both architectures is pre-populated with **`HistorySamples = 100` historical observations** (stored as linked `hasObservation` nodes plus the materialized `last_value` in Dgraph for `Centralized` — the same model used during ingestion — and as hypertable rows in TimescaleDB for `Federated`).
- Both architectures resolve the same **Last Known Value (LKV)** of every target sensor: `Centralized` reads the materialized `last_value` predicate during the traversal, and `Federated` runs a top-1 index probe per sensor (`unnest(ids) CROSS JOIN LATERAL (... ORDER BY time DESC LIMIT 1)`) on the `(sensor_id, time DESC)` index of each ISU. In both cases the cost per sensor is independent of the history length.
- After each scenario's data load, the benchmark waits until every storage pod (Dgraph Alpha and all ISUs) stays below `IdleCPUThresholdCores = 0.10` cores for three consecutive Prometheus readings taken every 5 s (`irate` over a 1-minute window, i.e. the latest cAdvisor interval; 5-minute timeout). This keeps background work (Badger compactions and rollups, the cleanup of the previous scenario's `DropAll`, autovacuum) out of the measured runs.
- Plots report the **median** and **P95** of the 30 runs per configuration. The distributions are skewed, and with 30 samples a P99 is essentially the maximum.
- Every federated execution is decomposed into four explicitly measured stages:
  1. **`Stage1_Anchor_ms` (Anchor Resolution):** Traversing the topological hierarchy (`Thing` nodes via `hasChild` edges) in Dgraph and extracting the `(isu_id, external_id)` Data Anchors of the target articulated leaf nodes.
  2. **`Stage2_ISU_IO_ms` (Parallel ISU State Retrieval):** Concurrent LKV queries dispatched to the target TimescaleDB ISUs (by default each ISU's anchors are split into parallel sub-batches of `ISUSubBatch = 250` IDs; the `Sub_Batch` CSV column records the setting, with `0` meaning one call per ISU), returning dynamic telemetry wrapped in canonical **JSON Semantic Wrappers**.
  3. **`Stage3_Hydration_ms` (Semantic Hydration):** Parsing the JSON Semantic Wrappers and synthesizing canonical RDF triples (`RDFTriple`) that merge topological relationships from Stage 1 with dynamic state from Stage 2.
  4. **`Stage4_QueryExec_ms` (In-Memory Triple Indexing & Reasoning):** Indexing the hydrated triples into an ephemeral in-memory RDF store (`SPO`, `POS`, and `OSP` hash indexes) and executing SPARQL-equivalent Basic Graph Pattern (BGP) joins, value filtering, and forward-chaining inference rules.

---

#### Sub-Experiment 1: Horizontal Scalability & Stage Breakdown (`-exp 1`)
- **Output:** `exp1_scalability.csv`
- **Evaluated Scenarios:** Varies the number of sensors $S \in \{500, 1000, 2000, 3000, 4000, 5000\}$ across `Centralized`, `Federated_1ISU`, `Federated_2ISU`, `Federated_4ISU`, and `Federated_8ISU` under the most demanding full-hierarchy inference workload (`Q3_Inference`, concurrency $C = 1$, `Uniform` partitioning). Every federated configuration is measured twice on the same loaded data: with intra-ISU sub-batching (`Sub_Batch = 250`, the full system) and with one call per ISU (`Sub_Batch = 0`, ablation).
- **Why:**
  1. Quantifies how the `Centralized` architecture suffers from historical node explosion ($5000 \times 100 = 500{,}000$ observation nodes in the KG), which degrades graph traversal latency and inflates Dgraph RAM consumption.
  2. Provides a granular breakdown of Stages 1–4 across $1, 2, 4,$ and $8$ ISUs, quantifying the orchestration overhead of Stage 2 (ISU I/O), Stage 3 (JSON-to-RDF hydration), and Stage 4 (ephemeral triple indexing + reasoning).
  3. Separates two sources of Stage 2 speed-up that would otherwise be confounded: horizontal data-plane scaling (more ISUs, fewer anchors per ISU) and intra-ISU parallelism (sub-batching). Without sub-batching, the number of parallel ISU calls equals the number of ISUs, so the `Sub_Batch = 0` series isolates the effect of horizontal scaling (`query_exp1_subbatch_ablation.pdf`).

---

#### Sub-Experiment 2: Semantic Query Complexity & Concurrency (`-exp 2`)
- **Output:** `exp2_complexity_concurrency.csv`
- **Evaluated Scenarios:** Fixes the topology at maximum scale ($5000$ sensors, $500{,}000$ historical observations) and compares `Centralized` vs. `Federated_8ISU` across **3 Semantic Query Profiles ($Q_1, Q_2, Q_3$)** and **4 Concurrent Client Levels ($C \in \{1, 5, 10, 20\}$)**:
  - **`Q1_Selective` (Selective Sub-Graph Retrieval):** Targets a single wind farm zone (`Zone_0`, encompassing 20% of the sensors).
    - *Why:* Evaluates **topological anchor pruning**. Demonstrates that the federated orchestrator only queries the Data Anchors belonging to the selected sub-graph, avoiding full-dataset scans.
  - **`Q2_Filtering` (Multi-Hop Structural + Dynamic Value Filtering):** Traverses the hierarchy to locate sensors attached specifically to `Gearbox` components (`dt:componentType == "Gearbox"`) and filters those whose live LKV telemetry exceeds a critical threshold (`sosa:hasSimpleResult > 75.0`).
    - *Why:* Tests hybrid queries that combine **Semantic Plane structural constraints** with **Data Plane quantitative filtering** over the hydrated RDF triple store.
  - **`Q3_Inference` (Full-Hierarchy Anomaly Detection & Forward-Chaining Fault Propagation):** Reconstructs the entire 5,000-sensor graph and executes a 2-step **forward-chaining rule engine** over the ephemeral triple store:
    - *Rule 1:* `(Sensor value > max_threshold) ^ (Component hasChild Sensor) -> (Component rdf:type dt:FaultyComponent)`
    - *Rule 2:* `(Component rdf:type dt:FaultyComponent) ^ (Turbine hasChild Component) -> (Turbine rdf:type dt:MaintenanceRequiredTurbine)`
    - *Why:* Validates that the Federated Data Model supports **ontological reasoning and semantic upward fault propagation** across dynamic telemetry values, while testing throughput and tail-latency resilience under high multi-user concurrency ($C = 20$ simultaneous orchestrations).

---

#### Sub-Experiment 3: Data Partition Skew Sensitivity (`-exp 3`)
- **Output:** `exp3_skew.csv`
- **Evaluated Scenarios:** Evaluates `Federated_8ISU` at $5000$ sensors (`Q3_Inference`, $C = 1$) under three data distribution patterns across the 8 ISUs, each measured with intra-ISU sub-batching (`Sub_Batch = 250`) and with one call per ISU (`Sub_Batch = 0`) on the same loaded data:
  1. **`Uniform`:** Sensors are hash-partitioned across all 8 ISUs (`splitmix64(external_id) mod 8`, ~625 sensors each, within ±4%). Hashing rather than round-robin keeps every query's target subset balanced: with round-robin, all `Gearbox` sensors (external ID ≡ 1 mod 4) would land on only 2 of the 8 ISUs.
  2. **`ModerateSkew`:** $50\%$ of all sensors (2,500 sensors) are concentrated on a single bottleneck instance (`ISU 0`), while the remaining $50\%$ are distributed across `ISUs 1..7`.
  3. **`HotspotSkew`:** $80\%$ of all sensors (4,000 sensors) are concentrated on `ISU 0`, leaving only $20\%$ across `ISUs 1..7`.
- **Why:** In real-world federated deployments, Data Anchors are rarely distributed uniformly across storage units (e.g., a legacy historian or a specific plant zone may hold the vast majority of sensors). Because Stage 2 executes in parallel across ISUs, its completion time is bounded by the slowest responder (*straggler effect*). This experiment measures how much the straggler inflates Stage 2 and end-to-end latency under severe partition skew, and how much of that penalty the orchestrator's sub-batching recovers by splitting the hotspot ISU's anchors into parallel calls.

---

## Evaluation

Follow the instructions below to replicate the experimental results discussed in the manuscript.

#### Configuration
Prior to execution, you must configure the environment variables to match your specific cluster deployment. Copy the provided template and update the values as necessary:

```bash
# Navigate to the evaluation directory
cd evaluation

# Initialize environment variables from the template
cp .env.example .env

# Note: Edit the .env file to ensure the connection strings, NodePorts,
# and credentials align with your deployed infrastructure.
```

#### Execution

The benchmarking scripts are modular and may be executed independently or in any preferred order:
```bash
# Navigate to the evaluation directory
cd evaluation

# Run the ingestion performance evaluation (Centralized 1-to-1, Batched, and Federated 1..8 ISUs)
go run ./ingestion

# Run ALL end-to-end query sub-experiments sequentially (default: -exp all)
go run ./query

# Alternatively, run individual query sub-experiments (or comma-separated combinations):
go run ./query -exp 1     # Sub-Experiment 1: Horizontal Scalability (exp1_scalability.csv)
go run ./query -exp 2     # Sub-Experiment 2: Query Complexity & Concurrency (exp2_complexity_concurrency.csv)
go run ./query -exp 3     # Sub-Experiment 3: Data Partition Skew (exp3_skew.csv)
go run ./query -exp 1,3   # Run only Sub-Experiments 1 and 3
```
Each evaluation run automatically stores its CSV output files inside a timestamped folder:
- **Ingestion:** `evaluation/ingestion/output/<YYYY-MM-DD_HH-MM-SS>/ingestion_evaluation_results.csv`
- **Query:** `evaluation/query/output/<YYYY-MM-DD_HH-MM-SS>/` (`exp1_scalability.csv`, `exp2_complexity_concurrency.csv`, `exp3_skew.csv`)

#### Results
After generating the raw performance data, you can process the results and generate plots by navigating to the `plots` directory. Ensure your Python virtual environment is activated before execution.

```bash
# Navigate to the plots directory
cd plots

# Create a virtual environment
python -m venv venv

# Activate the virtual environment
# On Linux/macOS:
source venv/bin/activate
# On Windows:
# .\venv\Scripts\activate

# Install required dependencies
pip install -r requirements.txt

# Generate ingestion plots (automatically selects the latest execution)
python src/ingestion.py

# Or generate ingestion plots from a specific timestamped execution folder (positional or via --dir):
python src/ingestion.py 2026-10-26_14-00-00
python src/ingestion.py --dir 2026-10-26_14-00-00
python src/ingestion.py --dir ../evaluation/ingestion/output/2026-10-26_14-00-00

# Generate ALL query evaluation plots and LaTeX tables (automatically selects the latest execution)
python src/query.py

# Or generate plots for each query sub-experiment individually (defaults to latest execution):
python src/query_exp1_scalability.py
python src/query_exp2_complexity.py
python src/query_exp3_skew.py

# LaTeX (booktabs) tables for the three query sub-experiments -> output/query_tables.tex
python src/query_tables.py

# To generate query plots from a specific timestamped execution folder (positional or via --dir):
python src/query_exp1_scalability.py 2026-10-19_16-30-00
python src/query_exp2_complexity.py --dir 2026-10-19_16-30-00
python src/query_exp3_skew.py --dir ../evaluation/query/output/2026-10-19_16-30-00
python src/query.py --dir 2026-10-19_16-30-00
```