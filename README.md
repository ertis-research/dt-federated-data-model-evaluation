To replicate the results presented in the paper, please follow the steps outlined below.

## Infrastructure
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
go run ingestion/main.go

# Run ALL end-to-end query sub-experiments sequentially (default: -exp all)
go run ./query

# Alternatively, run individual query sub-experiments (or comma-separated combinations):
go run ./query -exp 1     # Sub-Experiment 1: Horizontal Scalability (exp1_scalability.csv)
go run ./query -exp 2     # Sub-Experiment 2: Query Complexity & Concurrency (exp2_complexity_concurrency.csv)
go run ./query -exp 3     # Sub-Experiment 3: Data Partition Skew (exp3_skew.csv)
go run ./query -exp 1,3   # Run only Sub-Experiments 1 and 3
```
Each query evaluation run automatically stores its CSV output files inside a timestamped folder: `evaluation/query/output/<YYYY-MM-DD_HH-MM-SS>/`.

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

# Generate ingestion plots
python src/ingestion.py

# Generate ALL query evaluation plots (automatically selects the latest execution)
python src/query.py

# Or generate plots for each query sub-experiment individually (defaults to latest execution):
python src/query_exp1_scalability.py
python src/query_exp2_complexity.py
python src/query_exp3_skew.py

# To generate plots from a specific timestamped execution folder (positional or via --dir):
python src/query_exp1_scalability.py 2026-10-19_16-30-00
python src/query_exp2_complexity.py --dir 2026-10-19_16-30-00
python src/query_exp3_skew.py --dir ../evaluation/query/output/2026-10-19_16-30-00
python src/query.py --dir 2026-10-19_16-30-00
```