To replicate the results presented in the paper, please follow the steps outlined below.

## Infrastructure
#### Prerequisites
Ensure your environment meets the following technical requirements:

- **Kubernetes Cluster:** A running cluster.
- **Go:** Version 1.25
- **Helm:** For managing package installations.
- **kubectl:** Configured to interface with your cluster.

#### Deployment
Follow the instructions below to deploy the infrastructure.

##### DGraph (KG Database)
```bash
# Add the DGraph Helm repository
helm repo add dgraph https://charts.dgraph.io

# Install DGraph
helm install dgraph dgraph/dgraph -f kubernetes/dgraph.yaml
```

##### TimescaleDB (Time-Series Database)
```bash
kubectl apply -f kubernetes/timescaledb.yaml
```

##### EMQX (MQTT Broker)
```bash
# Add the EMQX Helm repository
helm repo add emqx https://repos.emqx.io/charts

# Install EMQX
helm install emqx emqx/emqx --version 5.5.0 -f kubernetes/emqx.yaml
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

# Note: Edit the .env file to ensure the connection strings and 
# credentials align with your deployed infrastructure.
```

#### Execution

The benchmarking scripts are modular and may be executed independently or in any preferred order:
```bash
# Navigate to the evaluation directory
cd evaluation

# Run the ingestion performance evaluation
go run ingestion/main.go

# Run the query performance evaluation
go run query/main.go
```

#### Results
After generating the raw performance data, you can process the results and generate plots by navigating to the plots directory. Ensure your Python virtual environment is activated before execution.

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

# Generate ingestion results
python src/ingestion.py

# Generate query results
python src/query.py
```

