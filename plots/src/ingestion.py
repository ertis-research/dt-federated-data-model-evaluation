import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# ============================================================
# Load evaluation data
# ============================================================
df = pd.read_csv("../evaluation/output/ingestion_evaluation_results.csv")

# ============================================================
# Compute mean metrics across runs for each scenario
# ============================================================
df_mean = (
    df.groupby(
        ["Step_ID", "Num_Sensors", "Freq_Hz", "Architecture"],
        as_index=False
    )
    .mean(numeric_only=True)
)

# ============================================================
# Create human-readable scenario labels: <devices>@<frequency> Hz
# ============================================================
df_mean["Scenario_Label"] = (
    df_mean["Num_Sensors"].astype(str)
    + "@"
    + df_mean["Freq_Hz"].astype(str)
    + " Hz"
)

# Ensure consistent ordering of the six scenarios
scenario_order = (
    df_mean[["Step_ID", "Scenario_Label"]]
    .drop_duplicates()
    .sort_values("Step_ID")
)

labels = scenario_order["Scenario_Label"].tolist()
x = np.arange(len(labels))
width = 0.35

# Split architectures
fed = df_mean[df_mean["Architecture"] == "Federated"]
cen = df_mean[df_mean["Architecture"] == "Centralized"]

# ============================================================
# IEEE-compliant plotting style
# ============================================================
plt.rcParams.update({
    "font.family": "serif",
    "font.size": 10,
    "axes.grid": True,
    "grid.linestyle": "--",
    "grid.alpha": 0.6,
})

# ============================================================
# FIGURE 1: Average Latency (Bar Chart)
# ============================================================
plt.figure(figsize=(3.6, 3.2))

plt.bar(
    x - width / 2,
    fed["Avg_Latency_ms"],
    width,
    label="Federated",
)
plt.bar(
    x + width / 2,
    cen["Avg_Latency_ms"],
    width,
    label="Centralized",
)

plt.xlabel("Scenario (Devices@Frequency)")
plt.ylabel("Average Latency (ms)")
plt.title("Average Latency per Scenario")
plt.xticks(x, labels, rotation=30, ha="right")
plt.legend()

plt.tight_layout()
plt.savefig(
    "output/ingestion_average_latency.pdf",
    bbox_inches="tight"
)

# ============================================================
# FIGURE 2: Throughput (Bar Chart)
# ============================================================
plt.figure(figsize=(3.6, 3.2))

plt.bar(
    x - width / 2,
    fed["Throughput_msg_sec"],
    width,
    label="Federated",
)
plt.bar(
    x + width / 2,
    cen["Throughput_msg_sec"],
    width,
    label="Centralized",
)

plt.xlabel("Scenario (Devices@Frequency)")
plt.ylabel("Throughput (messages/s)")
plt.title("Throughput per Scenario")
plt.xticks(x, labels, rotation=30, ha="right")
plt.legend()

plt.tight_layout()
plt.savefig(
    "output/ingestion_throughput.pdf",
    bbox_inches="tight"
)

# ============================================================
# FIGURE 3: Packet Loss (Bar Chart)
# ============================================================
plt.figure(figsize=(3.6, 3.2))

plt.bar(
    x - width / 2,
    fed["Packet_Loss_Network"],
    width,
    label="Federated",
)
plt.bar(
    x + width / 2,
    cen["Packet_Loss_Network"],
    width,
    label="Centralized",
)

plt.xlabel("Scenario (Devices@Frequency)")
plt.ylabel("Packet Loss (messages)")
plt.title("Packet Loss per Scenario")
plt.xticks(x, labels, rotation=30, ha="right")
plt.legend()

plt.tight_layout()
plt.savefig(
    "output/ingestion_packet_loss.pdf",
    bbox_inches="tight"
)
