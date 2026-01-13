import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# 1. Load Data
df = pd.read_csv("../test-go/ingestion_test/final_evaluation_results.csv")

# 2. Setup Plot Style for Academic Paper
sns.set_theme(style="whitegrid")
plt.rcParams.update({'font.family': 'serif', 'font.size': 12})

# Create a figure with 2 subplots (Latency and Packet Loss)
fig, axes = plt.subplots(1, 2, figsize=(14, 6))

# --- PLOT A: Latency vs Scenario ---
# We group by Scenario/Arch to verify variability, but Seaborn handles this automatically with error bars
sns.barplot(
    data=df, 
    x="Scenario_ID", 
    y="Avg_Latency_ms", 
    hue="Architecture",
    ax=axes[0],
    palette="viridis",
    capsize=.1,
    errorbar="sd" # Standard Deviation
)

axes[0].set_title("End-to-End Latency Comparison")
axes[0].set_ylabel("Average Latency (ms)")
axes[0].set_xlabel("Test Scenario (Increasing Complexity)")
axes[0].legend(title="Architecture")

# --- PLOT B: Throughput/Scalability ---
sns.lineplot(
    data=df, 
    x="Scenario_ID", 
    y="Throughput_msg_sec", 
    hue="Architecture",
    ax=axes[1],
    palette="viridis",
    marker="o",
    linewidth=2.5
)

axes[1].set_title("System Throughput (Scalability)")
axes[1].set_ylabel("Processed Messages / Second")
axes[1].set_xlabel("Test Scenario")
axes[1].grid(True, which="both", ls="--", alpha=0.5)

# Add scenario descriptions as a footnote
scenarios_desc = (
    "Scenarios:\n"
    "1: Baseline (50 sensors, 1Hz)\n"
    "2: High Freq (50 sensors, 10Hz)\n"
    "3: High Conc (200 sensors, 1Hz)\n"
    "4: Complex Graph A (High Node Count)\n"
    "5: Complex Graph B (Very High Node Count)"
)
plt.figtext(0.5, -0.05, scenarios_desc, ha="center", fontsize=10, bbox={"facecolor":"orange", "alpha":0.1, "pad":5})

plt.tight_layout()
plt.savefig("paper_evaluation_results.png", dpi=300, bbox_inches='tight')
print("Plot generated: paper_evaluation_results.png")