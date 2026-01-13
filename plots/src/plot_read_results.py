import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# 1. Load Data
# Asegúrate de que el CSV se llama igual que en tu código Go
df = pd.read_csv("../test-go/read_test/read_evaluation_results.csv")

# 2. Setup Plot Style
sns.set_theme(style="whitegrid")
plt.rcParams.update({'font.family': 'serif', 'font.size': 12})

# Create Figure
plt.figure(figsize=(10, 6))

# 3. Plot Line Chart with Markers
sns.lineplot(
    data=df, 
    x="Num_Sensors", 
    y="Avg_Read_Time_ms", 
    hue="Architecture",
    style="Architecture",
    markers=True,
    dashes=False,
    linewidth=2.5,
    palette="magma" # Different palette to distinguish from ingestion test
)

# 4. Customization
plt.title("Full Digital Twin State Retrieval Latency")
plt.ylabel("Query Latency (ms)")
plt.xlabel("Number of Sensors (Graph Size)")

# Add a threshold line for "Real-Time Perception" (e.g., 1 second)
plt.axhline(y=200, color='r', linestyle='--', alpha=0.5)
plt.text(0, 1050, "Soft Real-Time Limit (1s)", color='r', fontsize=9)

plt.legend(title="Architecture", loc="upper left")
plt.grid(True, which="both", ls="--", alpha=0.5)

# 5. Save
plt.tight_layout()
plt.savefig("paper_read_latency.png", dpi=300)
print("Plot generated: paper_read_latency.png")