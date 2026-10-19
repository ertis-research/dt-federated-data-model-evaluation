import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# ============================================================
# Load evaluation data
# ============================================================
csv_path = "../evaluation/output/ingestion_evaluation_results_v2.csv"
if not os.path.exists(csv_path):
    csv_path = "../evaluation/output/ingestion_evaluation_results2_NUEVO.csv"

df = pd.read_csv(csv_path)
os.makedirs("output", exist_ok=True)

df["Scenario_Label"] = (
    df["Num_Sensors"].astype(str) + "@" + df["Freq_Hz"].astype(str) + "Hz"
)

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

# Check if new multi-ISU / Batched columns exist
has_multi_isu = "P99_Latency_ms" in df.columns

if has_multi_isu:
    df_stats = (
        df.groupby(["Step_ID", "Scenario_Label", "Architecture", "Num_ISUs"], as_index=False)
        .agg(
            Latency_Mean=("Avg_Latency_ms", "mean"),
            Latency_Std=("Avg_Latency_ms", "std"),
            P50_Mean=("P50_Latency_ms", "mean"),
            P95_Mean=("P95_Latency_ms", "mean"),
            P99_Mean=("P99_Latency_ms", "mean"),
            P99_Std=("P99_Latency_ms", "std"),
            PacketLoss_Mean=("Packet_Loss_Network", "mean"),
            Throughput_Mean=("Throughput_msg_sec", "mean"),
            Throughput_Std=("Throughput_msg_sec", "std"),
            CPU_Mean=("DB_CPU_Avg_Cores", "mean"),
            CPU_Std=("DB_CPU_Avg_Cores", "std"),
            RAM_Mean=("DB_RAM_Max_MB", "mean"),
            RAM_Std=("DB_RAM_Max_MB", "std"),
        )
    ).sort_values("Step_ID")

    arch_order = [
        ("Centralized_1to1", "Centralized (1-to-1)", "o", "--"),
        ("Centralized_Batched", "Centralized (Batched)", "^", "-."),
        ("Federated_1ISU", "Federated (1 ISU)", "s", "-"),
        ("Federated_2ISU", "Federated (2 ISUs)", "D", "-"),
        ("Federated_4ISU", "Federated (4 ISUs)", "v", "-"),
        ("Federated_8ISU", "Federated (8 ISUs)", "P", "-"),
    ]

    scenarios = df_stats[["Step_ID", "Scenario_Label"]].drop_duplicates().sort_values("Step_ID")
    x = np.arange(len(scenarios))
    labels = scenarios["Scenario_Label"].tolist()

    def plot_multi_series(metric_col, ylabel, filename, log_scale=False):
        fig, ax = plt.subplots(figsize=(4.8, 3.4))
        for arch_key, arch_label, marker, ls in arch_order:
            sub = df_stats[df_stats["Architecture"] == arch_key].sort_values("Step_ID")
            if sub.empty:
                continue
            ax.plot(
                np.arange(len(sub)),
                sub[metric_col],
                marker=marker,
                linestyle=ls,
                label=arch_label,
                linewidth=1.5,
                markersize=5,
            )
        ax.set_xlabel("Scenario (Devices @ Frequency)")
        ax.set_ylabel(ylabel)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=30, ha="right")
        if log_scale:
            ax.set_yscale("log")
        ax.legend(fontsize=8, framealpha=0.85, edgecolor="black")
        fig.tight_layout()
        fig.savefig(f"output/{filename}.pdf", bbox_inches="tight")
        plt.close(fig)

    # 1. Average Latency (Log scale helps compare Centralized vs Multi-ISU Federated)
    plot_multi_series("Latency_Mean", "Average Latency (ms)", "ingestion_average_latency", log_scale=True)

    # 2. Tail Latency (P99)
    plot_multi_series("P99_Mean", "P99 Tail Latency (ms)", "ingestion_tail_latency_p99", log_scale=True)

    # 3. Throughput
    plot_multi_series("Throughput_Mean", "Throughput (msg/sec)", "ingestion_throughput", log_scale=False)

    # 4. CPU Usage
    plot_multi_series("CPU_Mean", "Total DB CPU Usage (Cores)", "ingestion_cpu", log_scale=False)

    # 5. RAM Usage
    plot_multi_series("RAM_Mean", "Total DB Memory (MB)", "ingestion_ram", log_scale=False)

    print("All multi-ISU & ablation ingestion plots generated successfully in output/.")
else:
    print("Legacy CSV detected. Run the new ingestion benchmark to generate v2 results.")
