import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# ============================================================
# Load evaluation results
# ============================================================
df = pd.read_csv("../evaluation/output/query_evaluation_results2.csv")

# ============================================================
# Aggregate results across multiple runs
# Mean and standard deviation are computed for robustness
# ============================================================
df_stats = (
    df.groupby(["Num_Sensors", "Architecture"], as_index=False)
    .agg(
        Total_Latency_Mean=("Total_Latency_ms", "mean"),
        Total_Latency_Std=("Total_Latency_ms", "std"),
        Orchestration_Mean=("Orchestration_Overhead_ms", "mean"),
        Memory_Mean=("Memory_Pressure_MB_per_Query", "mean"),
        IOWait_Mean=("IO_Wait_ms", "mean"),
    )
)

# Split architectures for explicit control of plotting order
fed = df_stats[df_stats["Architecture"] == "Federated"]
cen = df_stats[df_stats["Architecture"] == "Centralized"]

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
# FIGURE: Latency
# ============================================================
plt.figure(figsize=(3.6, 3.2))

# Federated architecture
plt.plot(
    fed["Num_Sensors"],
    fed["Total_Latency_Mean"],
    marker="s",
    label="Federated",
)
plt.fill_between(
    fed["Num_Sensors"],
    fed["Total_Latency_Mean"] - fed["Total_Latency_Std"],
    fed["Total_Latency_Mean"] + fed["Total_Latency_Std"],
    alpha=0.2,
)

# Centralized architecture
plt.plot(
    cen["Num_Sensors"],
    cen["Total_Latency_Mean"],
    marker="o",
    label="Centralized",
)
plt.fill_between(
    cen["Num_Sensors"],
    cen["Total_Latency_Mean"] - cen["Total_Latency_Std"],
    cen["Total_Latency_Mean"] + cen["Total_Latency_Std"],
    alpha=0.2,
)

# ============================================================
# Plot annotations and labels
# ============================================================
plt.xlabel("Number of Devices")
plt.ylabel("Average Latency (ms)")



plt.legend()
plt.tight_layout()

# ============================================================
# Save figure
# ============================================================
plt.savefig(
    "output/query_total_latency.pdf",
    bbox_inches="tight"
)

print("Plot generated: query_total_latency.pdf")


# ============================================================
# FIGURE 2: Orchestration Overhead
# ============================================================
plt.figure(figsize=(3.6, 3.2))

# Federated architecture
plt.plot(
    fed["Num_Sensors"],
    fed["Orchestration_Mean"],
    marker="s",
    label="Federated",
)

# Centralized architecture
plt.plot(
    cen["Num_Sensors"],
    cen["Orchestration_Mean"],
    marker="o",
    label="Centralized",
)

# ============================================================
# Plot annotations and labels
# ============================================================
plt.xlabel("Number of Devices")
plt.ylabel("Average Overhead (ms)")

plt.legend()
plt.tight_layout()

# ============================================================
# Save figure
# ============================================================
plt.savefig(
    "output/query_orchestration_overhead.pdf",
    bbox_inches="tight"
)

print("Plot generated: query_orchestration_overhead.pdf")

# ============================================================
# FIGURE 3: Memory Pressure
# ============================================================
plt.figure(figsize=(3.6, 3.2))

# Federated architecture
plt.plot(
    fed["Num_Sensors"],
    fed["Memory_Mean"],
    marker="s",
    label="Federated",
)

# Centralized architecture
plt.plot(
    cen["Num_Sensors"],
    cen["Memory_Mean"],
    marker="o",
    label="Centralized",
)

plt.xlabel("Number of Devices")
plt.ylabel("Memory Pressure (MB/Query)")
plt.legend()
plt.tight_layout()

plt.savefig(
    "output/query_memory_pressure.pdf",
    bbox_inches="tight"
)
print("Plot generated: query_memory_pressure.pdf")

# ============================================================
# FIGURE 4 (NUEVA): Orchestration / Processing Overhead
# Demuestra el costo computacional de la arquitectura
# ============================================================
plt.figure(figsize=(3.6, 3.2))

# Federated architecture
plt.plot(
    fed["Num_Sensors"],
    fed["Orchestration_Mean"],
    marker="s",
    label="Federated",
)

# Centralized architecture
plt.plot(
    cen["Num_Sensors"],
    cen["Orchestration_Mean"],
    marker="o",
    label="Centralized",
)

# ============================================================
# Plot annotations and labels
# ============================================================
plt.xlabel("Number of Devices")
plt.ylabel("Processing Overhead (ms)")

# Recomendación: Ubicar la leyenda donde no tape las líneas
# Como Centralized es 0 y Federated es > 0, el centro/derecha suele estar libre
plt.legend(loc="center right")
plt.tight_layout()

# ============================================================
# Save figure
# ============================================================
plt.savefig(
    "output/query_orchestration_overhead.pdf",
    bbox_inches="tight"
)

print("Plot generated: query_orchestration_overhead.pdf")

# ============================================================
# FIGURE 2 (NUEVA): Pure I/O Wait Comparison (Lines & Markers)
# Compara el tiempo puro de I/O manteniendo el estilo de las otras figuras
# ============================================================
plt.figure(figsize=(3.6, 3.2))

# Federated architecture
plt.plot(
    fed["Num_Sensors"],
    fed["IOWait_Mean"],
    marker="s",
    label="Federated",
)

# Centralized architecture
plt.plot(
    cen["Num_Sensors"],
    cen["IOWait_Mean"],
    marker="o",
    label="Centralized",
)

# ============================================================
# Plot annotations and labels
# ============================================================
plt.xlabel("Number of Devices")
plt.ylabel("Average I/O Wait (ms)")

plt.legend()
plt.tight_layout()

# ============================================================
# Save figure
# ============================================================
plt.savefig(
    "output/query_io_wait_comparison.pdf",
    bbox_inches="tight"
)

print("Plot generated: query_io_wait_comparison.pdf")