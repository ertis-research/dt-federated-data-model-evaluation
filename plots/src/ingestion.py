import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# ============================================================
# Load evaluation data
# ============================================================
df = pd.read_csv("../evaluation/output/ingestion_evaluation_results2_NUEVO.csv")

df["Scenario_Label"] = (
    df["Num_Sensors"].astype(str) + "@" + df["Freq_Hz"].astype(str) + " Hz"
)

# ============================================================
# Aggregate results
# ============================================================
df_stats = (
    df.groupby(["Step_ID", "Scenario_Label", "Architecture"], as_index=False)
    .agg(
        Latency_Mean=("Avg_Latency_ms", "mean"),
        Latency_Std=("Avg_Latency_ms", "std"),
        PacketLoss_Mean=("Packet_Loss_Network", "mean"),
        PacketLoss_Std=("Packet_Loss_Network", "std"),
        Throughput_Mean=("Throughput_msg_sec", "mean"),
        Throughput_Std=("Throughput_msg_sec", "std"),
        CPU_Mean=("DB_CPU_Avg_Cores", "mean"),
        CPU_Std=("DB_CPU_Avg_Cores", "std"),
        RAM_Mean=("DB_RAM_Max_MB", "mean"),
        RAM_Std=("DB_RAM_Max_MB", "std")
    )
).sort_values("Step_ID")

fed = df_stats[df_stats["Architecture"] == "Federated"].reset_index(drop=True)
cen = df_stats[df_stats["Architecture"] == "Centralized"].reset_index(drop=True)

# ============================================================
# IEEE-compliant plotting style
# ============================================================
plt.rcParams.update({
    "font.family": "serif",
    "font.size": 10,
    "axes.grid": True,
    "grid.linestyle": "--",
    "grid.alpha": 0.6,
    "axes.grid.axis": "y" # Solo líneas horizontales para gráficos de barras
})

# Configuración de los ejes X para las barras agrupadas
labels = fed["Scenario_Label"].tolist()
x = np.arange(len(labels))  # Posiciones en el eje X
width = 0.35                # Ancho de las barras

# Función auxiliar para evitar repetir código
def plot_bar_chart(y_mean_fed, y_std_fed, y_mean_cen, y_std_cen, ylabel, filename):
    fig, ax = plt.subplots(figsize=(4.0, 3.2)) # Un poco más ancho para las barras
    
    # Barras para Federated
    ax.bar(x - width/2, y_mean_fed, width, yerr=y_std_fed, 
           label='Federated', capsize=3, edgecolor='black', hatch='//', alpha=0.8)
    
    # Barras para Centralized
    ax.bar(x + width/2, y_mean_cen, width, yerr=y_std_cen, 
           label='Centralized', capsize=3, edgecolor='black', hatch='\\\\', alpha=0.8)
    
    ax.set_xlabel("Scenario (Devices@Frequency)")
    ax.set_ylabel(ylabel)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=30, ha="right")
    ax.legend(
        framealpha=0.7,        # Transparencia (0 = transparente, 1 = opaco)
        edgecolor="black",     # Borde negro para que se vea bien en el paper
        fontsize=9             # Un poco más pequeña para no tapar tanto
    )
    fig.tight_layout()
    fig.savefig(f"output/{filename}.pdf", bbox_inches="tight")
    plt.close(fig) # Cierra la figura para liberar memoria

# ============================================================
# Generar los 5 gráficos
# ============================================================
plot_bar_chart(fed["Latency_Mean"], fed["Latency_Std"], 
               cen["Latency_Mean"], cen["Latency_Std"], 
               "Average Latency (ms)", "ingestion_average_latency")

plot_bar_chart(fed["PacketLoss_Mean"], fed["PacketLoss_Std"], 
               cen["PacketLoss_Mean"], cen["PacketLoss_Std"], 
               "Packet Loss (messages)", "ingestion_packet_loss")

plot_bar_chart(fed["Throughput_Mean"], fed["Throughput_Std"], 
               cen["Throughput_Mean"], cen["Throughput_Std"], 
               "Throughput (msg/sec)", "ingestion_throughput")

plot_bar_chart(fed["CPU_Mean"], fed["CPU_Std"], 
               cen["CPU_Mean"], cen["CPU_Std"], 
               "CPU Usage (Cores)", "ingestion_cpu")

plot_bar_chart(fed["RAM_Mean"], fed["RAM_Std"], 
               cen["RAM_Mean"], cen["RAM_Std"], 
               "Memory Usage (MB)", "ingestion_ram")

print("All 5 bar plots generated successfully.")
