import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def generate_comparative_plots(df=None):
    if df is None:
        try: df = pd.read_csv("benchmark_results.csv")
        except: return

    sns.set_theme(style="whitegrid")
    
    # Etiqueta Completa
    df['Label'] = df['Scenario'] + "\n(" + df['Total_Nodes'].astype(str) + " nodes)"

    # --- PLOT CLAVE: DB WRITE LATENCY ---
    plt.figure(figsize=(10, 6))
    ax = sns.barplot(data=df, x="Label", y="DB_Latency_ms", hue="Architecture", palette="viridis")
    plt.title("Pure Database Write Latency (Graph vs Timescale)")
    plt.ylabel("Latency (ms)")
    plt.yscale("log") # Escala logarítmica para ver la diferencia masiva
    
    # Anotar valores
    for container in ax.containers:
        ax.bar_label(container, fmt='%.1f')
        
    plt.tight_layout()
    plt.savefig("plot_db_latency.png")

    # --- PLOT: E2E LATENCY (Para ver el impacto de la cola) ---
    plt.figure(figsize=(10, 6))
    sns.barplot(data=df, x="Label", y="E2E_Latency_ms", hue="Architecture", palette="mako")
    plt.title("End-to-End Latency (Includes Queueing & Network)")
    plt.ylabel("Latency (ms)")
    plt.tight_layout()
    plt.savefig("plot_e2e_latency.png")