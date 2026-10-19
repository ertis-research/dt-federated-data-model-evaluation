import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# ============================================================
# Load evaluation results
# ============================================================
csv_path = "../evaluation/output/query_evaluation_results_v2.csv"
if not os.path.exists(csv_path):
    csv_path = "../evaluation/output/query_evaluation_results2.csv"

df = pd.read_csv(csv_path)
os.makedirs("output", exist_ok=True)

plt.rcParams.update({
    "font.family": "serif",
    "font.size": 10,
    "axes.grid": True,
    "grid.linestyle": "--",
    "grid.alpha": 0.6,
})

if "Experiment_Type" in df.columns:
    # Helper functions for percentiles
    def p95(x):
        return np.percentile(x, 95)

    def p99(x):
        return np.percentile(x, 99)

    # =========================================================================
    # SUB-EXPERIMENT 1: Horizontal Scalability across N ISUs (1, 2, 4, 8)
    # =========================================================================
    exp1 = df[df["Experiment_Type"] == "Exp1_Scalability"]
    if not exp1.empty:
        stats1 = (
            exp1.groupby(["Num_Sensors", "Architecture", "Num_ISUs"], as_index=False)
            .agg(
                Total_Mean=("Total_Latency_ms", "mean"),
                Total_Std=("Total_Latency_ms", "std"),
                Total_P95=("Total_Latency_ms", p95),
                Total_P99=("Total_Latency_ms", p99),
                S1_Mean=("Stage1_Anchor_ms", "mean"),
                S2_Mean=("Stage2_ISU_IO_ms", "mean"),
                S3_Mean=("Stage3_Hydration_ms", "mean"),
                S4_Mean=("Stage4_QueryExec_ms", "mean"),
                Mem_Mean=("Memory_Pressure_MB_per_Query", "mean"),
            )
        )

        arch_list = [
            ("Centralized", "Centralized", "o", "--"),
            ("Federated_1ISU", "Federated (1 ISU)", "s", "-"),
            ("Federated_2ISU", "Federated (2 ISUs)", "D", "-"),
            ("Federated_4ISU", "Federated (4 ISUs)", "^", "-"),
            ("Federated_8ISU", "Federated (8 ISUs)", "P", "-"),
        ]

        # Figure 1A: End-to-End Total Query Latency (Mean)
        plt.figure(figsize=(4.2, 3.2))
        for key, label, marker, ls in arch_list:
            sub = stats1[stats1["Architecture"] == key].sort_values("Num_Sensors")
            if sub.empty:
                continue
            plt.plot(sub["Num_Sensors"], sub["Total_Mean"], marker=marker, linestyle=ls, label=label)
            plt.fill_between(
                sub["Num_Sensors"],
                sub["Total_Mean"] - sub["Total_Std"],
                sub["Total_Mean"] + sub["Total_Std"],
                alpha=0.15,
            )
        plt.xlabel("Number of Devices")
        plt.ylabel("End-to-End Query Latency (ms)")
        plt.legend(fontsize=8, edgecolor="black")
        plt.tight_layout()
        plt.savefig("output/query_total_latency.pdf", bbox_inches="tight")
        plt.close()

        # Figure 1B: Tail Latency (P99) across N ISUs
        plt.figure(figsize=(4.2, 3.2))
        for key, label, marker, ls in arch_list:
            sub = stats1[stats1["Architecture"] == key].sort_values("Num_Sensors")
            if sub.empty:
                continue
            plt.plot(sub["Num_Sensors"], sub["Total_P99"], marker=marker, linestyle=ls, label=label)
        plt.xlabel("Number of Devices")
        plt.ylabel("P99 Query Tail Latency (ms)")
        plt.legend(fontsize=8, edgecolor="black")
        plt.tight_layout()
        plt.savefig("output/query_tail_latency_p99.pdf", bbox_inches="tight")
        plt.close()

        # Figure 1C: Stage 2 Parallel ISU I/O Wait Scaling (1 vs 2 vs 4 vs 8 ISUs)
        plt.figure(figsize=(4.2, 3.2))
        for key, label, marker, ls in arch_list[1:]:
            sub = stats1[stats1["Architecture"] == key].sort_values("Num_Sensors")
            if sub.empty:
                continue
            plt.plot(sub["Num_Sensors"], sub["S2_Mean"], marker=marker, linestyle=ls, label=label)
        plt.xlabel("Number of Devices")
        plt.ylabel("Parallel ISU Retrieval I/O (ms)")
        plt.legend(fontsize=8, edgecolor="black")
        plt.tight_layout()
        plt.savefig("output/query_io_wait_comparison.pdf", bbox_inches="tight")
        plt.close()

        # Figure 1D: Stacked 4-Stage Breakdown at 5000 Sensors across ISU counts
        sub5000 = stats1[(stats1["Num_Sensors"] == 5000) & (stats1["Num_ISUs"] > 0)].sort_values("Num_ISUs")
        if not sub5000.empty:
            fig, ax = plt.subplots(figsize=(4.2, 3.2))
            x_isu = np.arange(len(sub5000))
            isu_labels = [f"{n} ISU" + ("s" if n > 1 else "") for n in sub5000["Num_ISUs"]]

            s1 = sub5000["S1_Mean"].values
            s2 = sub5000["S2_Mean"].values
            s3 = sub5000["S3_Mean"].values
            s4 = sub5000["S4_Mean"].values

            ax.bar(x_isu, s1, width=0.5, label="Stage 1: Anchor Resolution", edgecolor="black")
            ax.bar(x_isu, s2, width=0.5, bottom=s1, label="Stage 2: Parallel ISU I/O", edgecolor="black", hatch="//")
            ax.bar(x_isu, s3, width=0.5, bottom=s1 + s2, label="Stage 3: Semantic Hydration", edgecolor="black", hatch="..")
            ax.bar(x_isu, s4, width=0.5, bottom=s1 + s2 + s3, label="Stage 4: Query/Inference Exec", edgecolor="black", hatch="xx")

            ax.set_xticks(x_isu)
            ax.set_xticklabels(isu_labels)
            ax.set_xlabel("Data Plane Configuration (5000 Devices)")
            ax.set_ylabel("Latency Breakdown (ms)")
            ax.legend(fontsize=8, edgecolor="black")
            fig.tight_layout()
            fig.savefig("output/query_orchestration_overhead.pdf", bbox_inches="tight")
            plt.close(fig)

    # =========================================================================
    # SUB-EXPERIMENT 2: Query Complexity (Q1, Q2, Q3) & Concurrent Workloads
    # =========================================================================
    exp2 = df[df["Experiment_Type"] == "Exp2_Complexity_Concurrency"]
    if not exp2.empty:
        stats2 = (
            exp2.groupby(["Query_Type", "Concurrency", "Architecture"], as_index=False)
            .agg(
                Total_Mean=("Total_Latency_ms", "mean"),
                Total_P99=("Total_Latency_ms", p99),
            )
        )

        # Figure 2A: Query Complexity Comparison (Q1 Selective, Q2 Filtering, Q3 Inference) at Concurrency=1
        c1 = stats2[stats2["Concurrency"] == 1]
        if not c1.empty:
            fig, ax = plt.subplots(figsize=(4.2, 3.2))
            q_types = ["Q1_Selective", "Q2_Filtering", "Q3_Inference"]
            q_labels = ["Q1: Selective\nSub-graph", "Q2: Multi-hop\nFiltering", "Q3: Fault\nInference"]
            x = np.arange(len(q_types))
            width = 0.35

            cen_vals = [c1[(c1["Query_Type"] == q) & (c1["Architecture"] == "Centralized")]["Total_Mean"].mean() for q in q_types]
            fed_vals = [c1[(c1["Query_Type"] == q) & (c1["Architecture"].str.startswith("Federated"))]["Total_Mean"].mean() for q in q_types]

            ax.bar(x - width / 2, fed_vals, width, label="Federated (8 ISUs)", edgecolor="black", hatch="//", alpha=0.85)
            ax.bar(x + width / 2, cen_vals, width, label="Centralized", edgecolor="black", hatch="\\\\", alpha=0.85)

            ax.set_xticks(x)
            ax.set_xticklabels(q_labels)
            ax.set_ylabel("End-to-End Latency (ms)")
            ax.legend(fontsize=8, edgecolor="black")
            fig.tight_layout()
            fig.savefig("output/query_complexity_comparison.pdf", bbox_inches="tight")
            plt.close(fig)

        # Figure 2B: Concurrent Query Scalability (C = 1, 5, 10, 20) for Q3 Inference
        q3 = stats2[stats2["Query_Type"] == "Q3_Inference"]
        if not q3.empty:
            plt.figure(figsize=(4.2, 3.2))
            for arch_prefix, label, marker in [("Federated", "Federated (8 ISUs)", "s"), ("Centralized", "Centralized", "o")]:
                sub = q3[q3["Architecture"].str.startswith(arch_prefix)].sort_values("Concurrency")
                if sub.empty:
                    continue
                plt.plot(sub["Concurrency"], sub["Total_Mean"], marker=marker, label=f"{label} (Mean)")
                plt.plot(sub["Concurrency"], sub["Total_P99"], marker=marker, linestyle="--", label=f"{label} (P99)")
            plt.xlabel("Concurrent Clients (Queries in Parallel)")
            plt.ylabel("End-to-End Query Latency (ms)")
            plt.legend(fontsize=8, edgecolor="black")
            plt.tight_layout()
            plt.savefig("output/query_concurrency_scaling.pdf", bbox_inches="tight")
            plt.close()

    # =========================================================================
    # SUB-EXPERIMENT 3: Partition Skew Analysis (Uniform vs Moderate vs Hotspot)
    # =========================================================================
    exp3 = df[df["Experiment_Type"] == "Exp3_Skew"]
    if not exp3.empty:
        stats3 = (
            exp3.groupby(["Partition_Mode"], as_index=False)
            .agg(
                Total_Mean=("Total_Latency_ms", "mean"),
                Total_P99=("Total_Latency_ms", p99),
                S2_IO_Mean=("Stage2_ISU_IO_ms", "mean"),
            )
        )
        modes = ["Uniform", "ModerateSkew", "HotspotSkew"]
        mode_labels = ["Uniform\n(Balanced)", "Moderate Skew\n(50% in ISU-0)", "Hotspot Skew\n(80% in ISU-0)"]
        stats3["Order"] = stats3["Partition_Mode"].apply(lambda m: modes.index(m) if m in modes else 99)
        stats3 = stats3.sort_values("Order")

        fig, ax = plt.subplots(figsize=(4.2, 3.2))
        x = np.arange(len(stats3))
        width = 0.28

        ax.bar(x - width, stats3["S2_IO_Mean"], width, label="Stage 2: ISU I/O Wait", edgecolor="black", hatch="..")
        ax.bar(x, stats3["Total_Mean"], width, label="Total E2E Mean", edgecolor="black", hatch="//")
        ax.bar(x + width, stats3["Total_P99"], width, label="Total E2E P99", edgecolor="black", hatch="\\\\")

        ax.set_xticks(x)
        ax.set_xticklabels(mode_labels[:len(stats3)])
        ax.set_ylabel("Latency (ms)")
        ax.legend(fontsize=8, edgecolor="black")
        fig.tight_layout()
        fig.savefig("output/query_partition_skew.pdf", bbox_inches="tight")
        plt.close(fig)

    print("All end-to-end query, complexity, concurrency, and skew plots generated in output/.")
else:
    print("Legacy CSV detected. Run the new query benchmark to generate v2 results.")