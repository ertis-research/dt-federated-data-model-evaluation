"""
Plot generator for Sub-Experiment 1: Horizontal Scalability across N ISUs (1, 2, 4, 8).

Generates:
  - output/query_total_latency.pdf
  - output/query_tail_latency_p99.pdf
  - output/query_io_wait_comparison.pdf
  - output/query_orchestration_overhead.pdf

Usage:
  python query_exp1_scalability.py                           # Uses latest execution in evaluation/query/output/
  python query_exp1_scalability.py 2026-10-19_16-00-00       # Uses specific timestamp subfolder
  python query_exp1_scalability.py --dir /path/to/run_folder # Uses explicit directory path
"""

import argparse
import os
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

CSV_FILENAME = "exp1_scalability.csv"


def resolve_csv_path(target_dir: str | None, csv_filename: str) -> Path:
    """Resolve the path to the experiment CSV from a user-specified directory or the latest execution."""
    repo_root = Path(__file__).resolve().parents[2]
    base_output_dir = repo_root / "evaluation" / "query" / "output"

    if target_dir:
        candidate = Path(target_dir)
        if candidate.is_file():
            return candidate
        if not candidate.is_dir():
            candidate = base_output_dir / target_dir
        csv_file = candidate / csv_filename
        if not csv_file.exists():
            raise FileNotFoundError(f"CSV file '{csv_filename}' not found in '{candidate}'.")
        return csv_file

    if not base_output_dir.exists():
        raise FileNotFoundError(f"Base output directory not found: '{base_output_dir}'.")

    # Sort timestamped directories from newest to oldest and pick the latest containing this CSV
    subdirs = sorted([d for d in base_output_dir.iterdir() if d.is_dir()], key=lambda d: d.name, reverse=True)
    for d in subdirs:
        csv_file = d / csv_filename
        if csv_file.exists():
            return csv_file

    raise FileNotFoundError(
        f"No execution folder containing '{csv_filename}' found inside '{base_output_dir}'."
    )


def p95(x):
    return np.percentile(x, 95)


def p99(x):
    return np.percentile(x, 99)


def generate_exp1_plots(target_dir: str | None = None, output_dir: str = "output") -> None:
    """Load Sub-Experiment 1 CSV data and render scalability figures."""
    csv_path = resolve_csv_path(target_dir, CSV_FILENAME)
    print(f"[Exp1 Scalability] Loading data from: {csv_path}")

    df = pd.read_csv(csv_path)
    os.makedirs(output_dir, exist_ok=True)

    plt.rcParams.update({
        "font.family": "serif",
        "font.size": 10,
        "axes.grid": True,
        "grid.linestyle": "--",
        "grid.alpha": 0.6,
    })

    exp1 = df[df["Experiment_Type"] == "Exp1_Scalability"] if "Experiment_Type" in df.columns else df
    if exp1.empty:
        print("[Exp1 Scalability] Warning: No rows found for Exp1_Scalability.")
        return

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
    plt.savefig(os.path.join(output_dir, "query_total_latency.pdf"), bbox_inches="tight")
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
    plt.savefig(os.path.join(output_dir, "query_tail_latency_p99.pdf"), bbox_inches="tight")
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
    plt.savefig(os.path.join(output_dir, "query_io_wait_comparison.pdf"), bbox_inches="tight")
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
        fig.savefig(os.path.join(output_dir, "query_orchestration_overhead.pdf"), bbox_inches="tight")
        plt.close(fig)

    print(f"[Exp1 Scalability] Figures saved in '{output_dir}/'.")


def main():
    parser = argparse.ArgumentParser(description="Generate Sub-Experiment 1 (Horizontal Scalability) plots.")
    parser.add_argument(
        "run_folder",
        nargs="?",
        default=None,
        help="Optional timestamp folder name or path (defaults to latest execution).",
    )
    parser.add_argument(
        "-d", "--dir",
        dest="dir_flag",
        default=None,
        help="Optional timestamp folder name or path (defaults to latest execution).",
    )
    parser.add_argument(
        "-o", "--output",
        default="output",
        help="Output directory for generated PDF plots (default: output).",
    )
    args = parser.parse_args()
    target = args.dir_flag or args.run_folder
    generate_exp1_plots(target_dir=target, output_dir=args.output)


if __name__ == "__main__":
    main()
