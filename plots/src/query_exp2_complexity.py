"""
Plot generator for Sub-Experiment 2: Query Complexity (Q1, Q2, Q3) & Concurrent Workloads.

Generates:
  - output/query_exp2_complexity_comparison.pdf
  - output/query_exp2_concurrency_scaling.pdf  (one panel per query profile)

Usage:
  python query_exp2_complexity.py                           # Uses latest execution in evaluation/query/output/
  python query_exp2_complexity.py 2026-10-19_16-00-00       # Uses specific timestamp subfolder
  python query_exp2_complexity.py --dir /path/to/run_folder # Uses explicit directory path
"""

import argparse
import os
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

CSV_FILENAME = "exp2_complexity_concurrency.csv"


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


# Colors shared with the Sub-Experiment 1 figures (Centralized / Federated 8 ISUs)
CEN_COLOR = "#1f77b4"
FED_COLOR = "#9467bd"


def p95(x):
    return np.percentile(x, 95)


def p99(x):
    return np.percentile(x, 99)


def generate_exp2_plots(target_dir: str | None = None, output_dir: str = "output") -> None:
    """Load Sub-Experiment 2 CSV data and render query complexity & concurrency figures."""
    csv_path = resolve_csv_path(target_dir, CSV_FILENAME)
    print(f"[Exp2 Complexity & Concurrency] Loading data from: {csv_path}")

    df = pd.read_csv(csv_path)
    os.makedirs(output_dir, exist_ok=True)

    plt.rcParams.update({
        "font.family": "serif",
        "font.size": 10,
        "axes.grid": True,
        "grid.linestyle": "--",
        "grid.alpha": 0.6,
    })

    exp2 = df[df["Experiment_Type"] == "Exp2_Complexity_Concurrency"] if "Experiment_Type" in df.columns else df
    if exp2.empty:
        print("[Exp2 Complexity & Concurrency] Warning: No rows found for Exp2_Complexity_Concurrency.")
        return

    stats2 = (
        exp2.groupby(["Query_Type", "Concurrency", "Architecture"], as_index=False)
        .agg(
            Total_Median=("Total_Latency_ms", "median"),
            Total_P95=("Total_Latency_ms", p95),
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

        cen_vals = [
            c1[(c1["Query_Type"] == q) & (c1["Architecture"] == "Centralized")]["Total_Median"].mean()
            for q in q_types
        ]
        fed_vals = [
            c1[(c1["Query_Type"] == q) & (c1["Architecture"].str.startswith("Federated"))]["Total_Median"].mean()
            for q in q_types
        ]

        ax.bar(x - width / 2, cen_vals, width, label="Centralized", color=CEN_COLOR,
               edgecolor="black", hatch="\\\\", alpha=0.85)
        ax.bar(x + width / 2, fed_vals, width, label="Federated (8 ISUs)", color=FED_COLOR,
               edgecolor="black", hatch="//", alpha=0.85)

        ax.set_xticks(x)
        ax.set_xticklabels(q_labels)
        ax.set_ylabel("Median End-to-End Latency (ms)")
        ax.legend(fontsize=8, edgecolor="black")
        fig.tight_layout()
        fig.savefig(os.path.join(output_dir, "query_exp2_complexity_comparison.pdf"), bbox_inches="tight")
        plt.close(fig)

    # Figure 2B: Concurrent Query Scalability (C = 1, 5, 10, 20), one panel per query profile.
    # Concurrency is a burst of C simultaneous queries per run; each run's latency is the mean
    # over its C queries, and median/P95 are taken over the 30 runs.
    q_types = ["Q1_Selective", "Q2_Filtering", "Q3_Inference"]
    q_titles = ["Q1: Selective", "Q2: Filtering", "Q3: Inference"]
    present = [q for q in q_types if q in set(stats2["Query_Type"])]
    if present:
        fig, axes = plt.subplots(1, len(present), figsize=(2.6 * len(present), 3.0), squeeze=False)
        for ax, q in zip(axes[0], present):
            qs = stats2[stats2["Query_Type"] == q]
            for arch_prefix, label, marker, color in [
                ("Centralized", "Centralized", "o", CEN_COLOR),
                ("Federated", "Federated (8 ISUs)", "s", FED_COLOR),
            ]:
                sub = qs[qs["Architecture"].str.startswith(arch_prefix)].sort_values("Concurrency")
                if sub.empty:
                    continue
                ax.plot(sub["Concurrency"], sub["Total_Median"], marker=marker, color=color, label=label)
                ax.fill_between(sub["Concurrency"], sub["Total_Median"], sub["Total_P95"],
                                color=color, alpha=0.15, linewidth=0)
            ax.set_title(q_titles[q_types.index(q)], fontsize=9)
            ax.set_xticks([1, 5, 10, 20])
            ax.set_xlabel("Concurrent Queries (C)")
        axes[0][0].set_ylabel("End-to-End Query Latency (ms)")
        handles, _ = axes[0][0].get_legend_handles_labels()
        handles.append(Patch(facecolor="grey", alpha=0.3, label="Median to P95"))
        fig.legend(handles=handles, fontsize=8, ncol=3, loc="lower center",
                   bbox_to_anchor=(0.5, 1.0), edgecolor="black")
        fig.tight_layout()
        fig.savefig(os.path.join(output_dir, "query_exp2_concurrency_scaling.pdf"), bbox_inches="tight")
        plt.close(fig)

    print(f"[Exp2 Complexity & Concurrency] Figures saved in '{output_dir}/'.")


def main():
    parser = argparse.ArgumentParser(
        description="Generate Sub-Experiment 2 (Query Complexity & Concurrency) plots."
    )
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
    generate_exp2_plots(target_dir=target, output_dir=args.output)


if __name__ == "__main__":
    main()
