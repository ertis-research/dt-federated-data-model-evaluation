"""
Plot generator for Sub-Experiment 1: Horizontal Scalability across N ISUs (1, 2, 4, 8).

Generates:
  - output/query_exp1_total_latency.pdf          (Median lines, Median-to-P95 bands)
  - output/query_exp1_orchestration_overhead.pdf (4-Stage Median Latency Breakdown at 5000 devices, full system)
  - output/query_exp1_subbatch_ablation.pdf      (Stage 2 median vs ISUs, with/without sub-batching)

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
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

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


SUB_BATCH_DEFAULT = 250  # evaluation/query ISUSubBatch (full-system orchestrator configuration)

# Fixed colors per architecture, shared by all query figures
ARCH_COLORS = {
    "Centralized": "#1f77b4",
    "Federated_1ISU": "#ff7f0e",
    "Federated_2ISU": "#2ca02c",
    "Federated_4ISU": "#d62728",
    "Federated_8ISU": "#9467bd",
}


def add_sub_batch_column(df: pd.DataFrame) -> pd.DataFrame:
    """Older executions have no Sub_Batch column: they always used the default sub-batching."""
    if "Sub_Batch" in df.columns:
        return df
    df = df.copy()
    df["Sub_Batch"] = np.where(df["Num_ISUs"] > 0, SUB_BATCH_DEFAULT, 0)
    return df


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
    exp1 = add_sub_batch_column(exp1)

    stats_all = (
        exp1.groupby(["Num_Sensors", "Architecture", "Num_ISUs", "Sub_Batch"], as_index=False)
        .agg(
            Total_Median=("Total_Latency_ms", "median"),
            Total_P95=("Total_Latency_ms", p95),
            Total_P99=("Total_Latency_ms", p99),
            S1_Median=("Stage1_Anchor_ms", "median"),
            S2_Median=("Stage2_ISU_IO_ms", "median"),
            S3_Median=("Stage3_Hydration_ms", "median"),
            S4_Median=("Stage4_QueryExec_ms", "median"),
        )
    )
    # Full system: Centralized baseline + Federated with intra-ISU sub-batching
    stats1 = stats_all[(stats_all["Num_ISUs"] == 0) | (stats_all["Sub_Batch"] > 0)]

    arch_list = [
        ("Centralized", "Centralized", "o"),
        ("Federated_1ISU", "Fed. (1 ISU)", "s"),
        ("Federated_2ISU", "Fed. (2 ISUs)", "D"),
        ("Federated_4ISU", "Fed. (4 ISUs)", "^"),
        ("Federated_8ISU", "Fed. (8 ISUs)", "P"),
    ]

    # Figure 1A: End-to-End Total Query Latency of the full system (median lines).
    # The P95 is drawn as a shaded band only for Centralized and Fed. (8 ISUs): the federated
    # configurations overlap (ISU count barely affects latency), so five dashed P95 lines are unreadable.
    fig, ax = plt.subplots(figsize=(4.6, 4.0))
    for key, label, marker in arch_list:
        sub = stats1[stats1["Architecture"] == key].sort_values("Num_Sensors")
        if sub.empty:
            continue
        highlight = key in ("Centralized", "Federated_8ISU")
        ax.plot(
            sub["Num_Sensors"],
            sub["Total_Median"],
            marker=marker,
            linestyle="-",
            linewidth=1.6 if highlight else 1.0,
            markersize=5 if highlight else 4,
            color=ARCH_COLORS[key],
            alpha=1.0 if highlight else 0.6,
            label=label,
        )
        if highlight:
            ax.fill_between(sub["Num_Sensors"], sub["Total_Median"], sub["Total_P95"],
                            color=ARCH_COLORS[key], alpha=0.15, linewidth=0)

    band_handle = Patch(facecolor="grey", alpha=0.3, label="Median to P95")
    handles, _ = ax.get_legend_handles_labels()
    ax.locator_params(axis="y", nbins=10)
    ax.set_xlabel("Number of Devices")
    ax.set_ylabel("End-to-End Query Latency (ms)")
    ax.legend(
        handles=handles + [band_handle],
        fontsize=8,
        ncol=2,
        loc="upper left",
        edgecolor="black",
        framealpha=0.9,
        columnspacing=0.8,
        handletextpad=0.4,
    )
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, "query_exp1_total_latency.pdf"), bbox_inches="tight")
    plt.close(fig)

    # Figure 1B: Stacked 4-Stage median breakdown of the full system at 5000 devices
    # (Centralized as reference; Federated with sub-batching). The sub-batching ablation is
    # compared on Stage 2 only (Figure 1C), because its variants are measured sequentially.
    sub5000 = stats1[stats1["Num_Sensors"] == 5000].sort_values("Num_ISUs")
    if not sub5000.empty:
        fig, ax = plt.subplots(figsize=(4.6, 3.4))
        x_pos = np.arange(len(sub5000))
        bar_labels = ["Cen." if n == 0 else f"{n} ISU" + ("s" if n > 1 else "") for n in sub5000["Num_ISUs"]]
        stage_cols = ["S1_Median", "S2_Median", "S3_Median", "S4_Median"]
        stage_labels = ["Stage 1: Anchor Resolution", "Stage 2: Parallel ISU I/O",
                        "Stage 3: Semantic Hydration", "Stage 4: Query/Inference Exec"]
        hatches = ["", "//", "..", "xx"]
        bottom = np.zeros(len(sub5000))
        for col, label, hatch in zip(stage_cols, stage_labels, hatches):
            vals = sub5000[col].values
            ax.bar(x_pos, vals, width=0.55, bottom=bottom, label=label, edgecolor="black", hatch=hatch)
            bottom += vals

        ax.set_xticks(x_pos)
        ax.set_xticklabels(bar_labels)
        ax.set_xlabel("Architecture (5000 Devices)")
        ax.set_ylabel("Median Latency Breakdown (ms)")
        ax.legend(fontsize=7, edgecolor="black")
        fig.tight_layout()
        fig.savefig(os.path.join(output_dir, "query_exp1_orchestration_overhead.pdf"), bbox_inches="tight")
        plt.close(fig)

    # Figure 1C: Sub-batching ablation, Stage 2 (ISU I/O) median vs number of ISUs per sensor count
    fed_all = stats_all[stats_all["Num_ISUs"] > 0]
    if {SUB_BATCH_DEFAULT, 0} <= set(fed_all["Sub_Batch"]):
        fig, ax = plt.subplots(figsize=(4.6, 3.4))
        counts = sorted(fed_all["Num_Sensors"].unique())
        colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]
        for c_idx, count in enumerate(counts):
            for variant, ls in ((SUB_BATCH_DEFAULT, "-"), (0, "--")):
                sub = fed_all[(fed_all["Num_Sensors"] == count) & (fed_all["Sub_Batch"] == variant)].sort_values("Num_ISUs")
                if sub.empty:
                    continue
                ax.plot(sub["Num_ISUs"], sub["S2_Median"], marker="o", markersize=4, linestyle=ls,
                        color=colors[c_idx % len(colors)], label=f"{count} devices" if variant > 0 else None)
        ax.set_xscale("log", base=2)
        ax.set_xticks([1, 2, 4, 8])
        ax.set_xticklabels(["1", "2", "4", "8"])
        ax.set_xlabel("Number of ISUs")
        ax.set_ylabel("Median Stage 2 ISU I/O (ms)")
        style_handles = [
            Line2D([0], [0], color="black", lw=1.5, ls="-", label=f"Sub-batch {SUB_BATCH_DEFAULT}"),
            Line2D([0], [0], color="black", lw=1.5, ls="--", label="One call per ISU"),
        ]
        handles, _ = ax.get_legend_handles_labels()
        ax.legend(handles=handles + style_handles, fontsize=7, ncol=2, edgecolor="black")
        fig.tight_layout()
        fig.savefig(os.path.join(output_dir, "query_exp1_subbatch_ablation.pdf"), bbox_inches="tight")
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
