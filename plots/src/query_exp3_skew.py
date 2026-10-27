"""
Plot generator for Sub-Experiment 3: Partition Skew Analysis (Uniform vs Moderate vs Hotspot).

Generates:
  - output/query_exp3_partition_skew.pdf

Usage:
  python query_exp3_skew.py                           # Uses latest execution in evaluation/query/output/
  python query_exp3_skew.py 2026-10-19_16-00-00       # Uses specific timestamp subfolder
  python query_exp3_skew.py --dir /path/to/run_folder # Uses explicit directory path
"""

import argparse
import os
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

CSV_FILENAME = "exp3_skew.csv"


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


def p95(x):
    return np.percentile(x, 95)


def p99(x):
    return np.percentile(x, 99)


def generate_exp3_plots(target_dir: str | None = None, output_dir: str = "output") -> None:
    """Load Sub-Experiment 3 CSV data and render partition skew figures."""
    csv_path = resolve_csv_path(target_dir, CSV_FILENAME)
    print(f"[Exp3 Partition Skew] Loading data from: {csv_path}")

    df = pd.read_csv(csv_path)
    os.makedirs(output_dir, exist_ok=True)

    plt.rcParams.update({
        "font.family": "serif",
        "font.size": 10,
        "axes.grid": True,
        "grid.linestyle": "--",
        "grid.alpha": 0.6,
    })

    exp3 = df[df["Experiment_Type"] == "Exp3_Skew"] if "Experiment_Type" in df.columns else df
    if exp3.empty:
        print("[Exp3 Partition Skew] Warning: No rows found for Exp3_Skew.")
        return

    if "Sub_Batch" not in exp3.columns:
        # Older executions always used the default sub-batching
        exp3 = exp3.assign(Sub_Batch=SUB_BATCH_DEFAULT)

    stats3 = (
        exp3.groupby(["Partition_Mode", "Sub_Batch"], as_index=False)
        .agg(
            Total_Median=("Total_Latency_ms", "median"),
            Total_P95=("Total_Latency_ms", p95),
            S2_IO_Median=("Stage2_ISU_IO_ms", "median"),
        )
    )
    modes = ["Uniform", "ModerateSkew", "HotspotSkew"]
    mode_labels = ["Uniform\n(Balanced)", "Moderate Skew\n(50% in ISU-0)", "Hotspot Skew\n(80% in ISU-0)"]
    present_modes = [m for m in modes if m in set(stats3["Partition_Mode"])]
    variants = [v for v in (SUB_BATCH_DEFAULT, 0) if v in set(stats3["Sub_Batch"])]

    # (a) Stage 2 (ISU I/O) median per partition mode, with and without intra-ISU sub-batching.
    #     Variants are measured sequentially on the same data, so only Stage 2 is compared between them.
    # (b) End-to-end latency of the full system (sub-batching on): median bar, P95 whisker.
    fig, (ax_s2, ax_tot) = plt.subplots(1, 2, figsize=(7.0, 3.2))
    x = np.arange(len(present_modes))
    tick_labels = [mode_labels[modes.index(m)] for m in present_modes]
    width = 0.38 if len(variants) > 1 else 0.5
    variant_style = {SUB_BATCH_DEFAULT: ("#9467bd", "//", f"Sub-batch {SUB_BATCH_DEFAULT}"),
                     0: ("#c5b0d5", "..", "One call per ISU")}

    for v_idx, variant in enumerate(variants):
        sub = stats3[stats3["Sub_Batch"] == variant].set_index("Partition_Mode").reindex(present_modes)
        offset = (v_idx - (len(variants) - 1) / 2) * width
        color, hatch, label = variant_style[variant]
        ax_s2.bar(x + offset, sub["S2_IO_Median"].values, width, color=color, hatch=hatch,
                  edgecolor="black", label=label)
    ax_s2.set_xticks(x)
    ax_s2.set_xticklabels(tick_labels, fontsize=8)
    ax_s2.set_ylabel("Median Stage 2 ISU I/O (ms)")
    ax_s2.set_title("(a) Stage 2 per partitioning", fontsize=9)
    ax_s2.legend(fontsize=7, edgecolor="black")

    full = stats3[stats3["Sub_Batch"] == variants[0]].set_index("Partition_Mode").reindex(present_modes)
    total = full["Total_Median"].values
    ax_tot.bar(x, total, 0.5, color="#9467bd", edgecolor="black", alpha=0.85)
    ax_tot.errorbar(x, total, yerr=[np.zeros(len(total)), full["Total_P95"].values - total],
                    fmt="none", ecolor="black", capsize=3, lw=1)
    ax_tot.set_xticks(x)
    ax_tot.set_xticklabels(tick_labels, fontsize=8)
    ax_tot.set_ylabel("End-to-End Query Latency (ms)")
    name = f"sub-batch {variants[0]}" if variants[0] > 0 else "one call per ISU"
    ax_tot.set_title(f"(b) End-to-end, {name}\n(bar: median, whisker: P95)", fontsize=9)
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, "query_exp3_partition_skew.pdf"), bbox_inches="tight")
    plt.close(fig)

    print(f"[Exp3 Partition Skew] Figure saved in '{output_dir}/'.")


def main():
    parser = argparse.ArgumentParser(description="Generate Sub-Experiment 3 (Partition Skew) plots.")
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
    generate_exp3_plots(target_dir=target, output_dir=args.output)


if __name__ == "__main__":
    main()
