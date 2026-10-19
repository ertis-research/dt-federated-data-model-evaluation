"""
Plot generator for Sub-Experiment 3: Partition Skew Analysis (Uniform vs Moderate vs Hotspot).

Generates:
  - output/query_partition_skew.pdf

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
    fig.savefig(os.path.join(output_dir, "query_partition_skew.pdf"), bbox_inches="tight")
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
