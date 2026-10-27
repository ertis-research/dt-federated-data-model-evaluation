"""
Plot generator for Multi-ISU & Ablation Ingestion Evaluation.

Generates (all in linear scale):
  - output/ingestion_latency_mean.pdf     (Mean Ingestion Latency - linear scale)
  - output/ingestion_latency_p99.pdf      (P99 Tail Latency - linear scale)
  - output/ingestion_latency.pdf          (2-panel Mean & P99 Latency side-by-side - linear scale)
  - output/ingestion_throughput.pdf       (Throughput msg/sec)
  - output/ingestion_cpu.pdf              (Total DB CPU Usage)
  - output/ingestion_ram.pdf              (Total DB Memory Usage)
  - output/ingestion_resources.pdf        (Combined CPU & RAM Usage in a 2-panel figure)

Usage:
  python ingestion.py                           # Uses latest execution in evaluation/ingestion/output/
  python ingestion.py 2026-10-26_14-00-00       # Uses specific timestamp subfolder
  python ingestion.py --dir /path/to/run_folder # Uses explicit directory path
"""

import argparse
import os
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

CSV_FILENAME = "ingestion_evaluation_results.csv"


def resolve_csv_path(target_dir: str | None, csv_filename: str = CSV_FILENAME) -> Path:
    """Resolve the path to the ingestion CSV from a user-specified directory or the latest execution."""
    repo_root = Path(__file__).resolve().parents[2]
    base_output_dir = repo_root / "evaluation" / "ingestion" / "output"

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


def generate_ingestion_plots(target_dir: str | None = None, output_dir: str = "output") -> None:
    """Load ingestion evaluation CSV data and render performance & resource figures."""
    csv_path = resolve_csv_path(target_dir, CSV_FILENAME)
    print(f"[Ingestion Evaluation] Loading data from: {csv_path}")

    df = pd.read_csv(csv_path)
    # Older executions named the undelivered-message counter "Packet_Loss_Network"
    df = df.rename(columns={"Packet_Loss_Network": "Undelivered_Msgs"})
    os.makedirs(output_dir, exist_ok=True)

    df["Scenario_Label"] = (
        df["Num_Sensors"].astype(str) + "@" + df["Freq_Hz"].astype(str) + "Hz"
    )

    # IEEE-compliant plotting style
    plt.rcParams.update({
        "font.family": "serif",
        "font.size": 10,
        "axes.grid": True,
        "grid.linestyle": "--",
        "grid.alpha": 0.6,
    })

    has_multi_isu = "P99_Latency_ms" in df.columns

    # A failed Prometheus query is written as 0; treat it as missing so it does not bias the means
    for col in ("DB_CPU_Avg_Cores", "DB_RAM_Max_MB"):
        if col in df.columns:
            df[col] = df[col].mask(df[col] <= 0)

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
                PacketLoss_Mean=("Undelivered_Msgs", "mean"),
                Throughput_Mean=("Throughput_msg_sec", "mean"),
                Throughput_Std=("Throughput_msg_sec", "std"),
                CPU_Mean=("DB_CPU_Avg_Cores", "median"),  # robust to partial Prometheus windows
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
            fig.savefig(os.path.join(output_dir, f"{filename}.pdf"), bbox_inches="tight")
            plt.close(fig)

        def plot_linear_latency(ax, metric_col, ylabel, y_cap, show_legend=True, title=None):
            """Plot latency on a linear axis, capping extreme saturation outliers with callout labels."""
            handles, legend_labels = [], []
            off_scale_points = []

            for arch_key, arch_label, marker, ls in arch_order:
                sub = df_stats[df_stats["Architecture"] == arch_key].sort_values("Step_ID")
                if sub.empty:
                    continue
                x_sub = np.arange(len(sub))
                y_vals = sub[metric_col].to_numpy(dtype=float)

                (line,) = ax.plot(
                    x_sub,
                    np.minimum(y_vals, y_cap * 1.03),
                    marker=marker,
                    linestyle=ls,
                    linewidth=1.5,
                    markersize=5,
                    label=arch_label,
                )
                handles.append(line)
                legend_labels.append(arch_label)

                for idx, val in enumerate(y_vals):
                    if val > y_cap:
                        off_scale_points.append((idx, val, arch_label, line.get_color()))

            ax.set_ylim(0, y_cap)
            if title:
                ax.set_title(title, fontsize=10)
            ax.set_xlabel("Scenario (Devices @ Frequency)")
            ax.set_ylabel(ylabel)
            ax.set_xticks(x)
            ax.set_xticklabels(labels, rotation=30, ha="right")

            # Annotate off-scale saturation points cleanly near the top border
            off_scale_points.sort(key=lambda item: item[1], reverse=True)
            for rank, (x_idx, val, arch_label, color) in enumerate(off_scale_points):
                short_name = arch_label.replace("Centralized ", "Cent. ").replace("Federated ", "Fed. ")
                val_sec = val / 1000.0
                text_label = f"{short_name}: {val_sec:.1f}s" if val >= 1000 else f"{short_name}: {val:.0f}ms"
                y_pos = y_cap * (0.91 - rank * 0.11)
                ax.annotate(
                    text_label,
                    xy=(x_idx, y_cap * 0.99),
                    xytext=(x_idx - 1.55, y_pos),
                    fontsize=7.2,
                    fontweight="bold",
                    color=color,
                    bbox=dict(boxstyle="round,pad=0.2", fc="white", ec=color, alpha=0.9, lw=0.8),
                    arrowprops=dict(arrowstyle="->", color=color, lw=1.0),
                )

            if show_legend:
                ax.legend(fontsize=7.8, loc="upper left", framealpha=0.9, edgecolor="black")

            return handles, legend_labels

        # 1a. Standalone Mean Ingestion Latency (Linear scale, 0-88 ms operational window)
        fig, ax = plt.subplots(figsize=(4.8, 3.4))
        plot_linear_latency(ax, "Latency_Mean", "Mean Ingestion Latency (ms)", y_cap=88.0, show_legend=True)
        fig.tight_layout()
        fig.savefig(os.path.join(output_dir, "ingestion_latency_mean.pdf"), bbox_inches="tight")
        plt.close(fig)

        # 1b. Standalone P99 Tail Latency (Linear scale, 0-860 ms operational window)
        fig, ax = plt.subplots(figsize=(4.8, 3.4))
        plot_linear_latency(ax, "P99_Mean", "P99 Tail Latency (ms)", y_cap=860.0, show_legend=True)
        fig.tight_layout()
        fig.savefig(os.path.join(output_dir, "ingestion_latency_p99.pdf"), bbox_inches="tight")
        plt.close(fig)

        # 1c. Combined 2-Panel Linear Latency Figure: (a) Mean Latency & (b) P99 Tail Latency
        fig, (ax_mean, ax_p99) = plt.subplots(1, 2, figsize=(8.6, 3.4))
        handles, legend_labels = plot_linear_latency(
            ax_mean, "Latency_Mean", "Mean Ingestion Latency (ms)", y_cap=88.0, show_legend=False, title="(a) Mean Ingestion Latency"
        )
        plot_linear_latency(
            ax_p99, "P99_Mean", "P99 Tail Latency (ms)", y_cap=860.0, show_legend=False, title="(b) P99 Tail Latency"
        )
        fig.legend(
            handles,
            legend_labels,
            loc="upper center",
            bbox_to_anchor=(0.5, 1.08),
            ncol=3,
            fontsize=8,
            framealpha=0.85,
            edgecolor="black",
        )
        fig.tight_layout()
        fig.savefig(os.path.join(output_dir, "ingestion_latency.pdf"), bbox_inches="tight")
        plt.close(fig)

        # 2. Throughput
        plot_multi_series("Throughput_Mean", "Throughput (msg/sec)", "ingestion_throughput", log_scale=False)

        # 3. CPU Usage (individual)
        plot_multi_series("CPU_Mean", "Total DB CPU Usage (Cores)", "ingestion_cpu", log_scale=False)

        # 4. RAM Usage (individual)
        plot_multi_series("RAM_Mean", "Total DB Memory (MB)", "ingestion_ram", log_scale=False)

        # 5. Combined Resource Consumption (CPU + RAM in a 2-panel figure with shared legend)
        fig, (ax_cpu, ax_ram) = plt.subplots(1, 2, figsize=(8.6, 3.4))
        handles, legend_labels = [], []

        for arch_key, arch_label, marker, ls in arch_order:
            sub = df_stats[df_stats["Architecture"] == arch_key].sort_values("Step_ID")
            if sub.empty:
                continue
            x_sub = np.arange(len(sub))
            (l_cpu,) = ax_cpu.plot(
                x_sub,
                sub["CPU_Mean"],
                marker=marker,
                linestyle=ls,
                label=arch_label,
                linewidth=1.5,
                markersize=5,
            )
            ax_ram.plot(
                x_sub,
                sub["RAM_Mean"],
                marker=marker,
                linestyle=ls,
                label=arch_label,
                linewidth=1.5,
                markersize=5,
            )
            handles.append(l_cpu)
            legend_labels.append(arch_label)

        ax_cpu.set_title("(a) CPU Consumption", fontsize=10)
        ax_cpu.set_xlabel("Scenario (Devices @ Frequency)")
        ax_cpu.set_ylabel("Total DB CPU Usage (Cores)")
        ax_cpu.set_xticks(x)
        ax_cpu.set_xticklabels(labels, rotation=30, ha="right")

        ax_ram.set_title("(b) Memory Consumption", fontsize=10)
        ax_ram.set_xlabel("Scenario (Devices @ Frequency)")
        ax_ram.set_ylabel("Total DB Memory (MB)")
        ax_ram.set_xticks(x)
        ax_ram.set_xticklabels(labels, rotation=30, ha="right")

        fig.legend(
            handles,
            legend_labels,
            loc="upper center",
            bbox_to_anchor=(0.5, 1.08),
            ncol=3,
            fontsize=8,
            framealpha=0.85,
            edgecolor="black",
        )
        fig.tight_layout()
        fig.savefig(os.path.join(output_dir, "ingestion_resources.pdf"), bbox_inches="tight")
        plt.close(fig)

        print(f"[Ingestion Evaluation] All figures saved in '{output_dir}/'.")
    else:
        print("Legacy CSV detected. Run the new ingestion benchmark to generate v2 results.")


def main():
    parser = argparse.ArgumentParser(
        description="Generate Multi-ISU & Ablation Ingestion Evaluation plots."
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
    generate_ingestion_plots(target_dir=target, output_dir=args.output)


if __name__ == "__main__":
    main()
