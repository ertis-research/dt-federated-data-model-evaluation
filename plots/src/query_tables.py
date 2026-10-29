"""
LaTeX table generator for the Query Evaluation sub-experiments.

Generates:
  - output/query_tables.tex
      Table A (Exp1): end-to-end mean (P95) latency per device count and architecture (full system)
      Table B (Exp2): end-to-end mean (P95) latency per query profile and concurrency level
      Table C (Exp3): state retrieval mean per partitioning, with split requests and with one request per ISU

Usage:
  python query_tables.py                           # Uses latest execution in evaluation/query/output/
  python query_tables.py 2026-10-19_16-00-00       # Uses specific timestamp subfolder
  python query_tables.py --dir /path/to/run_folder # Uses explicit directory path
"""

import argparse
import os

import numpy as np
import pandas as pd

from query_exp1_scalability import SUB_BATCH_DEFAULT, add_sub_batch_column, resolve_csv_path


def p95(x):
    return np.percentile(x, 95)


def fmt_cell(mean: float, tail: float) -> str:
    return f"{mean:.0f} ({tail:.0f})"


def try_load(target_dir: str | None, csv_filename: str) -> pd.DataFrame | None:
    try:
        return add_sub_batch_column(pd.read_csv(resolve_csv_path(target_dir, csv_filename)))
    except FileNotFoundError as exc:
        print(f"[Query Tables] Skipped {csv_filename} ({exc})")
        return None


def table_exp1(df: pd.DataFrame) -> str:
    full = df[(df["Num_ISUs"] == 0) | (df["Sub_Batch"] > 0)]
    stats = (
        full.groupby(["Num_Sensors", "Num_ISUs"])["Total_Latency_ms"]
        .agg(["mean", p95])
        .reset_index()
    )
    isus = sorted(stats["Num_ISUs"].unique())
    header = " & ".join(["Devices"] + ["Centralized" if n == 0 else f"Fed. {n} ISU" + ("s" if n > 1 else "") for n in isus])
    lines = [
        "\\begin{table}[t]",
        "\\centering",
        "\\caption{End-to-end query latency of \\texttt{Q3\\_Inference} ($C=1$), mean (P95) in ms over 30 runs. Federated configurations use intra-ISU sub-batching.}",
        "\\label{tab:query-exp1}",
        "\\begin{tabular}{" + "r" * (len(isus) + 1) + "}",
        "\\toprule",
        header + " \\\\",
        "\\midrule",
    ]
    for count in sorted(stats["Num_Sensors"].unique()):
        row = stats[stats["Num_Sensors"] == count].set_index("Num_ISUs")
        cells = [fmt_cell(row.loc[n, "mean"], row.loc[n, "p95"]) if n in row.index else "--" for n in isus]
        lines.append(" & ".join([f"{count:,}"] + cells) + " \\\\")
    lines += ["\\bottomrule", "\\end{tabular}", "\\end{table}"]
    return "\n".join(lines)


def table_exp2(df: pd.DataFrame) -> str:
    stats = (
        df.assign(Arch=np.where(df["Num_ISUs"] == 0, "Cen", "Fed"))
        .groupby(["Query_Type", "Concurrency", "Arch"])["Total_Latency_ms"]
        .agg(["mean", p95])
        .reset_index()
    )
    lines = [
        "\\begin{table}[t]",
        "\\centering",
        "\\caption{End-to-end query latency at 5{,}000 devices, mean (P95) in ms over 30 bursts of $C$ simultaneous queries. $\\Delta$ is the change of the Federated (8 ISUs) mean relative to Centralized.}",
        "\\label{tab:query-exp2}",
        "\\begin{tabular}{llrrr}",
        "\\toprule",
        "Query & $C$ & Centralized & Federated & $\\Delta$ \\\\",
        "\\midrule",
    ]
    q_labels = {"Q1_Selective": "Q1", "Q2_Filtering": "Q2", "Q3_Inference": "Q3"}
    for q_idx, q in enumerate(sorted(stats["Query_Type"].unique())):
        if q_idx > 0:
            lines.append("\\midrule")
        for conc in sorted(stats["Concurrency"].unique()):
            sub = stats[(stats["Query_Type"] == q) & (stats["Concurrency"] == conc)].set_index("Arch")
            if not {"Cen", "Fed"} <= set(sub.index):
                continue
            cen, fed = sub.loc["Cen"], sub.loc["Fed"]
            delta = (fed["mean"] - cen["mean"]) / cen["mean"] * 100.0
            lines.append(
                f"{q_labels.get(q, q)} & {conc} & {fmt_cell(cen['mean'], cen['p95'])} & "
                f"{fmt_cell(fed['mean'], fed['p95'])} & {delta:+.0f}\\% \\\\"
            )
    lines += ["\\bottomrule", "\\end{tabular}", "\\end{table}"]
    return "\n".join(lines)


def table_exp3(df: pd.DataFrame) -> str:
    modes = [m for m in ["Uniform", "ModerateSkew", "HotspotSkew"] if m in set(df["Partition_Mode"])]
    s2 = df.groupby(["Partition_Mode", "Sub_Batch"])["Stage2_ISU_IO_ms"].mean()
    has_off = 0 in set(df["Sub_Batch"])
    # Only the state retrieval is compared: the two variants are measured sequentially, so their
    # end-to-end latencies are not comparable
    lines = [
        "\\begin{table}[t]",
        "\\centering",
        "\\caption{Mean state retrieval time (ms) under partition skew at 5{,}000 devices and 8 ISUs ($Q_3$, $C=1$), "
        "when the requests to each ISU are split and when a single request is sent per ISU.}",
        "\\label{tab:query-exp3}",
        "\\begin{tabular}{lrr}",
        "\\toprule",
        "Partitioning & \\makecell{Split\\\\requests} & \\makecell{One request\\\\per ISU} \\\\",
        "\\midrule",
    ]
    labels = {"Uniform": "Uniform", "ModerateSkew": "Moderate (50\\% on ISU 0)", "HotspotSkew": "Hotspot (80\\% on ISU 0)"}
    for m in modes:
        on = s2.get((m, SUB_BATCH_DEFAULT), float("nan"))
        off = s2.get((m, 0), float("nan")) if has_off else float("nan")
        off_cell = f"{off:.1f}" if not np.isnan(off) else "--"
        lines.append(f"{labels[m]} & {on:.1f} & {off_cell} \\\\")
    lines += ["\\bottomrule", "\\end{tabular}", "\\end{table}"]
    return "\n".join(lines)


def generate_query_tables(target_dir: str | None = None, output_dir: str = "output") -> None:
    """Load the query CSVs and write the LaTeX tables (booktabs) to output/query_tables.tex."""
    sections = []
    os.makedirs(output_dir, exist_ok=True)
    for exp_id, csv_name, builder in [
        (1, "exp1_scalability.csv", table_exp1),
        (2, "exp2_complexity_concurrency.csv", table_exp2),
        (3, "exp3_skew.csv", table_exp3),
    ]:
        df = try_load(target_dir, csv_name)
        if df is not None and not df.empty:
            table = builder(df)
            sections.append(table)
            # One file per table, so each can be included on its own with \input
            with open(os.path.join(output_dir, f"query_table_exp{exp_id}.tex"), "w", encoding="utf-8") as f:
                f.write("% Requires \\usepackage{booktabs} and \\usepackage{makecell}\n" + table + "\n")
    if not sections:
        return
    out_path = os.path.join(output_dir, "query_tables.tex")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("% Requires \\usepackage{booktabs} and \\usepackage{makecell}\n\n" + "\n\n".join(sections) + "\n")
    print(f"[Query Tables] LaTeX tables saved in '{out_path}' (and one query_table_expN.tex per table).")


def main():
    parser = argparse.ArgumentParser(description="Generate LaTeX tables for the Query Evaluation sub-experiments.")
    parser.add_argument("run_folder", nargs="?", default=None,
                        help="Optional timestamp folder name or path (defaults to latest execution).")
    parser.add_argument("-d", "--dir", dest="dir_flag", default=None,
                        help="Optional timestamp folder name or path (defaults to latest execution).")
    parser.add_argument("-o", "--output", default="output",
                        help="Output directory for the generated .tex file (default: output).")
    args = parser.parse_args()
    generate_query_tables(target_dir=args.dir_flag or args.run_folder, output_dir=args.output)


if __name__ == "__main__":
    main()
