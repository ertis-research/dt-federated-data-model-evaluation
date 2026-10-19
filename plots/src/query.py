"""
Master plot orchestrator for all three Query Evaluation sub-experiments.
Delegates to the 3 individual sub-experiment plotting scripts:
  - query_exp1_scalability.py  (exp1_scalability.csv)
  - query_exp2_complexity.py   (exp2_complexity_concurrency.csv)
  - query_exp3_skew.py         (exp3_skew.csv)

Usage:
  python query.py                           # Uses latest execution in evaluation/query/output/
  python query.py 2026-10-19_16-00-00       # Uses specific timestamp subfolder
  python query.py --dir /path/to/run_folder # Uses explicit directory path
"""

import argparse
from query_exp1_scalability import generate_exp1_plots
from query_exp2_complexity import generate_exp2_plots
from query_exp3_skew import generate_exp3_plots


def main():
    parser = argparse.ArgumentParser(
        description="Generate plots for all available Query Evaluation sub-experiments."
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

    for name, fn in [
        ("Exp1 Scalability", generate_exp1_plots),
        ("Exp2 Complexity & Concurrency", generate_exp2_plots),
        ("Exp3 Partition Skew", generate_exp3_plots),
    ]:
        try:
            fn(target_dir=target, output_dir=args.output)
        except FileNotFoundError as exc:
            print(f"[{name}] Skipped ({exc})")


if __name__ == "__main__":
    main()