"""
run_demo.py — Full pipeline demo, all 9 activities, multiple months
Per period:  S3 raw → DuckDB → S3 intermediate (parquet) → S3 final (Excel)
At the end:  one DuckDB query across all months of history

Run: python pipeline\\run_demo.py                      (all periods in sample data)
     python pipeline\\run_demo.py --periods 202506
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime

import pandas as pd

from pipeline import steps
from pipeline.create_sample_data import PERIODS
from pipeline.write_to_s3 import BUCKET

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)


def print_header(text):
    print(f"\n{'='*70}")
    print(f"  {text}")
    print(f"{'='*70}")


def run_period(period: str):
    print_header(f"PERIOD {period}")
    manifest = steps.read_manifest(period)
    batches  = [f"B{i}" for i in range(1, manifest["total_batches"] + 1)]
    print(f"  Manifest: {manifest['total_batches']} batches")

    print("\n-- GSTR1 track: per batch (DAG 1) --")
    for b in batches:
        v = steps.validate_batch(period, b)                # Activity 1
        f = steps.fix_batch(period, b)                     # Activity 2
        u = steps.create_upload_file(period, b)            # Activity 3
        steps.mark_batch_done(period, b)
        print(f"  {b}: {v['rows']} rows | {v['errors']} errors → {f['fixed']} fixed, "
              f"{f['still_errors']} open | upload sheets {u}")
    done, total = steps.batch_progress(period)
    print(f"  Batches done: {done}/{total}")

    print("\n-- GSTR1 track: combine + reco (DAG 2) --")
    print(f"  Combine: {steps.combine(period)}")                 # Activity 4
    print(f"  EINV reco: {steps.einv_reco(period)}")             # Activity 5
    print(f"  EWB reco : {steps.ewb_reco(period)}")              # Activity 6
    steps.gstr1_summary(period)

    print("\n-- PR / 2B track (DAG 3) --")
    print(f"  PR validation: {steps.pr_validation(period)}")     # Activity 7
    print(f"  2B consolidation: {steps.gstr2b_consolidation(period)}")  # Activity 8
    print(f"  2B vs PR reco: {steps.gstr2b_pr_reco(period)}")    # Activity 9
    print(f"  Summary: {steps.pr_2b_summary(period)}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--periods", nargs="+", default=list(PERIODS))
    args = parser.parse_args()

    print_header("SWIGGY GST AUTOMATION DEMO")
    print(f"Stack  : DuckDB + AWS S3 + Apache Airflow + Python")
    print(f"Bucket : s3://{BUCKET}")
    print(f"Periods: {', '.join(args.periods)}")

    for period in args.periods:
        run_period(period)

    print_header("HISTORY: one DuckDB query across all months in S3")
    print(steps.cross_month_analysis().to_string(index=False))

    print(f"\n  Outputs per month: s3://{BUCKET}/intermediate/<period>/ (parquet, Athena-ready)")
    print(f"                     s3://{BUCKET}/final/<period>/ (Excel for client)")
    print(f"\n  Demo complete at {datetime.now().strftime('%H:%M:%S')}\n")


if __name__ == "__main__":
    main()
