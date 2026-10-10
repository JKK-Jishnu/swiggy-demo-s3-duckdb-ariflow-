"""
test_local.py — local test for GSTR1 validation (no S3, no Airflow)
Usage: python activities\\gstr1_validation\\src\\test_local.py --batch B1
"""
import argparse
import os
import sys

import pandas as pd

# Add repo root to path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, REPO_ROOT)

from activities.gstr1_validation.src.transformation import run


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--period", default="202506", help="Period YYYYMM: 202504, 202505, 202506")
    parser.add_argument("--batch", default="B1", help="Batch ID: B1, B2, ...")
    args = parser.parse_args()

    batch, period = args.batch, args.period

    input_path = os.path.join(REPO_ROOT, "sample_data", "raw", period, "gstr1", batch, f"gstr1_{batch}.xlsx")
    output_dir = os.path.join(REPO_ROOT, "output", period)
    os.makedirs(output_dir, exist_ok=True)

    print(f"\n{'='*50}")
    print(f"GSTR1 VALIDATION — {period} Batch {batch}")
    print(f"{'='*50}")
    print(f"Input: {input_path}")

    df = pd.read_excel(input_path, sheet_name="GSTR1 Data")
    print(f"Rows read: {len(df)}")

    result = run(df, batch_id=batch, period=period)

    # Write Excel output
    out_path = os.path.join(output_dir, f"gstr1_validated_{batch}.xlsx")
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        result["result"].to_excel(writer,  sheet_name="All Results", index=False)
        result["success"].to_excel(writer, sheet_name="Success",     index=False)
        result["errors"].to_excel(writer,  sheet_name="Errors",      index=False)

    total   = len(result["result"])
    success = len(result["success"])
    errors  = len(result["errors"])

    print(f"\nRESULT SUMMARY")
    print(f"  Total rows  : {total}")
    print(f"  Success     : {success}")
    print(f"  Errors      : {errors}")
    print(f"  Success rate: {success/total*100:.1f}%")
    if errors:
        print(f"\nError rows:")
        for _, r in result["errors"].iterrows():
            print(f"  {r['invoice_number']:<20} {r['validation_remark']}")
    print(f"\nOutput: {out_path}")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    main()
