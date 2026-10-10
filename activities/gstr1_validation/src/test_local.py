"""
test_local.py — local test for GSTR1 validation (no S3, no Airflow)
Usage: python activities\\gstr1_validation\\src\\test_local.py --batch B1
"""
import argparse
import os
import sys

import pandas as pd

# Add repo root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from activities.gstr1_validation.src.transformation import run


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", default="B1", help="Batch ID: B1, B2, B3")
    args = parser.parse_args()

    batch = args.batch
    base  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    input_path = os.path.join(base, "data", "input", batch, f"gstr1_{batch}.xlsx")
    output_dir = os.path.join(base, "data", "output", batch)
    os.makedirs(output_dir, exist_ok=True)

    print(f"\n{'='*50}")
    print(f"GSTR1 VALIDATION — Batch {batch}")
    print(f"{'='*50}")
    print(f"Input: {input_path}")

    df = pd.read_excel(input_path, sheet_name="GSTR1 Data")
    print(f"Rows read: {len(df)}")

    result = run(df, batch_id=batch)

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
