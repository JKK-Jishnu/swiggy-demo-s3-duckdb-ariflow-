"""
run_demo.py — Full pipeline demo
Swiggy GSTR1: S3 read → DuckDB validate → S3 write parquet → combine → summary

Run: python pipeline\\run_demo.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime

import duckdb
import pandas as pd

from activities.gstr1_validation.src.transformation import run as validate
from pipeline.write_to_s3 import BUCKET, read_excel, s3_client, write_parquet

PERIOD  = "202506"
BATCHES = ["B1", "B2", "B3"]


def print_header(text):
    print(f"\n{'='*60}")
    print(f"  {text}")
    print(f"{'='*60}")


def main():
    print_header("SWIGGY GST AUTOMATION DEMO")
    print(f"Stack : DuckDB + AWS S3 + Apache Airflow + Python")
    print(f"Client: Swiggy | Period: {PERIOD}")
    print(f"Bucket: s3://{BUCKET}")

    s3 = s3_client()
    all_results = []

    # STEP 1: Process each batch
    print_header("STEP 1: GSTR1 Validation (per batch)")

    for batch in BATCHES:
        print(f"\n  Processing {batch}...")

        # Read from S3
        key = f"raw/{PERIOD}/gstr1/{batch}/gstr1_{batch}.xlsx"
        print(f"  Reading: s3://{BUCKET}/{key}")
        df = read_excel(key, s3=s3)
        print(f"  Rows fetched: {len(df)}")

        # DuckDB validation
        result = validate(df, batch_id=batch)
        all_results.append(result["result"])
        print(f"  DuckDB result: {len(result['success'])} success, {len(result['errors'])} errors")

        # Write parquet to S3 intermediate
        path = write_parquet(result["result"], f"intermediate/{PERIOD}/gstr1/{batch}_validated.parquet", s3=s3)
        print(f"  Written parquet: {path}")

        # Mark batch done
        done_key = f"batch_status/{PERIOD}/{batch}.done"
        s3.put_object(Bucket=BUCKET, Key=done_key, Body=b"done")
        print(f"  Marked done: {done_key}")

    # STEP 2: Combine batches
    print_header("STEP 2: Combine All Batches")
    combined = pd.concat(all_results, ignore_index=True)
    print(f"  Combined rows: {len(combined)}")
    print(f"  Batches included: {combined['batch_id'].unique().tolist()}")

    path = write_parquet(combined, f"intermediate/{PERIOD}/combined_sales.parquet", s3=s3)
    print(f"  Written: {path}")

    # STEP 3: DuckDB query on combined data
    print_header("STEP 3: DuckDB Analysis on Combined Data")
    con = duckdb.connect()
    con.register("combined", combined)

    analysis = con.execute("""
        SELECT
            batch_id,
            COUNT(*) as total_rows,
            CAST(COUNT_IF(validation_status = 'Success') AS INTEGER) as success_count,
            CAST(COUNT_IF(validation_status = 'Error') AS INTEGER)   as error_count,
            ROUND(SUM(CASE WHEN validation_status = 'Success' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as success_rate_pct,
            ROUND(SUM(taxable_value), 2) as total_taxable_value
        FROM combined
        GROUP BY batch_id
        ORDER BY batch_id
    """).df()

    print("\n  Batch-wise summary:")
    print(analysis.to_string(index=False))

    totals = con.execute("""
        SELECT
            COUNT(*) as total_rows,
            CAST(COUNT_IF(validation_status = 'Success') AS INTEGER) as total_success,
            CAST(COUNT_IF(validation_status = 'Error') AS INTEGER)   as total_errors,
            ROUND(SUM(taxable_value), 2) as total_taxable_value
        FROM combined
    """).df()

    # STEP 4: Final summary
    print_header("DEMO COMPLETE — FINAL SUMMARY")
    row = totals.iloc[0]
    print(f"  Total rows processed  : {int(row['total_rows'])}")
    print(f"  Success               : {int(row['total_success'])}")
    print(f"  Errors                : {int(row['total_errors'])}")
    print(f"  Total taxable value   : Rs.{row['total_taxable_value']:,.2f}")
    print(f"\n  S3 outputs written:")
    for batch in BATCHES:
        print(f"    intermediate/{PERIOD}/gstr1/{batch}_validated.parquet")
    print(f"    intermediate/{PERIOD}/combined_sales.parquet")
    print(f"\n  All data queryable via AWS Athena anytime")
    print(f"\n  {'='*60}")
    print(f"  Demo complete at {datetime.now().strftime('%H:%M:%S')}")
    print(f"  {'='*60}\n")


if __name__ == "__main__":
    main()
