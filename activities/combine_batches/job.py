"""
=====================================================================================================
JOB      : Combine Batches                                                        (Activity 4)
CLIENT   : Swiggy
RUNS     : Once per month, when every batch listed in the manifest has finished Activities 1-3
-----------------------------------------------------------------------------------------------------
WHAT IT DOES
  Stacks the validated lines of all batches (B1 ... Bn) of the month into one sales dataset,
  which the reconciliations (Activities 5 and 6) use.
  Before combining it checks that EVERY batch in the manifest is present - a missing batch
  stops the job, so a month is never reconciled on partial data.
  It also flags lines that appear twice across batches:
    is_duplicate_record_id  the same Record ID in more than one line
    is_duplicate_invoice    the same document type + invoice number in more than one line

STEPS
  1  Read the manifest: which batches should exist this month
  2  Stop if any batch has not finished validation
  3  Stack all batches into one table
  4  Add the duplicate flags
  5  Write the output

INPUT
  raw/<period>/gstr1/manifest.json                                   {"total_batches": n, "batches": [...]}
  intermediate/gstr1_final/period=<period>/<batch>.parquet           one per batch (from Activity 1)

OUTPUT
  intermediate/sales_combined/period=<period>/sales_combined.parquet (Athena table sales_combined)

RECORD ID
  Unchanged - every line keeps the Record ID it arrived with.

HOW TO RUN
  python -m activities.combine_batches.job --period 202506
  Add  --local-dir <folder>  to read/write the same paths in a local folder instead of S3.
=====================================================================================================
"""
import argparse
import io
import json
import os

import boto3
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError


# =====================================================================================================
# STORAGE - read and write files in S3, or in a local folder when --local-dir is given
# =====================================================================================================
class Storage:
    def __init__(self, local_dir: str = None):
        self.bucket    = os.environ.get("S3_BUCKET", "swiggy-idt-poc-bucket")
        self.local_dir = local_dir
        if local_dir:
            self.s3 = None
        else:
            self.s3 = boto3.client("s3", region_name=os.environ.get("AWS_DEFAULT_REGION", "ap-south-1"))

    def exists(self, key: str) -> bool:
        if self.local_dir:
            return os.path.exists(os.path.join(self.local_dir, key))
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    def read_bytes(self, key: str) -> bytes:
        if self.local_dir:
            with open(os.path.join(self.local_dir, key), "rb") as f:
                return f.read()
        return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def write_bytes(self, key: str, data: bytes) -> str:
        if self.local_dir:
            path = os.path.join(self.local_dir, key)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(data)
            return path
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=data)
        return f"s3://{self.bucket}/{key}"

    def read_parquet(self, key: str) -> pa.Table:
        return pq.read_table(io.BytesIO(self.read_bytes(key)))

    def write_parquet(self, table: pa.Table, key: str) -> str:
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        return self.write_bytes(key, buffer.getvalue())


# =====================================================================================================
# THE JOB
# =====================================================================================================
def run(period: str, local_dir: str = None) -> dict:
    store = Storage(local_dir)
    manifest_key = f"raw/{period}/gstr1/manifest.json"
    output_key   = f"intermediate/sales_combined/period={period}/sales_combined.parquet"

    print(f"[Combine batches] period={period}")

    # -------------------------------------------------------------------------------------------------
    # STEP 1 - Read the manifest: which batches should exist this month?
    # -------------------------------------------------------------------------------------------------
    manifest = json.loads(store.read_bytes(manifest_key))
    batches  = manifest.get("batches") or [f"B{n}" for n in range(1, manifest["total_batches"] + 1)]
    print(f"  Manifest: {len(batches)} batches {batches}")

    # -------------------------------------------------------------------------------------------------
    # STEP 2 - Every batch must have finished validation; otherwise stop
    # -------------------------------------------------------------------------------------------------
    batch_keys = {b: f"intermediate/gstr1_final/period={period}/{b}.parquet" for b in batches}
    missing    = [b for b, key in batch_keys.items() if not store.exists(key)]
    if missing:
        raise RuntimeError(f"Cannot combine {period}: batches not processed yet: {missing}")

    # -------------------------------------------------------------------------------------------------
    # STEP 3 - Stack all batches (they all have the same columns)
    # -------------------------------------------------------------------------------------------------
    tables = [store.read_parquet(key) for key in batch_keys.values()]
    con = duckdb.connect()
    con.register("all_lines", pa.concat_tables(tables))

    # -------------------------------------------------------------------------------------------------
    # STEP 4 - Duplicate flags (count how often each Record ID / invoice appears)
    # -------------------------------------------------------------------------------------------------
    combined = con.execute("""
        SELECT *,
            count(*) OVER (PARTITION BY record_id) > 1                       AS is_duplicate_record_id,
            count(*) OVER (PARTITION BY document_type, invoice_number) > 1   AS is_duplicate_invoice
        FROM all_lines
        ORDER BY batch_id, source_row_number
    """).arrow()

    summary_rows = con.execute("""
        SELECT batch_id,
               count(*)                                   AS lines,
               count_if(first_pass_status = 'Error')      AS errors_found,
               count_if(correction_applied)               AS corrected,
               count_if(validation_status = 'Error')      AS still_in_error
        FROM all_lines GROUP BY batch_id ORDER BY batch_id
    """).fetchall()
    duplicates = con.execute("""
        SELECT count(*) FROM (SELECT record_id FROM all_lines GROUP BY record_id HAVING count(*) > 1)
    """).fetchone()[0]
    con.close()

    # -------------------------------------------------------------------------------------------------
    # STEP 5 - Write the output
    # -------------------------------------------------------------------------------------------------
    store.write_parquet(combined, output_key)

    print("  batch  lines  errors_found  corrected  still_in_error")
    for batch, lines, found, corrected, still in summary_rows:
        print(f"  {batch:<6} {lines:>5} {found:>13} {corrected:>10} {still:>15}")
    print(f"  Combined lines: {combined.num_rows} | duplicate Record IDs: {duplicates}")
    print(f"  Output: {output_key}")
    return {"batches": len(batches), "lines": combined.num_rows, "duplicate_record_ids": int(duplicates)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Combine all GSTR-1 batches of a month")
    parser.add_argument("--period", required=True, help="return period YYYYMM, e.g. 202506")
    parser.add_argument("--local-dir", help="use this local folder instead of S3")
    args = parser.parse_args()
    run(args.period, args.local_dir)
