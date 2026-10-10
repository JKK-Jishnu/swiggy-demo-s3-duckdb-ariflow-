"""
=====================================================================================================
JOB      : GSTR-1 Error Fixing                                                    (Activity 2)
CLIENT   : Swiggy
RUNS     : Once per batch, after the first validation (Activity 1, stage "raw")
-----------------------------------------------------------------------------------------------------
WHAT IT DOES
  1. Takes the client's corrections file (one row per Record ID, only the fields they changed).
  2. Applies each correction to the ORIGINAL sales register line with the same Record ID.
     A blank cell in the corrections file means "keep the original value".
  3. Writes the corrected register, which Activity 1 then validates again (stage "corrected").
  4. Writes a log of every field that changed: Record ID, field, old value, new value.

  If the client has not sent a corrections file, the register is passed on unchanged.

ALTERYX EQUIVALENT
  Input Data (register) + Input Data (corrections) -> Join on Record ID
  -> Formula (take corrected value when not blank) -> Output Data (corrected register + change log)

INPUT
  raw/<period>/gstr1/<batch>/gstr1_<batch>.xlsx                      original sales register
  raw/<period>/gstr1/<batch>/corrections_<batch>.xlsx                client corrections (optional)
  intermediate/gstr1_validated/period=<period>/<batch>.parquet       first validation result

OUTPUT
  intermediate/gstr1_corrected/period=<period>/<batch>.parquet       corrected register, same headers as
                                                                     the original + 5 correction columns
  final/<period>/gstr1/<batch>_corrections_log.xlsx                  one row per changed field

RECORD ID
  Corrections are matched ONLY on Record ID (e.g. sales-202506-b2b-1). A correction whose Record ID
  does not exist in the register is reported in the log as "Record ID not found" and ignored.

HOW TO RUN
  python -m activities.gstr1_error_fixing.job --period 202506 --batch B1
  Add  --local-dir <folder>  to read/write the same paths in a local folder instead of S3.
=====================================================================================================
"""
import argparse
import io
import os

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError


# =====================================================================================================
# CONFIGURATION
# =====================================================================================================
RECORD_ID = "Record ID"

# Columns a client may correct (every register column except the Record ID itself)
CORRECTABLE_COLUMNS = [
    "Supplier GSTIN", "Document Type", "Invoice Type", "Invoice Number", "Invoice Date",
    "Receiver GSTIN", "Receiver Name", "Place of Supply", "Reverse Charge", "E-Commerce GSTIN",
    "HSN/SAC", "Item Description", "UQC", "Quantity", "GST Rate", "Taxable Value",
    "IGST Amount", "CGST Amount", "SGST Amount", "Cess Amount", "Total Invoice Value",
    "Original Invoice Number", "Original Invoice Date", "Export Type", "Port Code",
    "Shipping Bill Number", "Shipping Bill Date",
]


# =====================================================================================================
# STORAGE  (Input Data / Output Data tools)
# Reads and writes files in S3. With --local-dir the same paths are used inside a local folder.
# =====================================================================================================
class Storage:
    def __init__(self, local_dir: str = None):
        self.bucket    = os.environ.get("S3_BUCKET", "swiggy-idt-poc-bucket")
        self.local_dir = local_dir
        self.s3        = None if local_dir else boto3.client("s3", region_name=os.environ.get("AWS_DEFAULT_REGION", "ap-south-1"))

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

    def read_excel_as_text(self, key: str, sheet_name=0) -> pd.DataFrame:
        """Every cell is read as text (blank cell = "")."""
        return pd.read_excel(io.BytesIO(self.read_bytes(key)), sheet_name=sheet_name, dtype=str, na_filter=False)

    def read_parquet(self, key: str) -> pd.DataFrame:
        return pq.read_table(io.BytesIO(self.read_bytes(key))).to_pandas()

    def write_parquet(self, df: pd.DataFrame, key: str) -> str:
        buffer = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), buffer)
        return self.write_bytes(key, buffer.getvalue())

    def write_excel(self, sheets: dict, key: str) -> str:
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            for sheet_name, df in sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        return self.write_bytes(key, buffer.getvalue())


# =====================================================================================================
# THE JOB
# =====================================================================================================
def run(period: str, batch_id: str, local_dir: str = None) -> dict:
    store = Storage(local_dir)
    register_key    = f"raw/{period}/gstr1/{batch_id}/gstr1_{batch_id}.xlsx"
    corrections_key = f"raw/{period}/gstr1/{batch_id}/corrections_{batch_id}.xlsx"
    validated_key   = f"intermediate/gstr1_validated/period={period}/{batch_id}.parquet"
    output_key      = f"intermediate/gstr1_corrected/period={period}/{batch_id}.parquet"
    log_key         = f"final/{period}/gstr1/{batch_id}_corrections_log.xlsx"

    print(f"[GSTR1 error fixing] period={period} batch={batch_id}")

    # -------------------------------------------------------------------------------------------------
    # STEP 1 - INPUT: original register (text, exactly as uploaded) and the first validation result
    # -------------------------------------------------------------------------------------------------
    register  = store.read_excel_as_text(register_key, sheet_name="Sales Register")
    validated = store.read_parquet(validated_key)[["record_id", "validation_status", "validation_remark"]]
    print(f"  Register lines: {len(register)}")

    # -------------------------------------------------------------------------------------------------
    # STEP 2 - INPUT: client corrections (optional)
    # -------------------------------------------------------------------------------------------------
    if store.exists(corrections_key):
        corrections = store.read_excel_as_text(corrections_key, sheet_name="Corrections")
        print(f"  Corrections received: {len(corrections)}")
    else:
        corrections = pd.DataFrame(columns=[RECORD_ID] + CORRECTABLE_COLUMNS + ["Correction Remark"])
        print("  No corrections file received - register passed on unchanged")

    # -------------------------------------------------------------------------------------------------
    # STEP 3 - APPLY: for each correction row, copy every non-blank cell onto the register line
    #          with the same Record ID, and log what changed.
    # -------------------------------------------------------------------------------------------------
    register = register.set_index(RECORD_ID, drop=False)
    register["Correction Applied"] = "N"
    register["Corrected Fields"]   = ""
    register["Correction Remark"]  = ""
    change_log = []

    for _, correction in corrections.iterrows():
        record_id = correction[RECORD_ID].strip()

        if record_id not in register.index:
            change_log.append({"Record ID": record_id, "Field": "", "Old Value": "", "New Value": "",
                               "Status": "Record ID not found - correction ignored"})
            continue

        changed_fields = []
        for field in CORRECTABLE_COLUMNS:
            new_value = str(correction.get(field, "")).strip()
            old_value = register.at[record_id, field]
            if new_value == "" or new_value == old_value:
                continue                                    # blank or same value = nothing to change
            register.at[record_id, field] = new_value
            changed_fields.append(field)
            change_log.append({"Record ID": record_id, "Field": field, "Old Value": old_value,
                               "New Value": new_value, "Status": "Applied"})

        if changed_fields:
            register.at[record_id, "Correction Applied"] = "Y"
            register.at[record_id, "Corrected Fields"]   = ", ".join(changed_fields)
            register.at[record_id, "Correction Remark"]  = str(correction.get("Correction Remark", "")).strip()

    # -------------------------------------------------------------------------------------------------
    # STEP 4 - JOIN: carry the first validation result along, so the final data shows
    #          "Error before correction -> Success after correction"
    # -------------------------------------------------------------------------------------------------
    first_pass = validated.set_index("record_id")
    register["First Pass Status"] = register[RECORD_ID].map(first_pass["validation_status"]).fillna("")
    register["First Pass Remark"] = register[RECORD_ID].map(first_pass["validation_remark"]).fillna("")
    register = register.reset_index(drop=True)

    # -------------------------------------------------------------------------------------------------
    # STEP 5 - OUTPUT: corrected register (for re-validation) and the change log (for audit)
    # -------------------------------------------------------------------------------------------------
    store.write_parquet(register.astype(str), output_key)
    log_df = pd.DataFrame(change_log, columns=["Record ID", "Field", "Old Value", "New Value", "Status"])
    store.write_excel({"Corrections Log": log_df}, log_key)

    summary = {"corrections_received": len(corrections),
               "lines_corrected": int((register["Correction Applied"] == "Y").sum()),
               "fields_changed": int((log_df["Status"] == "Applied").sum()),
               "unknown_record_ids": int((log_df["Status"] != "Applied").sum())}
    print(f"  Lines corrected: {summary['lines_corrected']} ({summary['fields_changed']} fields changed)")
    if summary["unknown_record_ids"]:
        print(f"  WARNING: {summary['unknown_record_ids']} corrections refer to unknown Record IDs")
    print(f"  Output: {output_key}")
    print(f"          {log_key}")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Apply client corrections to one GSTR-1 batch")
    parser.add_argument("--period", required=True, help="return period YYYYMM, e.g. 202506")
    parser.add_argument("--batch", required=True, help="batch id, e.g. B1")
    parser.add_argument("--local-dir", help="use this local folder instead of S3")
    args = parser.parse_args()
    run(args.period, args.batch, args.local_dir)
