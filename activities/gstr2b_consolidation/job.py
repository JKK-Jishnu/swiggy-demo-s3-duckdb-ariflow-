"""
=====================================================================================================
JOB      : GSTR-2B Consolidation                                                  (Activity 8)
CLIENT   : Swiggy
RUNS     : Once per month (PR / GSTR-2B track). Runs in parallel with Activity 7.
-----------------------------------------------------------------------------------------------------
WHAT IT DOES
  GSTR-2B is downloaded through the Bolton tool as one Excel workbook per Swiggy GSTIN
  (2b_<GSTIN>.xlsx). Each workbook has a "B2B" sheet (invoices) and a "CDNR" sheet (credit /
  debit notes), and the two sheets use different column headers.
  This job reads every workbook and every sheet, maps both header layouts to ONE layout, and
  stacks everything into a single table used by the 2B vs PR reconciliation (Activity 9).

  Added columns
    recipient_gstin  the Swiggy GSTIN, taken from the file name 2b_<GSTIN>.xlsx
    section          B2B or CDNR (the sheet the line came from)
    document_type    INV for invoices, CRN / DBN for credit / debit notes
    source_file      the workbook the line came from

STEPS
  1  Find every 2B workbook of the month
  2  Read each sheet and rename its headers to the common layout (SHEET_LAYOUTS)
  3  Stack all sheets, clean values and convert numbers and dates
  4  Write the output

QUICK CHANGES
  GSTN changed a 2B column header ...... SHEET_LAYOUTS
  New column ........................... SHEET_LAYOUTS + ALL_COLUMNS + STEP 3, then
                                         athena/tables/gstr2b_consolidated.sql

INPUT
  raw/<period>/2b_bolton/2b_<GSTIN>.xlsx                         sheets "B2B" and "CDNR"

OUTPUT
  intermediate/gstr2b_consolidated/period=<period>/gstr2b_consolidated.parquet   (Athena table)

RECORD ID
  Every 2B line arrives with a Record ID such as  2b-202506-10. It is kept unchanged and is
  used by Activity 9 to link each 2B line to its purchase register line.

HOW TO RUN
  python -m activities.gstr2b_consolidation.job --period 202506
  Add  --local-dir <folder>  to read/write the same paths in a local folder instead of S3.
=====================================================================================================
"""
import argparse
import io
import os
import re

import boto3
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# =====================================================================================================
# CONFIGURATION - header layouts of the two 2B sheets  ->  one common set of column names
# =====================================================================================================
COMMON_COLUMNS = {
    "Record ID":                     "record_id",
    "GSTIN of supplier":             "supplier_gstin",
    "Trade/Legal name":              "supplier_name",
    "Place of supply":               "place_of_supply",
    "Supply Attract Reverse Charge": "reverse_charge",
    "Rate (%)":                      "gst_rate",
    "Taxable Value (₹)":             "taxable_value",
    "Integrated Tax (₹)":            "igst_amount",
    "Central Tax (₹)":               "cgst_amount",
    "State/UT Tax (₹)":              "sgst_amount",
    "Cess (₹)":                      "cess_amount",
    "GSTR-1/IFF/GSTR-5 Period":      "supplier_filing_period",
    "GSTR-1/IFF/GSTR-5 Filing Date": "supplier_filing_date",
    "ITC Availability":              "itc_availability",
    "Reason":                        "itc_unavailability_reason",
    "Applicable % of Tax Rate":      "applicable_tax_rate_pct",
    "Source":                        "source",
    "IRN":                           "irn",
    "IRN Date":                      "irn_date",
}

SHEET_LAYOUTS = {
    "B2B": {
        **COMMON_COLUMNS,
        "Invoice number":    "document_number",
        "Invoice type":      "supply_type",
        "Invoice Date":      "document_date",
        "Invoice Value (₹)": "document_value",
    },
    "CDNR": {
        **COMMON_COLUMNS,
        "Note number":       "document_number",
        "Note type":         "note_type",
        "Note Supply type":  "supply_type",
        "Note date":         "document_date",
        "Note Value (₹)":    "document_value",
    },
}

# every column of the stacked table (a sheet that lacks one gets it blank)
ALL_COLUMNS = [
    "record_id", "supplier_gstin", "supplier_name", "document_number", "note_type", "supply_type",
    "document_date", "document_value", "place_of_supply", "reverse_charge", "gst_rate", "taxable_value",
    "igst_amount", "cgst_amount", "sgst_amount", "cess_amount", "supplier_filing_period",
    "supplier_filing_date", "itc_availability", "itc_unavailability_reason", "applicable_tax_rate_pct",
    "source", "irn", "irn_date",
]

GSTIN_IN_FILE_NAME = re.compile(r"2b_([0-9A-Z]{15})\.xlsx$", re.IGNORECASE)


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

    def list_files(self, prefix: str, suffix: str) -> list:
        """All file paths under a folder that end with suffix, e.g. every .xlsx in raw/202506/2b_bolton/"""
        if self.local_dir:
            folder = os.path.join(self.local_dir, prefix)
            names  = os.listdir(folder) if os.path.isdir(folder) else []
            return sorted(prefix + name for name in names if name.endswith(suffix))
        pages = self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix)
        return sorted(obj["Key"] for page in pages for obj in page.get("Contents", []) if obj["Key"].endswith(suffix))

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

    def read_all_sheets_as_text(self, key: str) -> dict:
        """{sheet name: DataFrame}; every cell read as text (blank cell = "")."""
        return pd.read_excel(io.BytesIO(self.read_bytes(key)), sheet_name=None, dtype=str, na_filter=False)

    def write_parquet(self, table: pa.Table, key: str) -> str:
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        return self.write_bytes(key, buffer.getvalue())


# =====================================================================================================
# THE JOB
# =====================================================================================================
def run(period: str, local_dir: str = None) -> dict:
    store = Storage(local_dir)
    input_folder = f"raw/{period}/2b_bolton/"
    output_key   = f"intermediate/gstr2b_consolidated/period={period}/gstr2b_consolidated.parquet"

    print(f"[GSTR-2B consolidation] period={period}")

    # -------------------------------------------------------------------------------------------------
    # STEP 1 - Find every 2B workbook of the month
    # -------------------------------------------------------------------------------------------------
    files = store.list_files(input_folder, ".xlsx")
    if not files:
        raise FileNotFoundError(f"No GSTR-2B workbooks found in {input_folder}")

    # -------------------------------------------------------------------------------------------------
    # STEP 2 - Read each sheet and rename its headers to the common layout
    # -------------------------------------------------------------------------------------------------
    pieces = []
    for file_key in files:
        file_name = file_key.rsplit("/", 1)[-1]
        gstin_match = GSTIN_IN_FILE_NAME.search(file_name)
        if not gstin_match:
            raise ValueError(f"Cannot read Swiggy GSTIN from file name {file_name} (expected 2b_<GSTIN>.xlsx)")

        for sheet_name, sheet in store.read_all_sheets_as_text(file_key).items():
            layout = SHEET_LAYOUTS.get(sheet_name.upper())
            if layout is None:
                print(f"  {file_name}: sheet '{sheet_name}' is not B2B/CDNR - skipped")
                continue
            if sheet.empty:
                continue
            missing = [h for h in layout if h not in sheet.columns]
            if missing:
                raise ValueError(f"{file_name} sheet {sheet_name} is missing columns: {missing}")

            piece = sheet[list(layout)].rename(columns=layout).reindex(columns=ALL_COLUMNS, fill_value="")
            piece["recipient_gstin"] = gstin_match.group(1).upper()
            piece["section"]         = sheet_name.upper()
            piece["source_file"]     = file_name
            pieces.append(piece)
            print(f"  {file_name} / {sheet_name}: {len(piece)} lines")

    # -------------------------------------------------------------------------------------------------
    # STEP 3 - Stack all sheets, clean values and convert numbers and dates
    # -------------------------------------------------------------------------------------------------
    con = duckdb.connect()
    con.register("stacked", pd.concat(pieces, ignore_index=True))
    result = con.execute("""
        SELECT
            NULLIF(trim(record_id), '')                                          AS record_id,
            recipient_gstin,
            section,
            CASE WHEN section = 'B2B' THEN 'INV'
                 WHEN upper(note_type) LIKE 'DEBIT%' THEN 'DBN'
                 ELSE 'CRN' END                                                  AS document_type,
            upper(NULLIF(trim(supplier_gstin), ''))                              AS supplier_gstin,
            NULLIF(trim(supplier_name), '')                                      AS supplier_name,
            NULLIF(trim(document_number), '')                                    AS document_number,
            CAST(try_strptime(trim(document_date), '%d/%m/%Y') AS DATE)          AS document_date,
            NULLIF(trim(supply_type), '')                                        AS supply_type,
            NULLIF(trim(note_type), '')                                          AS note_type,
            TRY_CAST(NULLIF(trim(document_value), '') AS DOUBLE)                 AS document_value,
            NULLIF(trim(place_of_supply), '')                                    AS place_of_supply,
            upper(NULLIF(trim(reverse_charge), ''))                              AS reverse_charge,
            TRY_CAST(NULLIF(trim(gst_rate), '') AS DOUBLE)                       AS gst_rate,
            TRY_CAST(NULLIF(trim(taxable_value), '') AS DOUBLE)                  AS taxable_value,
            COALESCE(TRY_CAST(NULLIF(trim(igst_amount), '') AS DOUBLE), 0)       AS igst_amount,
            COALESCE(TRY_CAST(NULLIF(trim(cgst_amount), '') AS DOUBLE), 0)       AS cgst_amount,
            COALESCE(TRY_CAST(NULLIF(trim(sgst_amount), '') AS DOUBLE), 0)       AS sgst_amount,
            COALESCE(TRY_CAST(NULLIF(trim(cess_amount), '') AS DOUBLE), 0)       AS cess_amount,
            NULLIF(trim(supplier_filing_period), '')                             AS supplier_filing_period,
            CAST(try_strptime(trim(supplier_filing_date), '%d/%m/%Y') AS DATE)   AS supplier_filing_date,
            NULLIF(trim(itc_availability), '')                                   AS itc_availability,
            NULLIF(trim(itc_unavailability_reason), '')                          AS itc_unavailability_reason,
            TRY_CAST(NULLIF(trim(applicable_tax_rate_pct), '') AS DOUBLE)        AS applicable_tax_rate_pct,
            NULLIF(trim(source), '')                                             AS source,
            NULLIF(trim(irn), '')                                                AS irn,
            CAST(try_strptime(trim(irn_date), '%d/%m/%Y') AS DATE)               AS irn_date,
            source_file
        FROM stacked
    """).arrow()
    con.register("consolidated", result)

    by_file = con.execute("""
        SELECT source_file, section, count(*) AS lines, round(sum(taxable_value), 2) AS taxable_value
        FROM consolidated GROUP BY source_file, section ORDER BY source_file, section
    """).fetchall()
    duplicates = con.execute("""
        SELECT count(*) FROM (SELECT record_id FROM consolidated GROUP BY record_id HAVING count(*) > 1)
    """).fetchone()[0]
    con.close()

    # -------------------------------------------------------------------------------------------------
    # STEP 4 - Write the output
    # -------------------------------------------------------------------------------------------------
    store.write_parquet(result, output_key)

    print(f"  Consolidated lines: {result.num_rows} from {len(files)} workbooks | duplicate Record IDs: {duplicates}")
    print(f"  Output: {output_key}")
    return {"files": len(files), "lines": result.num_rows, "duplicate_record_ids": int(duplicates),
            "by_file": [list(row) for row in by_file]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consolidate all GSTR-2B workbooks of a month")
    parser.add_argument("--period", required=True, help="return period YYYYMM, e.g. 202506")
    parser.add_argument("--local-dir", help="use this local folder instead of S3")
    args = parser.parse_args()
    run(args.period, args.local_dir)
