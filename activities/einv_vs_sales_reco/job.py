"""
=====================================================================================================
JOB      : E-Invoice (EINV) vs Sales Reconciliation                               (Activity 5)
CLIENT   : Swiggy
RUNS     : Once per month, after Combine Batches (Activity 4). Runs in parallel with Activity 6.
-----------------------------------------------------------------------------------------------------
WHAT IT DOES
  Every B2B invoice, credit/debit note to a registered buyer and export must have an e-invoice
  (IRN) on the IRP portal. This job matches the IRN list downloaded through the Bolton tool
  against Swiggy's validated sales and reports what matches and what does not.

  LEFT  = EINV (active IRNs only)
  RIGHT = Sales lines that need an IRN (status Success, invoice type B2B / CDNR / EXP)

  Matching ladder - each level only looks at rows NOT matched by an earlier level:
    L1 Exact              buyer GSTIN + invoice number + taxable value (within Rs.1)
    L2 Amount mismatch    buyer GSTIN + invoice number              -> taxable values differ
    L3 Invoice no mismatch buyer GSTIN + taxable value (within Rs.1) -> invoice numbers differ
    Only in EINV          IRN exists but the invoice is not in the sales register
    Only in Sales         invoice in the sales register but no active IRN
  Invoice numbers are compared after removing "/", "-" and spaces and ignoring case,
  so "SWG/2506/00012" and "swg-2506-00012" count as the same number.
  Each row is matched at most once; if several rows qualify, the closest amount wins.

STEPS
  1  Read the EINV file, rename the columns, clean values and convert numbers and dates
  2  Build the two sides in the same shape: LEFT = EINV, RIGHT = sales
  3  Matching ladder: level 1, then level 2 and 3 on what is still unmatched
  4  Stack matched pairs, EINV-only lines and sales-only lines into one table
  5  Differences and a remark per row
  6  Summary per match level
  7  Write the outputs

QUICK CHANGES
  Change a match level / add a level ... STEP 3 (one match_level(...) call per level)
  Amount tolerance ..................... AMOUNT_TOLERANCE_RS
  Which sales lines are compared ....... SALES_NEEDING_EINV
  Column renamed in the EINV file ..... EINV_COLUMN_MAP

INPUT
  raw/<period>/einv_bolton/einv_bolton.xlsx                                   sheet "EINV"
  intermediate/sales_combined/period=<period>/sales_combined.parquet          (Activity 4)

OUTPUT
  intermediate/einv_records/period=<period>/einv_records.parquet              EINV data, cleaned (Athena)
  intermediate/einv_vs_sales_reco/period=<period>/einv_vs_sales_reco.parquet  one row per match (Athena)
  final/<period>/einv_vs_sales_reco.xlsx                                      Summary + Reco Details

RECORD ID
  EINV lines carry IDs like einv-202506-1, sales lines like sales-202506-b2b-1.
  Every reco row shows BOTH ids (einv_record_id, sales_record_id). The Summary sheet lists
  all ids behind each count, joined with " | ".

HOW TO RUN
  python -m activities.einv_vs_sales_reco.job --period 202506
  Add  --local-dir <folder>  to read/write the same paths in a local folder instead of S3.
=====================================================================================================
"""
import argparse
import io
import os

import boto3
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# =====================================================================================================
# CONFIGURATION
# =====================================================================================================
AMOUNT_TOLERANCE_RS = 1.0

# Column header in the EINV file  ->  column name used in this job
EINV_COLUMN_MAP = {
    "Record ID":              "record_id",
    "IRN":                    "irn",
    "Ack No":                 "ack_no",
    "Ack Date":               "ack_date",
    "Document Type":          "document_type",
    "Document No":            "document_number",
    "Document Date":          "document_date",
    "Supply Type":            "supply_type",
    "Supplier GSTIN":         "supplier_gstin",
    "Buyer GSTIN":            "buyer_gstin",
    "Buyer Legal Name":       "buyer_legal_name",
    "Place of Supply":        "place_of_supply",
    "Reverse Charge":         "reverse_charge",
    "Total Assessable Value": "assessable_value",
    "IGST Amount":            "igst_amount",
    "CGST Amount":            "cgst_amount",
    "SGST Amount":            "sgst_amount",
    "Cess Amount":            "cess_amount",
    "Total Invoice Value":    "total_invoice_value",
    "IRN Status":             "irn_status",
    "EWB No":                 "ewb_no",
}

# Which sales lines need an e-invoice
SALES_NEEDING_IRN = "validation_status = 'Success' AND invoice_type IN ('B2B', 'CDNR', 'EXP')"


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
        """Every cell is read as text (blank cell = ""). Numbers and dates are converted later, on purpose."""
        return pd.read_excel(io.BytesIO(self.read_bytes(key)), sheet_name=sheet_name, dtype=str, na_filter=False)

    def read_parquet(self, key: str) -> pa.Table:
        return pq.read_table(io.BytesIO(self.read_bytes(key)))

    def write_parquet(self, table: pa.Table, key: str) -> str:
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        return self.write_bytes(key, buffer.getvalue())

    def write_excel(self, sheets: dict, key: str) -> str:
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            for sheet_name, df in sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        return self.write_bytes(key, buffer.getvalue())


# =====================================================================================================
# MATCHING LADDER - one level
# =====================================================================================================
def match_level(con, level_no: int, level_name: str, join_condition: str) -> int:
    """
    Join the rows of left_side and right_side that are BOTH still unmatched, using join_condition.
    When one row could pair with several, the pair with the smallest amount difference wins,
    and every row is used at most once. Pairs found are added to the table "matches".
    """
    con.execute(f"""
        INSERT INTO matches
        SELECT left_record_id, right_record_id, {level_no}, '{level_name}'
        FROM (
            SELECT l.record_id AS left_record_id,
                   r.record_id AS right_record_id,
                   row_number() OVER (PARTITION BY l.record_id ORDER BY abs(l.amount - r.amount), r.record_id) AS left_pick,
                   row_number() OVER (PARTITION BY r.record_id ORDER BY abs(l.amount - r.amount), l.record_id) AS right_pick
            FROM left_side l
            JOIN right_side r ON {join_condition}
            WHERE l.record_id NOT IN (SELECT left_record_id  FROM matches)
              AND r.record_id NOT IN (SELECT right_record_id FROM matches)
        )
        WHERE left_pick = 1 AND right_pick = 1
    """)
    return con.execute(f"SELECT count(*) FROM matches WHERE level_no = {level_no}").fetchone()[0]


# =====================================================================================================
# THE JOB
# =====================================================================================================
def run(period: str, local_dir: str = None) -> dict:
    store = Storage(local_dir)
    einv_key    = f"raw/{period}/einv_bolton/einv_bolton.xlsx"
    sales_key   = f"intermediate/sales_combined/period={period}/sales_combined.parquet"
    records_key = f"intermediate/einv_records/period={period}/einv_records.parquet"
    reco_key    = f"intermediate/einv_vs_sales_reco/period={period}/einv_vs_sales_reco.parquet"
    excel_key   = f"final/{period}/einv_vs_sales_reco.xlsx"

    print(f"[EINV vs Sales reco] period={period}")

    # -------------------------------------------------------------------------------------------------
    # STEP 1 - Read the EINV file, rename the columns, clean values and convert numbers and dates
    # -------------------------------------------------------------------------------------------------
    einv_raw = store.read_excel_as_text(einv_key, sheet_name="EINV")
    missing  = [h for h in EINV_COLUMN_MAP if h not in einv_raw.columns]
    if missing:
        raise ValueError(f"EINV file is missing columns: {missing}")
    einv_raw = einv_raw[list(EINV_COLUMN_MAP)].rename(columns=EINV_COLUMN_MAP)

    con = duckdb.connect()
    con.register("einv_raw", einv_raw)
    einv_records = con.execute("""
        SELECT
            NULLIF(trim(record_id), '')                                       AS record_id,
            NULLIF(trim(irn), '')                                             AS irn,
            NULLIF(trim(ack_no), '')                                          AS ack_no,
            CAST(try_strptime(trim(ack_date), '%d/%m/%Y') AS DATE)            AS ack_date,
            upper(NULLIF(trim(document_type), ''))                            AS document_type,
            NULLIF(trim(document_number), '')                                 AS document_number,
            CAST(try_strptime(trim(document_date), '%d/%m/%Y') AS DATE)       AS document_date,
            upper(NULLIF(trim(supply_type), ''))                              AS supply_type,
            upper(NULLIF(trim(supplier_gstin), ''))                           AS supplier_gstin,
            upper(NULLIF(trim(buyer_gstin), ''))                              AS buyer_gstin,
            NULLIF(trim(buyer_legal_name), '')                                AS buyer_legal_name,
            NULLIF(trim(place_of_supply), '')                                 AS place_of_supply,
            upper(NULLIF(trim(reverse_charge), ''))                           AS reverse_charge,
            TRY_CAST(NULLIF(trim(assessable_value), '') AS DOUBLE)            AS assessable_value,
            COALESCE(TRY_CAST(NULLIF(trim(igst_amount), '') AS DOUBLE), 0)    AS igst_amount,
            COALESCE(TRY_CAST(NULLIF(trim(cgst_amount), '') AS DOUBLE), 0)    AS cgst_amount,
            COALESCE(TRY_CAST(NULLIF(trim(sgst_amount), '') AS DOUBLE), 0)    AS sgst_amount,
            COALESCE(TRY_CAST(NULLIF(trim(cess_amount), '') AS DOUBLE), 0)    AS cess_amount,
            TRY_CAST(NULLIF(trim(total_invoice_value), '') AS DOUBLE)         AS total_invoice_value,
            upper(NULLIF(trim(irn_status), ''))                               AS irn_status,
            NULLIF(trim(ewb_no), '')                                          AS ewb_no
        FROM einv_raw
    """).arrow()
    con.register("einv", einv_records)
    con.register("sales", store.read_parquet(sales_key))

    # -------------------------------------------------------------------------------------------------
    # STEP 2 - The two sides of the reconciliation, in the same shape
    #   invoice_key = invoice number without "/", "-", spaces, in upper case
    # -------------------------------------------------------------------------------------------------
    con.execute("""
        CREATE TABLE left_side AS                      -- LEFT = EINV, active IRNs only
        SELECT record_id, irn, buyer_gstin AS gstin, document_number AS invoice_number,
               upper(regexp_replace(document_number, '[^0-9A-Za-z]', '', 'g')) AS invoice_key,
               document_date AS invoice_date, assessable_value AS amount
        FROM einv
        WHERE irn_status = 'ACT'
    """)
    con.execute(f"""
        CREATE TABLE right_side AS                     -- RIGHT = sales lines that need an IRN
        SELECT record_id, batch_id, COALESCE(receiver_gstin, 'URP') AS gstin, invoice_number,
               upper(regexp_replace(invoice_number, '[^0-9A-Za-z]', '', 'g')) AS invoice_key,
               invoice_date, taxable_value AS amount
        FROM sales
        WHERE {SALES_NEEDING_IRN}
    """)
    cancelled = con.execute("SELECT count(*) FROM einv WHERE irn_status <> 'ACT'").fetchone()[0]
    left_count, right_count = (con.execute("SELECT count(*) FROM left_side").fetchone()[0],
                               con.execute("SELECT count(*) FROM right_side").fetchone()[0])
    print(f"  EINV active IRNs: {left_count} (cancelled ignored: {cancelled}) | sales lines needing IRN: {right_count}")

    # -------------------------------------------------------------------------------------------------
    # STEP 3 - Matching ladder: level 1, 2, 3 (each level only sees what is still unmatched)
    # -------------------------------------------------------------------------------------------------
    con.execute("CREATE TABLE matches (left_record_id VARCHAR, right_record_id VARCHAR, level_no INTEGER, match_level VARCHAR)")
    match_level(con, 1, "L1 Exact",
                f"l.gstin = r.gstin AND l.invoice_key = r.invoice_key AND abs(l.amount - r.amount) <= {AMOUNT_TOLERANCE_RS}")
    match_level(con, 2, "L2 Amount mismatch",
                "l.gstin = r.gstin AND l.invoice_key = r.invoice_key")
    match_level(con, 3, "L3 Invoice no mismatch",
                f"l.gstin = r.gstin AND abs(l.amount - r.amount) <= {AMOUNT_TOLERANCE_RS}")

    # -------------------------------------------------------------------------------------------------
    # STEP 4 - Stack matched pairs + EINV-only lines + sales-only lines into one table.
    #   UNION ALL BY NAME matches columns by name; columns a part does not have become NULL.
    # -------------------------------------------------------------------------------------------------
    con.execute("""
        CREATE TABLE reco_union AS
        SELECT m.match_level, m.level_no,
               l.record_id AS einv_record_id, r.record_id AS sales_record_id, r.batch_id AS sales_batch_id,
               l.irn, l.gstin AS einv_buyer_gstin, r.gstin AS sales_receiver_gstin,
               l.invoice_number AS einv_document_number, r.invoice_number AS sales_invoice_number,
               l.invoice_date AS einv_document_date, r.invoice_date AS sales_invoice_date,
               l.amount AS einv_taxable_value, r.amount AS sales_taxable_value
        FROM matches m
        JOIN left_side  l ON l.record_id = m.left_record_id
        JOIN right_side r ON r.record_id = m.right_record_id

        UNION ALL BY NAME
        SELECT 'Only in EINV' AS match_level, 4 AS level_no,
               l.record_id AS einv_record_id, l.irn, l.gstin AS einv_buyer_gstin,
               l.invoice_number AS einv_document_number, l.invoice_date AS einv_document_date,
               l.amount AS einv_taxable_value
        FROM left_side l WHERE l.record_id NOT IN (SELECT left_record_id FROM matches)

        UNION ALL BY NAME
        SELECT 'Only in Sales' AS match_level, 5 AS level_no,
               r.record_id AS sales_record_id, r.batch_id AS sales_batch_id, r.gstin AS sales_receiver_gstin,
               r.invoice_number AS sales_invoice_number, r.invoice_date AS sales_invoice_date,
               r.amount AS sales_taxable_value
        FROM right_side r WHERE r.record_id NOT IN (SELECT right_record_id FROM matches)
    """)

    # -------------------------------------------------------------------------------------------------
    # STEP 5 - Differences and a remark per row (final column list and types - Athena reads them)
    # -------------------------------------------------------------------------------------------------
    reco = con.execute("""
        SELECT match_level, CAST(level_no AS INTEGER) AS level_no,
               einv_record_id, sales_record_id, sales_batch_id, irn,
               einv_buyer_gstin, sales_receiver_gstin,
               einv_document_number, sales_invoice_number,
               CAST(einv_document_date AS DATE) AS einv_document_date,
               CAST(sales_invoice_date AS DATE) AS sales_invoice_date,
               einv_taxable_value, sales_taxable_value,
               round(COALESCE(einv_taxable_value, 0) - COALESCE(sales_taxable_value, 0), 2) AS taxable_value_difference,
               CASE level_no
                   WHEN 1 THEN 'Matched'
                   WHEN 2 THEN 'Taxable value differs by Rs.' || CAST(round(einv_taxable_value - sales_taxable_value, 2) AS VARCHAR)
                   WHEN 3 THEN 'Invoice number differs: EINV ' || einv_document_number || ' vs sales ' || sales_invoice_number
                   WHEN 4 THEN 'Active IRN but invoice not in sales register'
                   WHEN 5 THEN 'Invoice in sales register but no active IRN'
               END AS remark
        FROM reco_union
        ORDER BY level_no, COALESCE(sales_record_id, einv_record_id)
    """).arrow()
    con.register("reco_result", reco)

    # -------------------------------------------------------------------------------------------------
    # STEP 6 - Summary per match level: counts, amounts and every Record ID behind each count
    # -------------------------------------------------------------------------------------------------
    summary = con.execute("""
        SELECT match_level                                              AS "Match Level",
               count(*)                                                 AS "Records",
               round(sum(COALESCE(einv_taxable_value, 0)), 2)           AS "EINV Taxable Value",
               round(sum(COALESCE(sales_taxable_value, 0)), 2)          AS "Sales Taxable Value",
               round(sum(taxable_value_difference), 2)                  AS "Difference",
               string_agg(einv_record_id,  ' | ' ORDER BY einv_record_id)  AS "EINV Record IDs",
               string_agg(sales_record_id, ' | ' ORDER BY sales_record_id) AS "Sales Record IDs"
        FROM reco_result
        GROUP BY match_level, level_no
        ORDER BY level_no
    """).df()
    con.close()

    # -------------------------------------------------------------------------------------------------
    # STEP 7 - Write the outputs
    # -------------------------------------------------------------------------------------------------
    store.write_parquet(einv_records, records_key)
    store.write_parquet(reco, reco_key)
    details = reco.to_pandas()
    for column in ["einv_document_date", "sales_invoice_date"]:
        details[column] = pd.to_datetime(details[column]).dt.strftime("%d/%m/%Y")
    store.write_excel({"Summary": summary, "Reco Details": details}, excel_key)

    print(summary[["Match Level", "Records"]].to_string(index=False))
    print(f"  Output: {reco_key}")
    print(f"          {excel_key}")
    return {level: int(count) for level, count in zip(summary["Match Level"], summary["Records"])}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reconcile EINV (IRN) data against sales")
    parser.add_argument("--period", required=True, help="return period YYYYMM, e.g. 202506")
    parser.add_argument("--local-dir", help="use this local folder instead of S3")
    args = parser.parse_args()
    run(args.period, args.local_dir)
