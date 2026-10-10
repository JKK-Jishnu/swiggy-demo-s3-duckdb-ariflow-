"""
=====================================================================================================
JOB      : E-Way Bill (EWB) vs Sales Reconciliation                               (Activity 6)
CLIENT   : Swiggy
RUNS     : Once per month, after Combine Batches (Activity 4). Runs in parallel with Activity 5.
-----------------------------------------------------------------------------------------------------
WHAT IT DOES
  Goods moved with an invoice value above Rs.50,000 need an e-way bill. This job matches the
  e-way bills downloaded through the Bolton tool against Swiggy's validated sales.

  LEFT  = EWB (active e-way bills only)
  RIGHT = Sales lines that need an e-way bill:
          status Success, tax invoice (INV), goods (HSN not starting with 99), value > Rs.50,000

  Matching ladder - each level only looks at rows NOT matched by an earlier level:
    L1 Exact               recipient GSTIN + invoice number + taxable value (within Rs.1)
    L2 Amount mismatch     recipient GSTIN + invoice number              -> taxable values differ
    L3 Invoice no mismatch recipient GSTIN + taxable value (within Rs.1) -> invoice numbers differ
    Only in EWB            e-way bill exists but the invoice is not in the sales register
    Only in Sales          invoice needs an e-way bill but none was found
  Unregistered recipients (B2C, exports) use "URP" as GSTIN on both sides.
  Invoice numbers are compared after removing "/", "-" and spaces and ignoring case.
  Each row is matched at most once; if several rows qualify, the closest amount wins.

ALTERYX EQUIVALENT
  Input Data (EWB) + Input Data (sales) -> Filter -> Formula (normalise invoice no)
  -> Join (level 1) -> unjoined L/R into Join (level 2) -> unjoined into Join (level 3)
  -> Union (J + L + R) -> Summarize -> Output Data

INPUT
  raw/<period>/ewb_bolton/ewb_bolton.xlsx                                     sheet "EWB"
  intermediate/sales_combined/period=<period>/sales_combined.parquet          (Activity 4)

OUTPUT
  intermediate/ewb_records/period=<period>/ewb_records.parquet                EWB data, cleaned (Athena)
  intermediate/ewb_vs_sales_reco/period=<period>/ewb_vs_sales_reco.parquet    one row per match (Athena)
  final/<period>/ewb_vs_sales_reco.xlsx                                       Summary + Reco Details

RECORD ID
  EWB lines carry IDs like ewb-202506-1, sales lines like sales-202506-b2b-1.
  Every reco row shows BOTH ids. The Summary sheet lists all ids behind each count, joined with " | ".

HOW TO RUN
  python -m activities.ewb_vs_sales_reco.job --period 202506
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
EWB_LIMIT_RS        = 50000

# EWB file headers -> column names used inside this job (Select tool)
EWB_COLUMN_MAP = {
    "Record ID":           "record_id",
    "EWB No":              "ewb_no",
    "EWB Date":            "ewb_date",
    "Supply Type":         "supply_type",
    "Sub Type":            "sub_type",
    "Document Type":       "document_type",
    "Document No":         "document_number",
    "Document Date":       "document_date",
    "From GSTIN":          "from_gstin",
    "From Trade Name":     "from_trade_name",
    "From Pincode":        "from_pincode",
    "From State Code":     "from_state_code",
    "To GSTIN":            "to_gstin",
    "To Trade Name":       "to_trade_name",
    "To Pincode":          "to_pincode",
    "To State Code":       "to_state_code",
    "HSN Code":            "hsn_code",
    "Taxable Value":       "taxable_value",
    "IGST Amount":         "igst_amount",
    "CGST Amount":         "cgst_amount",
    "SGST Amount":         "sgst_amount",
    "Cess Amount":         "cess_amount",
    "Total Invoice Value": "total_invoice_value",
    "Transport Mode":      "transport_mode",
    "Distance (km)":       "distance_km",
    "Transporter ID":      "transporter_id",
    "Vehicle No":          "vehicle_no",
    "Valid Upto":          "valid_upto",
    "EWB Status":          "ewb_status",
}

# Which sales lines need an e-way bill
SALES_NEEDING_EWB = (f"validation_status = 'Success' AND document_type = 'INV' "
                     f"AND hsn_sac NOT LIKE '99%' AND total_invoice_value > {EWB_LIMIT_RS}")


# =====================================================================================================
# STORAGE  (Input Data / Output Data tools)
# Reads and writes files in S3. With --local-dir the same paths are used inside a local folder.
# =====================================================================================================
class Storage:
    def __init__(self, local_dir: str = None):
        self.bucket    = os.environ.get("S3_BUCKET", "swiggy-idt-poc-bucket")
        self.local_dir = local_dir
        self.s3        = None if local_dir else boto3.client("s3", region_name=os.environ.get("AWS_DEFAULT_REGION", "ap-south-1"))

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
    ewb_key     = f"raw/{period}/ewb_bolton/ewb_bolton.xlsx"
    sales_key   = f"intermediate/sales_combined/period={period}/sales_combined.parquet"
    records_key = f"intermediate/ewb_records/period={period}/ewb_records.parquet"
    reco_key    = f"intermediate/ewb_vs_sales_reco/period={period}/ewb_vs_sales_reco.parquet"
    excel_key   = f"final/{period}/ewb_vs_sales_reco.xlsx"

    print(f"[EWB vs Sales reco] period={period}")

    # -------------------------------------------------------------------------------------------------
    # STEP 1 - INPUT + SELECT + CLEANSING: EWB file (text) -> typed EWB records
    # -------------------------------------------------------------------------------------------------
    ewb_raw = store.read_excel_as_text(ewb_key, sheet_name="EWB")
    missing = [h for h in EWB_COLUMN_MAP if h not in ewb_raw.columns]
    if missing:
        raise ValueError(f"EWB file is missing columns: {missing}")
    ewb_raw = ewb_raw[list(EWB_COLUMN_MAP)].rename(columns=EWB_COLUMN_MAP)

    con = duckdb.connect()
    con.register("ewb_raw", ewb_raw)
    ewb_records = con.execute("""
        SELECT
            NULLIF(trim(record_id), '')                                       AS record_id,
            NULLIF(trim(ewb_no), '')                                          AS ewb_no,
            CAST(try_strptime(trim(ewb_date), '%d/%m/%Y') AS DATE)            AS ewb_date,
            NULLIF(trim(supply_type), '')                                     AS supply_type,
            NULLIF(trim(sub_type), '')                                        AS sub_type,
            NULLIF(trim(document_type), '')                                   AS document_type,
            NULLIF(trim(document_number), '')                                 AS document_number,
            CAST(try_strptime(trim(document_date), '%d/%m/%Y') AS DATE)       AS document_date,
            upper(NULLIF(trim(from_gstin), ''))                               AS from_gstin,
            NULLIF(trim(from_trade_name), '')                                 AS from_trade_name,
            NULLIF(trim(from_pincode), '')                                    AS from_pincode,
            NULLIF(trim(from_state_code), '')                                 AS from_state_code,
            upper(NULLIF(trim(to_gstin), ''))                                 AS to_gstin,
            NULLIF(trim(to_trade_name), '')                                   AS to_trade_name,
            NULLIF(trim(to_pincode), '')                                      AS to_pincode,
            NULLIF(trim(to_state_code), '')                                   AS to_state_code,
            NULLIF(trim(hsn_code), '')                                        AS hsn_code,
            TRY_CAST(NULLIF(trim(taxable_value), '') AS DOUBLE)               AS taxable_value,
            COALESCE(TRY_CAST(NULLIF(trim(igst_amount), '') AS DOUBLE), 0)    AS igst_amount,
            COALESCE(TRY_CAST(NULLIF(trim(cgst_amount), '') AS DOUBLE), 0)    AS cgst_amount,
            COALESCE(TRY_CAST(NULLIF(trim(sgst_amount), '') AS DOUBLE), 0)    AS sgst_amount,
            COALESCE(TRY_CAST(NULLIF(trim(cess_amount), '') AS DOUBLE), 0)    AS cess_amount,
            TRY_CAST(NULLIF(trim(total_invoice_value), '') AS DOUBLE)         AS total_invoice_value,
            NULLIF(trim(transport_mode), '')                                  AS transport_mode,
            TRY_CAST(NULLIF(trim(distance_km), '') AS INTEGER)                AS distance_km,
            NULLIF(trim(transporter_id), '')                                  AS transporter_id,
            NULLIF(trim(vehicle_no), '')                                      AS vehicle_no,
            CAST(try_strptime(trim(valid_upto), '%d/%m/%Y') AS DATE)          AS valid_upto,
            upper(NULLIF(trim(ewb_status), ''))                               AS ewb_status
        FROM ewb_raw
    """).arrow()
    con.register("ewb", ewb_records)
    con.register("sales", store.read_parquet(sales_key))

    # -------------------------------------------------------------------------------------------------
    # STEP 2 - FILTER + FORMULA: the two sides of the reconciliation, in the same shape
    #   invoice_key = invoice number without "/", "-", spaces, in upper case
    # -------------------------------------------------------------------------------------------------
    con.execute("""
        CREATE TABLE left_side AS                      -- LEFT = EWB, active e-way bills only
        SELECT record_id, ewb_no, to_gstin AS gstin, document_number AS invoice_number,
               upper(regexp_replace(document_number, '[^0-9A-Za-z]', '', 'g')) AS invoice_key,
               document_date AS invoice_date, taxable_value AS amount
        FROM ewb
        WHERE ewb_status = 'ACT'
    """)
    con.execute(f"""
        CREATE TABLE right_side AS                     -- RIGHT = sales lines that need an e-way bill
        SELECT record_id, batch_id, COALESCE(receiver_gstin, 'URP') AS gstin, invoice_number,
               upper(regexp_replace(invoice_number, '[^0-9A-Za-z]', '', 'g')) AS invoice_key,
               invoice_date, taxable_value AS amount
        FROM sales
        WHERE {SALES_NEEDING_EWB}
    """)
    left_count, right_count = (con.execute("SELECT count(*) FROM left_side").fetchone()[0],
                               con.execute("SELECT count(*) FROM right_side").fetchone()[0])
    print(f"  Active e-way bills: {left_count} | sales lines needing an e-way bill: {right_count}")

    # -------------------------------------------------------------------------------------------------
    # STEP 3 - JOIN LADDER: level 1, 2, 3 (each on what is still unmatched)
    # -------------------------------------------------------------------------------------------------
    con.execute("CREATE TABLE matches (left_record_id VARCHAR, right_record_id VARCHAR, level_no INTEGER, match_level VARCHAR)")
    match_level(con, 1, "L1 Exact",
                f"l.gstin = r.gstin AND l.invoice_key = r.invoice_key AND abs(l.amount - r.amount) <= {AMOUNT_TOLERANCE_RS}")
    match_level(con, 2, "L2 Amount mismatch",
                "l.gstin = r.gstin AND l.invoice_key = r.invoice_key")
    match_level(con, 3, "L3 Invoice no mismatch",
                f"l.gstin = r.gstin AND abs(l.amount - r.amount) <= {AMOUNT_TOLERANCE_RS}")

    # -------------------------------------------------------------------------------------------------
    # STEP 4 - UNION: matched pairs (J) + EWB only (L) + Sales only (R), with a remark per row
    # -------------------------------------------------------------------------------------------------
    con.execute("""
        CREATE TABLE reco_union AS
        SELECT m.match_level, m.level_no,
               l.record_id AS ewb_record_id, r.record_id AS sales_record_id, r.batch_id AS sales_batch_id,
               l.ewb_no, l.gstin AS ewb_to_gstin, r.gstin AS sales_receiver_gstin,
               l.invoice_number AS ewb_document_number, r.invoice_number AS sales_invoice_number,
               l.invoice_date AS ewb_document_date, r.invoice_date AS sales_invoice_date,
               l.amount AS ewb_taxable_value, r.amount AS sales_taxable_value
        FROM matches m
        JOIN left_side  l ON l.record_id = m.left_record_id
        JOIN right_side r ON r.record_id = m.right_record_id

        UNION ALL
        SELECT 'Only in EWB', 4, l.record_id, NULL, NULL, l.ewb_no, l.gstin, NULL,
               l.invoice_number, NULL, l.invoice_date, NULL, l.amount, NULL
        FROM left_side l WHERE l.record_id NOT IN (SELECT left_record_id FROM matches)

        UNION ALL
        SELECT 'Only in Sales', 5, NULL, r.record_id, r.batch_id, NULL, NULL, r.gstin,
               NULL, r.invoice_number, NULL, r.invoice_date, NULL, r.amount
        FROM right_side r WHERE r.record_id NOT IN (SELECT right_record_id FROM matches)
    """)

    reco = con.execute("""
        SELECT match_level, CAST(level_no AS INTEGER) AS level_no,
               ewb_record_id, sales_record_id, sales_batch_id, ewb_no,
               ewb_to_gstin, sales_receiver_gstin,
               ewb_document_number, sales_invoice_number,
               CAST(ewb_document_date AS DATE)  AS ewb_document_date,
               CAST(sales_invoice_date AS DATE) AS sales_invoice_date,
               ewb_taxable_value, sales_taxable_value,
               round(COALESCE(ewb_taxable_value, 0) - COALESCE(sales_taxable_value, 0), 2) AS taxable_value_difference,
               CASE level_no
                   WHEN 1 THEN 'Matched'
                   WHEN 2 THEN 'Taxable value differs by Rs.' || CAST(round(ewb_taxable_value - sales_taxable_value, 2) AS VARCHAR)
                   WHEN 3 THEN 'Invoice number differs: EWB ' || ewb_document_number || ' vs sales ' || sales_invoice_number
                   WHEN 4 THEN 'E-way bill exists but invoice not in sales register'
                   WHEN 5 THEN 'Invoice needs an e-way bill but none found'
               END AS remark
        FROM reco_union
        ORDER BY level_no, COALESCE(sales_record_id, ewb_record_id)
    """).arrow()
    con.register("reco_result", reco)

    # -------------------------------------------------------------------------------------------------
    # STEP 5 - SUMMARIZE: counts and amounts per match level, with every Record ID behind each count
    # -------------------------------------------------------------------------------------------------
    summary = con.execute("""
        SELECT match_level                                              AS "Match Level",
               count(*)                                                 AS "Records",
               round(sum(COALESCE(ewb_taxable_value, 0)), 2)            AS "EWB Taxable Value",
               round(sum(COALESCE(sales_taxable_value, 0)), 2)          AS "Sales Taxable Value",
               round(sum(taxable_value_difference), 2)                  AS "Difference",
               string_agg(ewb_record_id,   ' | ' ORDER BY ewb_record_id)   AS "EWB Record IDs",
               string_agg(sales_record_id, ' | ' ORDER BY sales_record_id) AS "Sales Record IDs"
        FROM reco_result
        GROUP BY match_level, level_no
        ORDER BY level_no
    """).df()
    con.close()

    # -------------------------------------------------------------------------------------------------
    # STEP 6 - OUTPUT
    # -------------------------------------------------------------------------------------------------
    store.write_parquet(ewb_records, records_key)
    store.write_parquet(reco, reco_key)
    details = reco.to_pandas()
    for column in ["ewb_document_date", "sales_invoice_date"]:
        details[column] = pd.to_datetime(details[column]).dt.strftime("%d/%m/%Y")
    store.write_excel({"Summary": summary, "Reco Details": details}, excel_key)

    print(summary[["Match Level", "Records"]].to_string(index=False))
    print(f"  Output: {reco_key}")
    print(f"          {excel_key}")
    return {level: int(count) for level, count in zip(summary["Match Level"], summary["Records"])}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reconcile e-way bill data against sales")
    parser.add_argument("--period", required=True, help="return period YYYYMM, e.g. 202506")
    parser.add_argument("--local-dir", help="use this local folder instead of S3")
    args = parser.parse_args()
    run(args.period, args.local_dir)
