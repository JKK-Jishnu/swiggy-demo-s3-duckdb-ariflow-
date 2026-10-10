"""
=====================================================================================================
JOB      : GSTR-2B vs Purchase Register Reconciliation                            (Activity 9)
CLIENT   : Swiggy
RUNS     : Once per month, after PR Validation (Activity 7) and 2B Consolidation (Activity 8)
-----------------------------------------------------------------------------------------------------
WHAT IT DOES
  Input tax credit (ITC) can only be claimed for invoices the supplier has reported in GSTR-2B.
  This job matches what suppliers reported (GSTR-2B) against what Swiggy booked (purchase
  register) and shows the ITC that is at risk.

  LEFT  = GSTR-2B lines (all)
  RIGHT = Purchase register lines that passed validation (status Success)

  Matching ladder - each level only looks at rows NOT matched by an earlier level.
  All levels require the same Swiggy GSTIN (recipient) and the same supplier GSTIN.
    L1 Exact               + invoice number + taxable value (within Rs.1)
    L2 Amount mismatch     + invoice number                 -> taxable values differ
    L3 Invoice no mismatch + taxable value (within Rs.1)    -> invoice numbers differ
    Only in 2B             supplier reported it, Swiggy has not booked it
    Only in PR             Swiggy booked it, supplier has not reported it  -> ITC at risk
  Invoice numbers are compared after removing "/", "-" and spaces and ignoring case.
  Each row is matched at most once; if several rows qualify, the closest amount wins.

  ITC at risk (column itc_at_risk)
    Only in PR       : all ITC claimed on the line
    L2 / L3          : ITC claimed above the tax shown in 2B (if any)
    L1 / Only in 2B  : 0

ALTERYX EQUIVALENT
  Input Data (2B) + Input Data (PR) -> Filter -> Formula (normalise invoice no)
  -> Join (level 1) -> unjoined L/R into Join (level 2) -> unjoined into Join (level 3)
  -> Union (J + L + R) -> Formula (ITC at risk) -> Summarize -> Output Data

INPUT
  intermediate/gstr2b_consolidated/period=<period>/gstr2b_consolidated.parquet   (Activity 8)
  intermediate/pr_validated/period=<period>/pr_validated.parquet                 (Activity 7)

OUTPUT
  intermediate/gstr2b_vs_pr_reco/period=<period>/gstr2b_vs_pr_reco.parquet       (Athena table)
  final/<period>/gstr2b_vs_pr_reco.xlsx                                          Summary + Reco Details

RECORD ID
  2B lines carry IDs like 2b-202506-10, purchase register lines like pr-202506-1.
  Every reco row shows BOTH ids. The Summary sheet lists all ids behind each count, joined with " | ".

HOW TO RUN
  python -m activities.gstr2b_vs_pr_reco.job --period 202506
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
    gstr2b_key = f"intermediate/gstr2b_consolidated/period={period}/gstr2b_consolidated.parquet"
    pr_key     = f"intermediate/pr_validated/period={period}/pr_validated.parquet"
    reco_key   = f"intermediate/gstr2b_vs_pr_reco/period={period}/gstr2b_vs_pr_reco.parquet"
    excel_key  = f"final/{period}/gstr2b_vs_pr_reco.xlsx"

    print(f"[GSTR-2B vs PR reco] period={period}")

    # -------------------------------------------------------------------------------------------------
    # STEP 1 - INPUT
    # -------------------------------------------------------------------------------------------------
    con = duckdb.connect()
    con.register("gstr2b", store.read_parquet(gstr2b_key))
    con.register("pr", store.read_parquet(pr_key))

    # -------------------------------------------------------------------------------------------------
    # STEP 2 - FILTER + FORMULA: the two sides of the reconciliation, in the same shape
    #   invoice_key = invoice number without "/", "-", spaces, in upper case
    # -------------------------------------------------------------------------------------------------
    con.execute("""
        CREATE TABLE left_side AS                      -- LEFT = GSTR-2B (what suppliers reported)
        SELECT record_id, recipient_gstin, supplier_gstin, supplier_name,
               document_number AS invoice_number,
               upper(regexp_replace(document_number, '[^0-9A-Za-z]', '', 'g')) AS invoice_key,
               document_date AS invoice_date, taxable_value AS amount,
               igst_amount + cgst_amount + sgst_amount + cess_amount           AS tax
        FROM gstr2b
    """)
    con.execute("""
        CREATE TABLE right_side AS                     -- RIGHT = purchase register (validated lines only)
        SELECT record_id, recipient_gstin, supplier_gstin, supplier_legal_name AS supplier_name,
               invoice_number,
               upper(regexp_replace(invoice_number, '[^0-9A-Za-z]', '', 'g')) AS invoice_key,
               invoice_date, taxable_value AS amount, itc_total AS itc_claimed
        FROM pr
        WHERE validation_status = 'Success'
    """)
    left_count, right_count = (con.execute("SELECT count(*) FROM left_side").fetchone()[0],
                               con.execute("SELECT count(*) FROM right_side").fetchone()[0])
    print(f"  GSTR-2B lines: {left_count} | purchase register lines (validated): {right_count}")

    # -------------------------------------------------------------------------------------------------
    # STEP 3 - JOIN LADDER: level 1, 2, 3 (each on what is still unmatched)
    # -------------------------------------------------------------------------------------------------
    same_parties = "l.recipient_gstin = r.recipient_gstin AND l.supplier_gstin = r.supplier_gstin"
    con.execute("CREATE TABLE matches (left_record_id VARCHAR, right_record_id VARCHAR, level_no INTEGER, match_level VARCHAR)")
    match_level(con, 1, "L1 Exact",
                f"{same_parties} AND l.invoice_key = r.invoice_key AND abs(l.amount - r.amount) <= {AMOUNT_TOLERANCE_RS}")
    match_level(con, 2, "L2 Amount mismatch",
                f"{same_parties} AND l.invoice_key = r.invoice_key")
    match_level(con, 3, "L3 Invoice no mismatch",
                f"{same_parties} AND abs(l.amount - r.amount) <= {AMOUNT_TOLERANCE_RS}")

    # -------------------------------------------------------------------------------------------------
    # STEP 4 - UNION: matched pairs (J) + 2B only (L) + PR only (R)
    # -------------------------------------------------------------------------------------------------
    con.execute("""
        CREATE TABLE reco_union AS
        SELECT m.match_level, m.level_no,
               l.record_id AS gstr2b_record_id, r.record_id AS pr_record_id,
               l.recipient_gstin, l.supplier_gstin, COALESCE(r.supplier_name, l.supplier_name) AS supplier_name,
               l.invoice_number AS gstr2b_invoice_number, r.invoice_number AS pr_invoice_number,
               l.invoice_date AS gstr2b_invoice_date, r.invoice_date AS pr_invoice_date,
               l.amount AS gstr2b_taxable_value, r.amount AS pr_taxable_value,
               l.tax AS gstr2b_tax, r.itc_claimed AS pr_itc_claimed
        FROM matches m
        JOIN left_side  l ON l.record_id = m.left_record_id
        JOIN right_side r ON r.record_id = m.right_record_id

        UNION ALL
        SELECT 'Only in 2B', 4, l.record_id, NULL, l.recipient_gstin, l.supplier_gstin, l.supplier_name,
               l.invoice_number, NULL, l.invoice_date, NULL, l.amount, NULL, l.tax, NULL
        FROM left_side l WHERE l.record_id NOT IN (SELECT left_record_id FROM matches)

        UNION ALL
        SELECT 'Only in PR', 5, NULL, r.record_id, r.recipient_gstin, r.supplier_gstin, r.supplier_name,
               NULL, r.invoice_number, NULL, r.invoice_date, NULL, r.amount, NULL, r.itc_claimed
        FROM right_side r WHERE r.record_id NOT IN (SELECT right_record_id FROM matches)
    """)

    # -------------------------------------------------------------------------------------------------
    # STEP 5 - FORMULA: differences, ITC at risk and a remark per row
    # -------------------------------------------------------------------------------------------------
    reco = con.execute("""
        SELECT match_level, CAST(level_no AS INTEGER) AS level_no,
               gstr2b_record_id, pr_record_id, recipient_gstin, supplier_gstin, supplier_name,
               gstr2b_invoice_number, pr_invoice_number,
               CAST(gstr2b_invoice_date AS DATE) AS gstr2b_invoice_date,
               CAST(pr_invoice_date AS DATE)     AS pr_invoice_date,
               gstr2b_taxable_value, pr_taxable_value,
               round(COALESCE(gstr2b_taxable_value, 0) - COALESCE(pr_taxable_value, 0), 2) AS taxable_value_difference,
               round(gstr2b_tax, 2)      AS gstr2b_tax,
               round(pr_itc_claimed, 2)  AS pr_itc_claimed,
               round(CASE WHEN level_no = 5      THEN pr_itc_claimed
                          WHEN level_no IN (2, 3) THEN greatest(pr_itc_claimed - gstr2b_tax, 0)
                          ELSE 0 END, 2)  AS itc_at_risk,
               CASE level_no
                   WHEN 1 THEN 'Matched'
                   WHEN 2 THEN 'Taxable value differs by Rs.' || CAST(round(gstr2b_taxable_value - pr_taxable_value, 2) AS VARCHAR)
                   WHEN 3 THEN 'Invoice number differs: 2B ' || gstr2b_invoice_number || ' vs PR ' || pr_invoice_number
                   WHEN 4 THEN 'Reported by supplier in GSTR-2B but not booked in purchase register'
                   WHEN 5 THEN 'Booked in purchase register but not reported by supplier in GSTR-2B - ITC at risk'
               END AS remark
        FROM reco_union
        ORDER BY level_no, COALESCE(pr_record_id, gstr2b_record_id)
    """).arrow()
    con.register("reco_result", reco)

    # -------------------------------------------------------------------------------------------------
    # STEP 6 - SUMMARIZE: counts, amounts and ITC at risk per match level, with every Record ID
    # -------------------------------------------------------------------------------------------------
    summary = con.execute("""
        SELECT match_level                                              AS "Match Level",
               count(*)                                                 AS "Records",
               round(sum(COALESCE(gstr2b_taxable_value, 0)), 2)         AS "2B Taxable Value",
               round(sum(COALESCE(pr_taxable_value, 0)), 2)             AS "PR Taxable Value",
               round(sum(taxable_value_difference), 2)                  AS "Difference",
               round(sum(itc_at_risk), 2)                               AS "ITC at Risk",
               string_agg(gstr2b_record_id, ' | ' ORDER BY gstr2b_record_id) AS "2B Record IDs",
               string_agg(pr_record_id,     ' | ' ORDER BY pr_record_id)     AS "PR Record IDs"
        FROM reco_result
        GROUP BY match_level, level_no
        ORDER BY level_no
    """).df()
    con.close()

    # -------------------------------------------------------------------------------------------------
    # STEP 7 - OUTPUT
    # -------------------------------------------------------------------------------------------------
    store.write_parquet(reco, reco_key)
    details = reco.to_pandas()
    for column in ["gstr2b_invoice_date", "pr_invoice_date"]:
        details[column] = pd.to_datetime(details[column]).dt.strftime("%d/%m/%Y")
    store.write_excel({"Summary": summary, "Reco Details": details}, excel_key)

    print(summary[["Match Level", "Records", "ITC at Risk"]].to_string(index=False))
    print(f"  Output: {reco_key}")
    print(f"          {excel_key}")
    return {**{level: int(count) for level, count in zip(summary["Match Level"], summary["Records"])},
            "itc_at_risk": float(summary["ITC at Risk"].sum())}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reconcile GSTR-2B against the purchase register")
    parser.add_argument("--period", required=True, help="return period YYYYMM, e.g. 202506")
    parser.add_argument("--local-dir", help="use this local folder instead of S3")
    args = parser.parse_args()
    run(args.period, args.local_dir)
