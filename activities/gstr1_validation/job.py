"""
=====================================================================================================
JOB      : GSTR-1 Validation                                                      (Activity 1)
CLIENT   : Swiggy
RUNS     : Once per batch (B1, B2, ... as listed in the month's manifest), in two stages:
             stage "raw"       - on the sales register exactly as the client uploaded it
             stage "corrected" - again, after Activity 2 has applied the client's corrections
-----------------------------------------------------------------------------------------------------
WHAT IT DOES
  Checks every line of the sales register against the GST rules in RULES (see CONFIGURATION).
  Each line is marked "Success" or "Error" and gets a remark naming every rule it failed,
  e.g. "R07 Receiver GSTIN invalid ...: 29AADCB2230 | R15 Tax split wrong ...".

STEPS
  1  Read the sales register
  2  Check all mandatory columns are present and rename them
  3  Clean the values (trim, upper case, blank -> empty) and convert numbers and dates
  4  Work out the tax we expect on each line
  5  Check every rule in RULES
  6  Status, error count and remark
  7  Final list of output columns
  8  Write the outputs

INPUT
  stage raw       : raw/<period>/gstr1/<batch>/gstr1_<batch>.xlsx                 (sheet "Sales Register")
  stage corrected : intermediate/gstr1_corrected/period=<period>/<batch>.parquet  (from Activity 2)

OUTPUT
  stage raw       : intermediate/gstr1_validated/period=<period>/<batch>.parquet  every line + status
                    final/<period>/gstr1/<batch>_errors_for_client.xlsx           error lines only
  stage corrected : intermediate/gstr1_final/period=<period>/<batch>.parquet      every line + status
                    final/<period>/gstr1/<batch>_errors_after_correction.xlsx     error lines only

RECORD ID
  Every line arrives with a Record ID such as  sales-202506-b2b-1  (source-period-type-number).
  The ID is never changed, so the same line can be looked up in every later file and in Athena.

QUICK CHANGES
  Add / change / remove a rule ......... RULES          (one entry per rule, nothing else to edit)
  Client renamed a column header ....... COLUMN_MAP     (left side = header in the client's file)
  New valid code / rate / state ........ VALID_... lists
  Rounding tolerance ................... TOLERANCE_RS
  New output column .................... STEP 7, then add it to athena/tables/gstr1_validated.sql
                                         and sales_combined.sql and run  python athena/athena.py setup

HOW TO RUN
  python -m activities.gstr1_validation.job --period 202506 --batch B1
  python -m activities.gstr1_validation.job --period 202506 --batch B1 --stage corrected
  Add  --local-dir <folder>  to read/write the same paths in a local folder instead of S3.
=====================================================================================================
"""
import argparse
import calendar
import io
import os
import re
from datetime import date

import boto3
import duckdb
import pandas as pd
import pyarrow.parquet as pq
from duckdb.typing import BOOLEAN, VARCHAR


# =====================================================================================================
# CONFIGURATION
# =====================================================================================================

# Column header in the client's sales register  ->  column name used in this job
COLUMN_MAP = {
    "Record ID":               "record_id",
    "Supplier GSTIN":          "supplier_gstin",
    "Document Type":           "document_type",
    "Invoice Type":            "invoice_type",
    "Invoice Number":          "invoice_number",
    "Invoice Date":            "invoice_date_text",
    "Receiver GSTIN":          "receiver_gstin",
    "Receiver Name":           "receiver_name",
    "Place of Supply":         "place_of_supply",
    "Reverse Charge":          "reverse_charge",
    "E-Commerce GSTIN":        "ecommerce_gstin",
    "HSN/SAC":                 "hsn_sac",
    "Item Description":        "item_description",
    "UQC":                     "uqc",
    "Quantity":                "quantity",
    "GST Rate":                "gst_rate",
    "Taxable Value":           "taxable_value",
    "IGST Amount":             "igst_amount",
    "CGST Amount":             "cgst_amount",
    "SGST Amount":             "sgst_amount",
    "Cess Amount":             "cess_amount",
    "Total Invoice Value":     "total_invoice_value",
    "Original Invoice Number": "original_invoice_number",
    "Original Invoice Date":   "original_invoice_date",
    "Export Type":             "export_type",
    "Port Code":               "port_code",
    "Shipping Bill Number":    "shipping_bill_number",
    "Shipping Bill Date":      "shipping_bill_date",
}

# Correction history columns. They are added by Activity 2 (stage "corrected");
# on the first run (stage "raw") they are added empty.
CORRECTION_COLUMN_MAP = {
    "Correction Applied": "correction_applied",
    "Corrected Fields":   "corrected_fields",
    "Correction Remark":  "correction_remark",
    "First Pass Status":  "first_pass_status",
    "First Pass Remark":  "first_pass_remark",
}

VALID_INVOICE_TYPES = "'B2B', 'B2C', 'CDNR', 'CDNUR', 'EXP'"
VALID_GST_RATES     = "0, 0.1, 0.25, 1, 1.5, 3, 5, 6, 7.5, 12, 18, 28"
VALID_UQC           = "'NA', 'NOS', 'PCS', 'KGS', 'GMS', 'LTR', 'MTR', 'BOX', 'SET', 'UNT', 'OTH'"
VALID_STATE_CODES   = ("'01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', "
                       "'15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '26', '27', '29', '30', "
                       "'31', '32', '33', '34', '35', '36', '37', '38', '96', '97'")
TOLERANCE_RS        = 1.0     # rounding tolerance (Rs.) for tax and total checks

# -----------------------------------------------------------------------------------------------------
# VALIDATION RULES - a line must pass every rule to be "Success".
#   code   : shown at the start of the error text
#   error  : error text for the client (do not use single quotes ' in it)
#   check  : SQL condition that is TRUE when the line is correct.
#            A missing value makes the check fail automatically.
#   show   : SQL value added after the error text (None = show nothing)
# Columns available to the checks: every column of STEP 3 plus pos_state_code, period_start,
# period_end, is_inter_state, expected_igst, expected_cgst_sgst (STEP 4).
# -----------------------------------------------------------------------------------------------------
RULES = [
    {"code": "R01", "error": "Record ID missing or duplicated",
     "check": "record_id IS NOT NULL AND count(*) OVER (PARTITION BY record_id) = 1",
     "show": "record_id"},
    {"code": "R02", "error": "Invalid supplier GSTIN",
     "check": "is_valid_gstin(supplier_gstin)",
     "show": "supplier_gstin"},
    {"code": "R03", "error": "Document type does not match invoice type (credit/debit notes need CRN/DBN, others INV)",
     "check": "CASE WHEN invoice_type IN ('CDNR', 'CDNUR') THEN document_type IN ('CRN', 'DBN') "
              "ELSE document_type = 'INV' END",
     "show": "document_type"},
    {"code": "R04", "error": "Invalid invoice type",
     "check": f"invoice_type IN ({VALID_INVOICE_TYPES})",
     "show": "invoice_type"},
    {"code": "R05", "error": "Invoice number missing, over 16 characters or has characters other than letters, digits, / and -",
     "check": "length(invoice_number) <= 16 AND regexp_full_match(invoice_number, '[A-Za-z0-9/-]+')",
     "show": "invoice_number"},
    {"code": "R06", "error": "Invoice date missing or outside the return period",
     "check": "invoice_date BETWEEN period_start AND period_end",
     "show": "invoice_date_text"},
    {"code": "R07", "error": "Receiver GSTIN invalid for B2B/CDNR, or filled for B2C/CDNUR/EXP",
     "check": "CASE WHEN invoice_type IN ('B2B', 'CDNR') THEN is_valid_gstin(receiver_gstin) "
              "ELSE receiver_gstin IS NULL END",
     "show": "receiver_gstin"},
    {"code": "R08", "error": "Receiver name missing for B2B/CDNR",
     "check": "invoice_type NOT IN ('B2B', 'CDNR') OR receiver_name IS NOT NULL",
     "show": None},
    {"code": "R09", "error": "Invalid place of supply (format 29-Karnataka, exports 96)",
     "check": f"pos_state_code IN ({VALID_STATE_CODES}) AND place_of_supply LIKE '__-%' "
              "AND (invoice_type <> 'EXP' OR pos_state_code = '96')",
     "show": "place_of_supply"},
    {"code": "R10", "error": "Reverse charge must be Y or N",
     "check": "reverse_charge IN ('Y', 'N')",
     "show": "reverse_charge"},
    {"code": "R11", "error": "HSN/SAC must be 6 or 8 digits",
     "check": "regexp_full_match(hsn_sac, '[0-9]{6}|[0-9]{8}')",
     "show": "hsn_sac"},
    {"code": "R12", "error": "Invalid UQC, or quantity missing for goods",
     "check": f"uqc IN ({VALID_UQC}) AND (hsn_sac LIKE '99%' OR quantity > 0)",
     "show": "uqc"},
    {"code": "R13", "error": "Invalid GST rate",
     "check": f"gst_rate IN ({VALID_GST_RATES})",
     "show": "gst_rate"},
    {"code": "R14", "error": "Taxable value must be more than zero",
     "check": "taxable_value > 0",
     "show": "taxable_value"},
    {"code": "R15", "error": "Tax split wrong for place of supply",
     "check": f"abs(igst_amount - expected_igst) <= {TOLERANCE_RS} "
              f"AND abs(cgst_amount - expected_cgst_sgst) <= {TOLERANCE_RS} "
              f"AND abs(sgst_amount - expected_cgst_sgst) <= {TOLERANCE_RS}",
     "show": "'expected IGST ' || expected_igst || ', CGST and SGST ' || expected_cgst_sgst || ' each'"},
    {"code": "R16", "error": "Total invoice value is not taxable value + taxes + cess",
     "check": f"abs(total_invoice_value - (taxable_value + igst_amount + cgst_amount + sgst_amount "
              f"+ cess_amount)) <= {TOLERANCE_RS}",
     "show": "total_invoice_value"},
    {"code": "R17", "error": "Original invoice number or date missing on credit/debit note",
     "check": "invoice_type NOT IN ('CDNR', 'CDNUR') "
              "OR (original_invoice_number IS NOT NULL AND original_invoice_date IS NOT NULL)",
     "show": None},
    {"code": "R18", "error": "Export type (WPAY/WOPAY) or 6 character port code missing",
     "check": "invoice_type <> 'EXP' OR (export_type IN ('WPAY', 'WOPAY') AND length(port_code) = 6)",
     "show": "port_code"},
]


# =====================================================================================================
# GSTIN CHECK  (format + check digit)
# =====================================================================================================
GSTIN_PATTERN = re.compile(r"^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][1-9A-Z]Z[0-9A-Z]$")
GSTIN_CHARS   = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def is_valid_gstin(gstin) -> bool:
    """
    A GSTIN is valid when it has the right pattern AND its 15th character matches the
    check digit calculated from the first 14 characters (the GSTN "mod 36" method).
    Example of a valid GSTIN: 27AAPFU0939F1ZV
    """
    if not gstin or not GSTIN_PATTERN.match(gstin):
        return False
    total = 0
    for position in range(14):
        value = GSTIN_CHARS.index(gstin[position])
        if position % 2 == 1:              # every second character counts double
            value = value * 2
        total = total + value // 36 + value % 36
    check_char = GSTIN_CHARS[(36 - total % 36) % 36]
    return gstin[14] == check_char


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

    def write_bytes(self, key: str, data: bytes):
        if self.local_dir:
            path = os.path.join(self.local_dir, key)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(data)
        else:
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=data)

    def read_excel_as_text(self, key: str, sheet_name) -> pd.DataFrame:
        """Every cell is read as text (an empty cell becomes ""). Numbers and dates are converted later."""
        return pd.read_excel(io.BytesIO(self.read_bytes(key)), sheet_name=sheet_name, dtype=str, na_filter=False)

    def read_parquet(self, key: str) -> pd.DataFrame:
        return pq.read_table(io.BytesIO(self.read_bytes(key))).to_pandas()

    def write_parquet(self, table, key: str):
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        self.write_bytes(key, buffer.getvalue())

    def write_excel(self, sheets: dict, key: str):
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            for sheet_name, df in sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        self.write_bytes(key, buffer.getvalue())


# =====================================================================================================
# THE JOB
# =====================================================================================================
def run(period: str, batch_id: str, stage: str = "raw", local_dir: str = None) -> dict:
    store = Storage(local_dir)

    if stage == "raw":
        input_key  = f"raw/{period}/gstr1/{batch_id}/gstr1_{batch_id}.xlsx"
        output_key = f"intermediate/gstr1_validated/period={period}/{batch_id}.parquet"
        error_key  = f"final/{period}/gstr1/{batch_id}_errors_for_client.xlsx"
    elif stage == "corrected":
        input_key  = f"intermediate/gstr1_corrected/period={period}/{batch_id}.parquet"
        output_key = f"intermediate/gstr1_final/period={period}/{batch_id}.parquet"
        error_key  = f"final/{period}/gstr1/{batch_id}_errors_after_correction.xlsx"
    else:
        raise ValueError(f"stage must be 'raw' or 'corrected', not {stage!r}")

    # Return period, e.g. 202506 -> 2025-06-01 to 2025-06-30
    year         = int(period[:4])
    month        = int(period[4:])
    period_start = date(year, month, 1)
    period_end   = date(year, month, calendar.monthrange(year, month)[1])

    print(f"[GSTR1 validation] period={period} batch={batch_id} stage={stage}")
    print(f"  Input : {input_key}")

    # -------------------------------------------------------------------------------------------------
    # STEP 1 - Read the sales register (every cell as text)
    # -------------------------------------------------------------------------------------------------
    if stage == "raw":
        register = store.read_excel_as_text(input_key, sheet_name="Sales Register")
    else:
        register = store.read_parquet(input_key)
    print(f"  Lines read: {len(register)}")

    # -------------------------------------------------------------------------------------------------
    # STEP 2 - Check all mandatory columns are present, then rename them
    # -------------------------------------------------------------------------------------------------
    missing = [header for header in COLUMN_MAP if header not in register.columns]
    if missing:
        raise ValueError(f"Sales register is missing mandatory columns: {missing}")

    for header in CORRECTION_COLUMN_MAP:
        if header not in register.columns:
            register[header] = ""                 # first run: no correction history yet

    all_columns = {**COLUMN_MAP, **CORRECTION_COLUMN_MAP}
    register = register[list(all_columns)].rename(columns=all_columns)

    con = duckdb.connect()
    con.create_function("is_valid_gstin", is_valid_gstin, [VARCHAR], BOOLEAN, null_handling="special")
    con.register("register", register)

    # -------------------------------------------------------------------------------------------------
    # STEP 3 - Clean the values and convert numbers and dates
    #   NULLIF(trim(x), '')  : remove spaces; an empty cell becomes NULL (= missing)
    #   TRY_CAST(... DOUBLE) : text -> number; text that is not a number becomes NULL
    #   try_strptime         : text -> date (dd/mm/yyyy or yyyy-mm-dd); anything else becomes NULL
    # -------------------------------------------------------------------------------------------------
    con.execute(f"""
        CREATE TABLE cleaned AS
        SELECT
            CAST(row_number() OVER () + 1 AS INTEGER)                    AS source_row_number,  -- Excel row, row 1 = headers
            NULLIF(trim(record_id), '')                                  AS record_id,
            upper(NULLIF(trim(supplier_gstin), ''))                      AS supplier_gstin,
            upper(NULLIF(trim(document_type), ''))                       AS document_type,
            upper(NULLIF(trim(invoice_type), ''))                        AS invoice_type,
            NULLIF(trim(invoice_number), '')                             AS invoice_number,
            NULLIF(trim(invoice_date_text), '')                          AS invoice_date_text,
            CAST(COALESCE(try_strptime(trim(invoice_date_text), '%d/%m/%Y'),
                          try_strptime(trim(invoice_date_text), '%Y-%m-%d')) AS DATE) AS invoice_date,
            upper(NULLIF(trim(receiver_gstin), ''))                      AS receiver_gstin,
            NULLIF(trim(receiver_name), '')                              AS receiver_name,
            NULLIF(trim(place_of_supply), '')                            AS place_of_supply,
            left(trim(place_of_supply), 2)                               AS pos_state_code,
            upper(NULLIF(trim(reverse_charge), ''))                      AS reverse_charge,
            upper(NULLIF(trim(ecommerce_gstin), ''))                     AS ecommerce_gstin,
            NULLIF(trim(hsn_sac), '')                                    AS hsn_sac,
            NULLIF(trim(item_description), '')                           AS item_description,
            upper(NULLIF(trim(uqc), ''))                                 AS uqc,
            TRY_CAST(NULLIF(trim(quantity), '') AS DOUBLE)               AS quantity,
            TRY_CAST(NULLIF(trim(gst_rate), '') AS DOUBLE)               AS gst_rate,
            TRY_CAST(NULLIF(trim(taxable_value), '') AS DOUBLE)          AS taxable_value,
            COALESCE(TRY_CAST(NULLIF(trim(igst_amount), '') AS DOUBLE), 0) AS igst_amount,
            COALESCE(TRY_CAST(NULLIF(trim(cgst_amount), '') AS DOUBLE), 0) AS cgst_amount,
            COALESCE(TRY_CAST(NULLIF(trim(sgst_amount), '') AS DOUBLE), 0) AS sgst_amount,
            COALESCE(TRY_CAST(NULLIF(trim(cess_amount), '') AS DOUBLE), 0) AS cess_amount,
            TRY_CAST(NULLIF(trim(total_invoice_value), '') AS DOUBLE)    AS total_invoice_value,
            NULLIF(trim(original_invoice_number), '')                    AS original_invoice_number,
            NULLIF(trim(original_invoice_date), '')                      AS original_invoice_date,
            upper(NULLIF(trim(export_type), ''))                         AS export_type,
            upper(NULLIF(trim(port_code), ''))                           AS port_code,
            NULLIF(trim(shipping_bill_number), '')                       AS shipping_bill_number,
            NULLIF(trim(shipping_bill_date), '')                         AS shipping_bill_date,
            trim(correction_applied) = 'Y'                               AS correction_applied,
            NULLIF(trim(corrected_fields), '')                           AS corrected_fields,
            NULLIF(trim(correction_remark), '')                          AS correction_remark,
            NULLIF(trim(first_pass_status), '')                          AS first_pass_status,
            NULLIF(trim(first_pass_remark), '')                          AS first_pass_remark,
            DATE '{period_start}'                                        AS period_start,
            DATE '{period_end}'                                          AS period_end
        FROM register
    """)

    # -------------------------------------------------------------------------------------------------
    # STEP 4 - The tax we expect on each line
    #   inter-state (place of supply state <> supplier state, or export) -> all tax is IGST
    #   intra-state (same state)                                         -> half CGST + half SGST
    #   export without payment of tax (WOPAY)                            -> no tax
    # -------------------------------------------------------------------------------------------------
    con.execute("""
        CREATE TABLE with_supply_type AS
        SELECT *,
            invoice_type = 'EXP' OR pos_state_code <> left(supplier_gstin, 2)         AS is_inter_state
        FROM cleaned
    """)
    con.execute("""
        CREATE TABLE with_expected_tax AS
        SELECT *,
            CASE WHEN export_type = 'WOPAY' THEN 0
                 WHEN is_inter_state        THEN round(taxable_value * gst_rate / 100, 2)
                 ELSE 0 END                                                           AS expected_igst,
            CASE WHEN is_inter_state        THEN 0
                 ELSE round(taxable_value * gst_rate / 200, 2) END                    AS expected_cgst_sgst
        FROM with_supply_type
    """)

    # -------------------------------------------------------------------------------------------------
    # STEP 5 - Check every rule: one TRUE/FALSE column per rule, e.g. r07_ok (TRUE = passed)
    # -------------------------------------------------------------------------------------------------
    rule_columns = []
    for rule in RULES:
        rule_columns.append(f"COALESCE({rule['check']}, false) AS {rule['code'].lower()}_ok")

    con.execute(f"""
        CREATE TABLE checked AS
        SELECT *, {", ".join(rule_columns)}
        FROM with_expected_tax
    """)

    # -------------------------------------------------------------------------------------------------
    # STEP 6 - Error count and remark (the remark lists every failed rule, separated by " | ")
    # -------------------------------------------------------------------------------------------------
    error_counts = []
    error_texts  = []
    for rule in RULES:
        ok_column = f"{rule['code'].lower()}_ok"
        text      = f"{rule['code']} {rule['error']}"
        error_counts.append(f"CAST(NOT {ok_column} AS INTEGER)")
        if rule["show"]:
            error_texts.append(f"CASE WHEN NOT {ok_column} THEN '{text}: ' || "
                               f"coalesce(CAST({rule['show']} AS VARCHAR), '(blank)') END")
        else:
            error_texts.append(f"CASE WHEN NOT {ok_column} THEN '{text}' END")

    con.execute(f"""
        CREATE TABLE validated AS
        SELECT *,
            {" + ".join(error_counts)}                 AS error_count,
            concat_ws(' | ', {", ".join(error_texts)}) AS failed_rules
        FROM checked
    """)

    # -------------------------------------------------------------------------------------------------
    # STEP 7 - Final list of output columns (order and types are fixed - Athena reads them)
    # -------------------------------------------------------------------------------------------------
    result = con.execute(f"""
        SELECT
            record_id,
            '{batch_id}'                                                  AS batch_id,
            source_row_number,
            supplier_gstin, document_type, invoice_type, invoice_number,
            invoice_date, invoice_date_text,
            receiver_gstin, receiver_name, place_of_supply, reverse_charge, ecommerce_gstin,
            hsn_sac, item_description, uqc, quantity, gst_rate,
            taxable_value, igst_amount, cgst_amount, sgst_amount, cess_amount,
            total_invoice_value,
            original_invoice_number, original_invoice_date,
            export_type, port_code, shipping_bill_number, shipping_bill_date,
            CASE WHEN error_count = 0 THEN 'Success' ELSE 'Error' END     AS validation_status,
            CASE WHEN error_count = 0 THEN 'All checks passed' ELSE failed_rules END AS validation_remark,
            CAST(error_count AS INTEGER)                                  AS error_count,
            -- correction history; on the first run, the first-pass result is this result
            COALESCE(correction_applied, false)                           AS correction_applied,
            corrected_fields,
            correction_remark,
            COALESCE(first_pass_status,
                     CASE WHEN error_count = 0 THEN 'Success' ELSE 'Error' END) AS first_pass_status,
            COALESCE(first_pass_remark,
                     CASE WHEN error_count = 0 THEN 'All checks passed' ELSE failed_rules END) AS first_pass_remark,
            CAST(now() AS TIMESTAMP)                                      AS processed_at
        FROM validated
        ORDER BY source_row_number
    """).arrow()
    con.close()

    # -------------------------------------------------------------------------------------------------
    # STEP 8 - Write every line to parquet (next jobs + Athena) and the error lines to Excel (client)
    # -------------------------------------------------------------------------------------------------
    store.write_parquet(result, output_key)

    all_lines = result.to_pandas()
    errors    = all_lines[all_lines["validation_status"] == "Error"].copy()
    errors["invoice_date"] = pd.to_datetime(errors["invoice_date"]).dt.strftime("%d/%m/%Y")
    errors["processed_at"] = errors["processed_at"].dt.strftime("%d/%m/%Y %H:%M")
    store.write_excel({"Errors": errors}, error_key)

    success_count = int((all_lines["validation_status"] == "Success").sum())
    print(f"  Result: {success_count} success, {len(errors)} errors")
    print(f"  Output: {output_key}")
    print(f"          {error_key}")
    return {"lines": len(all_lines), "success": success_count, "errors": len(errors)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GSTR-1 validation for one batch")
    parser.add_argument("--period", required=True, help="return period YYYYMM, e.g. 202506")
    parser.add_argument("--batch", required=True, help="batch id, e.g. B1")
    parser.add_argument("--stage", default="raw", choices=["raw", "corrected"])
    parser.add_argument("--local-dir", help="use this local folder instead of S3")
    args = parser.parse_args()
    run(args.period, args.batch, args.stage, args.local_dir)
