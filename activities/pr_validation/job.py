"""
=====================================================================================================
JOB      : Purchase Register (PR) Validation                                      (Activity 7)
CLIENT   : Swiggy
RUNS     : Once per month (PR / GSTR-2B track). Runs in parallel with Activity 8.
-----------------------------------------------------------------------------------------------------
WHAT IT DOES
  Checks every line of Swiggy's purchase register against the GST rules in RULES (see
  CONFIGURATION) before the register is reconciled with GSTR-2B (Activity 9). Each line is marked
  "Success" or "Error" with a remark naming every rule it failed. Error lines go back to the client
  to correct; only "Success" lines are reconciled.

STEPS
  1  Read the purchase register
  2  Check all mandatory columns are present and rename them
  3  Clean the values (trim, upper case, blank -> empty) and convert numbers and dates
  4  Work out the tax we expect on each line
  5  Check every rule in RULES
  6  Status, error count and remark
  7  Final list of output columns
  8  Write the outputs

INPUT
  raw/<period>/pr/purchase_register.xlsx                         sheet "Purchase Register"

OUTPUT
  intermediate/pr_validated/period=<period>/pr_validated.parquet every line + status (Athena)
  final/<period>/pr_errors_for_client.xlsx                       error lines only

RECORD ID
  Every line arrives with a Record ID such as  pr-202506-1. It is never changed, so the same
  line can be looked up in the 2B reconciliation and in Athena.

QUICK CHANGES
  Add / change / remove a rule ......... RULES          (one entry per rule, nothing else to edit)
  Client renamed a column header ....... COLUMN_MAP     (left side = header in the client's file)
  New Swiggy registration .............. SWIGGY_GSTINS
  New valid code / rate / state ........ VALID_... lists
  Rounding tolerance ................... TOLERANCE_RS
  New output column .................... STEP 7, then add it to athena/tables/pr_validated.sql
                                         and run  python athena/athena.py setup

HOW TO RUN
  python -m activities.pr_validation.job --period 202506
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

# Column header in the client's purchase register  ->  column name used in this job
COLUMN_MAP = {
    "Record ID":               "record_id",
    "Recipient GSTIN":         "recipient_gstin",
    "Supplier GSTIN":          "supplier_gstin",
    "Supplier Legal Name":     "supplier_legal_name",
    "Document Type":           "document_type",
    "Invoice Type":            "invoice_type",
    "Invoice Number":          "invoice_number",
    "Invoice Date":            "invoice_date_text",
    "Place of Supply":         "place_of_supply",
    "Reverse Charge":          "reverse_charge",
    "HSN/SAC":                 "hsn_sac",
    "Item Description":        "item_description",
    "GST Rate":                "gst_rate",
    "Taxable Value":           "taxable_value",
    "IGST Amount":             "igst_amount",
    "CGST Amount":             "cgst_amount",
    "SGST Amount":             "sgst_amount",
    "Cess Amount":             "cess_amount",
    "Total Invoice Value":     "total_invoice_value",
    "ITC Eligibility":         "itc_eligibility",
    "ITC IGST":                "itc_igst",
    "ITC CGST":                "itc_cgst",
    "ITC SGST":                "itc_sgst",
    "ITC Cess":                "itc_cess",
    "Voucher Number":          "voucher_number",
    "Voucher Date":            "voucher_date",
    "Original Invoice Number": "original_invoice_number",
    "Original Invoice Date":   "original_invoice_date",
}

# Swiggy's own GST registrations that receive purchase invoices
SWIGGY_GSTINS = "'27AABCS1429B1ZU', '29AABCS1429B1ZQ'"

VALID_ITC_ELIGIBILITY = "'INPUTS', 'INPUT SERVICES', 'CAPITAL GOODS', 'INELIGIBLE'"
VALID_GST_RATES       = "0, 0.1, 0.25, 1, 1.5, 3, 5, 6, 7.5, 12, 18, 28"
VALID_STATE_CODES     = ("'01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', "
                         "'14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '26', '27', "
                         "'29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '97'")
TOLERANCE_RS          = 1.0     # rounding tolerance (Rs.) for tax, total and ITC checks

# -----------------------------------------------------------------------------------------------------
# VALIDATION RULES - a line must pass every rule to be "Success".
#   code   : shown at the start of the error text
#   error  : error text for the client (do not use single quotes ' in it)
#   check  : SQL condition that is TRUE when the line is correct.
#            A missing value makes the check fail automatically.
#   show   : SQL value added after the error text (None = show nothing)
# Columns available to the checks: every column of STEP 3 plus pos_state_code, period_end,
# expected_igst, expected_cgst_sgst (STEP 4).
# -----------------------------------------------------------------------------------------------------
RULES = [
    {"code": "P01", "error": "Record ID missing or duplicated",
     "check": "record_id IS NOT NULL AND count(*) OVER (PARTITION BY record_id) = 1",
     "show": "record_id"},
    {"code": "P02", "error": "Recipient GSTIN is not a Swiggy registration",
     "check": f"recipient_gstin IN ({SWIGGY_GSTINS})",
     "show": "recipient_gstin"},
    {"code": "P03", "error": "Invalid supplier GSTIN",
     "check": "is_valid_gstin(supplier_gstin)",
     "show": "supplier_gstin"},
    {"code": "P04", "error": "Document type does not match invoice type (CDNR needs CRN/DBN, B2B needs INV)",
     "check": "CASE WHEN invoice_type = 'CDNR' THEN document_type IN ('CRN', 'DBN') "
              "ELSE document_type = 'INV' END",
     "show": "document_type"},
    {"code": "P05", "error": "Invoice type must be B2B or CDNR",
     "check": "invoice_type IN ('B2B', 'CDNR')",
     "show": "invoice_type"},
    {"code": "P06", "error": "Invoice number missing, over 16 characters or has characters other than letters, digits, / and -",
     "check": "length(invoice_number) <= 16 AND regexp_full_match(invoice_number, '[A-Za-z0-9/-]+')",
     "show": "invoice_number"},
    {"code": "P07", "error": "Invoice date missing or after the return period",
     "check": "invoice_date <= period_end",
     "show": "invoice_date_text"},
    {"code": "P08", "error": "Invalid place of supply (format 27-Maharashtra)",
     "check": f"pos_state_code IN ({VALID_STATE_CODES}) AND place_of_supply LIKE '__-%'",
     "show": "place_of_supply"},
    {"code": "P09", "error": "Reverse charge must be Y or N",
     "check": "reverse_charge IN ('Y', 'N')",
     "show": "reverse_charge"},
    {"code": "P10", "error": "HSN/SAC must be 4, 6 or 8 digits",
     "check": "regexp_full_match(hsn_sac, '[0-9]{4}|[0-9]{6}|[0-9]{8}')",
     "show": "hsn_sac"},
    {"code": "P11", "error": "Invalid GST rate",
     "check": f"gst_rate IN ({VALID_GST_RATES})",
     "show": "gst_rate"},
    {"code": "P12", "error": "Taxable value must be more than zero",
     "check": "taxable_value > 0",
     "show": "taxable_value"},
    {"code": "P13", "error": "Tax split wrong for supplier state and place of supply",
     "check": f"abs(igst_amount - expected_igst) <= {TOLERANCE_RS} "
              f"AND abs(cgst_amount - expected_cgst_sgst) <= {TOLERANCE_RS} "
              f"AND abs(sgst_amount - expected_cgst_sgst) <= {TOLERANCE_RS}",
     "show": "'expected IGST ' || expected_igst || ', CGST and SGST ' || expected_cgst_sgst || ' each'"},
    {"code": "P14", "error": "Total invoice value is not taxable value + taxes + cess",
     "check": f"abs(total_invoice_value - (taxable_value + igst_amount + cgst_amount + sgst_amount "
              f"+ cess_amount)) <= {TOLERANCE_RS}",
     "show": "total_invoice_value"},
    {"code": "P15", "error": "ITC eligibility must be Inputs, Input Services, Capital Goods or Ineligible",
     "check": f"upper(itc_eligibility) IN ({VALID_ITC_ELIGIBILITY})",
     "show": "itc_eligibility"},
    {"code": "P16", "error": "ITC claimed is more than the tax charged, or claimed on an ineligible line",
     "check": f"CASE WHEN upper(itc_eligibility) = 'INELIGIBLE' "
              f"THEN itc_igst + itc_cgst + itc_sgst + itc_cess = 0 "
              f"ELSE itc_igst <= igst_amount + {TOLERANCE_RS} AND itc_cgst <= cgst_amount + {TOLERANCE_RS} "
              f"AND itc_sgst <= sgst_amount + {TOLERANCE_RS} AND itc_cess <= cess_amount + {TOLERANCE_RS} END",
     "show": "'ITC ' || (itc_igst + itc_cgst + itc_sgst + itc_cess) || ' vs tax ' || "
             "(igst_amount + cgst_amount + sgst_amount + cess_amount)"},
    {"code": "P17", "error": "Original invoice number or date missing on credit/debit note",
     "check": "invoice_type <> 'CDNR' "
              "OR (original_invoice_number IS NOT NULL AND original_invoice_date IS NOT NULL)",
     "show": None},
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
def run(period: str, local_dir: str = None) -> dict:
    store = Storage(local_dir)
    input_key  = f"raw/{period}/pr/purchase_register.xlsx"
    output_key = f"intermediate/pr_validated/period={period}/pr_validated.parquet"
    error_key  = f"final/{period}/pr_errors_for_client.xlsx"

    # Last day of the return period, e.g. 202506 -> 2025-06-30
    year       = int(period[:4])
    month      = int(period[4:])
    period_end = date(year, month, calendar.monthrange(year, month)[1])

    print(f"[PR validation] period={period}")

    # -------------------------------------------------------------------------------------------------
    # STEP 1 - Read the purchase register (every cell as text)
    # -------------------------------------------------------------------------------------------------
    register = store.read_excel_as_text(input_key, sheet_name="Purchase Register")
    print(f"  Lines read: {len(register)}")

    # -------------------------------------------------------------------------------------------------
    # STEP 2 - Check all mandatory columns are present, then rename them
    # -------------------------------------------------------------------------------------------------
    missing = [header for header in COLUMN_MAP if header not in register.columns]
    if missing:
        raise ValueError(f"Purchase register is missing mandatory columns: {missing}")
    register = register[list(COLUMN_MAP)].rename(columns=COLUMN_MAP)

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
            CAST(row_number() OVER () + 1 AS INTEGER)                        AS source_row_number,  -- Excel row, row 1 = headers
            NULLIF(trim(record_id), '')                                      AS record_id,
            upper(NULLIF(trim(recipient_gstin), ''))                         AS recipient_gstin,
            upper(NULLIF(trim(supplier_gstin), ''))                          AS supplier_gstin,
            NULLIF(trim(supplier_legal_name), '')                            AS supplier_legal_name,
            upper(NULLIF(trim(document_type), ''))                           AS document_type,
            upper(NULLIF(trim(invoice_type), ''))                            AS invoice_type,
            NULLIF(trim(invoice_number), '')                                 AS invoice_number,
            NULLIF(trim(invoice_date_text), '')                              AS invoice_date_text,
            CAST(COALESCE(try_strptime(trim(invoice_date_text), '%d/%m/%Y'),
                          try_strptime(trim(invoice_date_text), '%Y-%m-%d')) AS DATE) AS invoice_date,
            NULLIF(trim(place_of_supply), '')                                AS place_of_supply,
            left(trim(place_of_supply), 2)                                   AS pos_state_code,
            upper(NULLIF(trim(reverse_charge), ''))                          AS reverse_charge,
            NULLIF(trim(hsn_sac), '')                                        AS hsn_sac,
            NULLIF(trim(item_description), '')                               AS item_description,
            TRY_CAST(NULLIF(trim(gst_rate), '') AS DOUBLE)                   AS gst_rate,
            TRY_CAST(NULLIF(trim(taxable_value), '') AS DOUBLE)              AS taxable_value,
            COALESCE(TRY_CAST(NULLIF(trim(igst_amount), '') AS DOUBLE), 0)   AS igst_amount,
            COALESCE(TRY_CAST(NULLIF(trim(cgst_amount), '') AS DOUBLE), 0)   AS cgst_amount,
            COALESCE(TRY_CAST(NULLIF(trim(sgst_amount), '') AS DOUBLE), 0)   AS sgst_amount,
            COALESCE(TRY_CAST(NULLIF(trim(cess_amount), '') AS DOUBLE), 0)   AS cess_amount,
            TRY_CAST(NULLIF(trim(total_invoice_value), '') AS DOUBLE)        AS total_invoice_value,
            NULLIF(trim(itc_eligibility), '')                                AS itc_eligibility,
            COALESCE(TRY_CAST(NULLIF(trim(itc_igst), '') AS DOUBLE), 0)      AS itc_igst,
            COALESCE(TRY_CAST(NULLIF(trim(itc_cgst), '') AS DOUBLE), 0)      AS itc_cgst,
            COALESCE(TRY_CAST(NULLIF(trim(itc_sgst), '') AS DOUBLE), 0)      AS itc_sgst,
            COALESCE(TRY_CAST(NULLIF(trim(itc_cess), '') AS DOUBLE), 0)      AS itc_cess,
            NULLIF(trim(voucher_number), '')                                 AS voucher_number,
            CAST(try_strptime(trim(voucher_date), '%d/%m/%Y') AS DATE)       AS voucher_date,
            NULLIF(trim(original_invoice_number), '')                        AS original_invoice_number,
            NULLIF(trim(original_invoice_date), '')                          AS original_invoice_date,
            DATE '{period_end}'                                              AS period_end
        FROM register
    """)

    # -------------------------------------------------------------------------------------------------
    # STEP 4 - The tax we expect on each line
    #   supplier state <> place of supply state  -> all tax is IGST
    #   same state                               -> half CGST + half SGST
    # -------------------------------------------------------------------------------------------------
    con.execute("""
        CREATE TABLE with_expected_tax AS
        SELECT *,
            CASE WHEN pos_state_code <> left(supplier_gstin, 2)
                 THEN round(taxable_value * gst_rate / 100, 2) ELSE 0 END    AS expected_igst,
            CASE WHEN pos_state_code <> left(supplier_gstin, 2)
                 THEN 0 ELSE round(taxable_value * gst_rate / 200, 2) END    AS expected_cgst_sgst
        FROM cleaned
    """)

    # -------------------------------------------------------------------------------------------------
    # STEP 5 - Check every rule: one TRUE/FALSE column per rule, e.g. p03_ok (TRUE = passed)
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
    result = con.execute("""
        SELECT
            record_id, source_row_number, recipient_gstin, supplier_gstin, supplier_legal_name,
            document_type, invoice_type, invoice_number, invoice_date, invoice_date_text,
            place_of_supply, reverse_charge, hsn_sac, item_description, gst_rate,
            taxable_value, igst_amount, cgst_amount, sgst_amount, cess_amount,
            round(igst_amount + cgst_amount + sgst_amount + cess_amount, 2)      AS total_tax,
            total_invoice_value,
            itc_eligibility, itc_igst, itc_cgst, itc_sgst, itc_cess,
            round(itc_igst + itc_cgst + itc_sgst + itc_cess, 2)                  AS itc_total,
            voucher_number, voucher_date, original_invoice_number, original_invoice_date,
            CASE WHEN error_count = 0 THEN 'Success' ELSE 'Error' END            AS validation_status,
            CASE WHEN error_count = 0 THEN 'All checks passed' ELSE failed_rules END AS validation_remark,
            CAST(error_count AS INTEGER)                                         AS error_count,
            CAST(now() AS TIMESTAMP)                                             AS processed_at
        FROM validated
        ORDER BY source_row_number
    """).arrow()
    con.close()

    # -------------------------------------------------------------------------------------------------
    # STEP 8 - Write every line to parquet (Activity 9 + Athena) and the error lines to Excel (client)
    # -------------------------------------------------------------------------------------------------
    store.write_parquet(result, output_key)

    all_lines = result.to_pandas()
    errors    = all_lines[all_lines["validation_status"] == "Error"].copy()
    errors["invoice_date"] = pd.to_datetime(errors["invoice_date"]).dt.strftime("%d/%m/%Y")
    errors["voucher_date"] = pd.to_datetime(errors["voucher_date"]).dt.strftime("%d/%m/%Y")
    errors["processed_at"] = errors["processed_at"].dt.strftime("%d/%m/%Y %H:%M")
    store.write_excel({"Errors": errors}, error_key)

    success_count = int((all_lines["validation_status"] == "Success").sum())
    print(f"  Result: {success_count} success, {len(errors)} errors")
    print(f"  Output: {output_key}")
    print(f"          {error_key}")
    return {"lines": len(all_lines), "success": success_count, "errors": len(errors)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate the purchase register for one month")
    parser.add_argument("--period", required=True, help="return period YYYYMM, e.g. 202506")
    parser.add_argument("--local-dir", help="use this local folder instead of S3")
    args = parser.parse_args()
    run(args.period, args.local_dir)
