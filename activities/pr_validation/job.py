"""
=====================================================================================================
JOB      : Purchase Register (PR) Validation                                      (Activity 7)
CLIENT   : Swiggy
RUNS     : Once per month (PR / GSTR-2B track). Runs in parallel with Activity 8.
-----------------------------------------------------------------------------------------------------
WHAT IT DOES
  Checks every line of Swiggy's purchase register against the GST rules below before the
  register is reconciled with GSTR-2B (Activity 9). Each line is marked "Success" or "Error"
  with a remark naming every rule it failed. Error lines go back to the client to correct;
  only "Success" lines are reconciled.

ALTERYX EQUIVALENT
  Input Data -> Select (rename + types) -> Data Cleansing -> Formula (one per rule)
  -> Formula (status + remark) -> Filter (Error rows) -> Output Data (x2)

INPUT
  raw/<period>/pr/purchase_register.xlsx                         sheet "Purchase Register"

OUTPUT
  intermediate/pr_validated/period=<period>/pr_validated.parquet every line + status (Athena)
  final/<period>/pr_errors_for_client.xlsx                       error lines only

RECORD ID
  Every line arrives with a Record ID such as  pr-202506-1. It is never changed, so the same
  line can be looked up in the 2B reconciliation and in Athena.

VALIDATION RULES  (a line must pass all of them to be "Success")
  P01  Record ID present and unique in the file
  P02  Recipient GSTIN is one of Swiggy's own registrations
  P03  Supplier GSTIN valid (15 characters, correct pattern, correct check digit)
  P04  Document type is INV / CRN / DBN and matches the invoice type (CDNR = CRN/DBN)
  P05  Invoice type is B2B or CDNR
  P06  Invoice number present, max 16 characters, only letters, digits, "/" and "-"
  P07  Invoice date valid and not after the end of the return period
       (earlier dates are allowed - ITC can be claimed for older invoices)
  P08  Place of supply is a valid state code ("27-Maharashtra")
  P09  Reverse charge is Y or N
  P10  HSN/SAC is 4, 6 or 8 digits
  P11  GST rate is one of the notified rates
  P12  Taxable value is greater than zero
  P13  Tax split matches: supplier state <> place of supply = IGST, same state = CGST + SGST (Rs.1)
  P14  Total invoice value = taxable value + all taxes + cess (Rs.1 tolerance)
  P15  ITC eligibility is Inputs / Input Services / Capital Goods / Ineligible
  P16  ITC claimed per tax head is not more than the tax charged; "Ineligible" lines claim no ITC
  P17  Credit / debit notes carry the original invoice number and date

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
import pyarrow as pa
import pyarrow.parquet as pq
from duckdb.typing import BOOLEAN, VARCHAR


# =====================================================================================================
# CONFIGURATION
# =====================================================================================================

# Purchase register headers (as the client sends them) -> column names used inside this job (Select tool)
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
SWIGGY_GSTINS = ["27AABCS1429B1ZU", "29AABCS1429B1ZQ"]

VALID_ITC_ELIGIBILITY = ["INPUTS", "INPUT SERVICES", "CAPITAL GOODS", "INELIGIBLE"]
VALID_GST_RATES       = [0, 0.1, 0.25, 1, 1.5, 3, 5, 6, 7.5, 12, 18, 28]
VALID_STATE_CODES     = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13",
                         "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "26", "27",
                         "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "97"]
TOLERANCE_RS          = 1.0


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
    for position, char in enumerate(gstin[:14]):
        factor  = 1 if position % 2 == 0 else 2          # weights alternate 1, 2, 1, 2 ...
        product = GSTIN_CHARS.index(char) * factor
        total  += product // 36 + product % 36
    check_char = GSTIN_CHARS[(36 - total % 36) % 36]
    return gstin[14] == check_char


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
# THE JOB
# =====================================================================================================
def run(period: str, local_dir: str = None) -> dict:
    store = Storage(local_dir)
    input_key  = f"raw/{period}/pr/purchase_register.xlsx"
    output_key = f"intermediate/pr_validated/period={period}/pr_validated.parquet"
    error_key  = f"final/{period}/pr_errors_for_client.xlsx"

    year, month = int(period[:4]), int(period[4:])
    period_end  = date(year, month, calendar.monthrange(year, month)[1])

    print(f"[PR validation] period={period}")

    # -------------------------------------------------------------------------------------------------
    # STEP 1 - INPUT: purchase register (all cells as text)
    # -------------------------------------------------------------------------------------------------
    register = store.read_excel_as_text(input_key, sheet_name="Purchase Register")
    print(f"  Lines read: {len(register)}")

    # -------------------------------------------------------------------------------------------------
    # STEP 2 - SELECT: check every mandatory column is present, then rename
    # -------------------------------------------------------------------------------------------------
    missing = [header for header in COLUMN_MAP if header not in register.columns]
    if missing:
        raise ValueError(f"Purchase register is missing mandatory columns: {missing}")
    register = register[list(COLUMN_MAP)].rename(columns=COLUMN_MAP)

    con = duckdb.connect()
    con.create_function("is_valid_gstin", is_valid_gstin, [VARCHAR], BOOLEAN, null_handling="special")
    con.register("register", register)

    # -------------------------------------------------------------------------------------------------
    # STEP 3 - DATA CLEANSING + TYPES (blank -> NULL, codes upper case, TRY_CAST numbers and dates)
    # -------------------------------------------------------------------------------------------------
    con.execute("""
        CREATE TABLE cleaned AS
        SELECT
            CAST(row_number() OVER () + 1 AS INTEGER)                        AS source_row_number,  -- Excel row
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
            NULLIF(trim(original_invoice_date), '')                          AS original_invoice_date
        FROM register
    """)

    # -------------------------------------------------------------------------------------------------
    # STEP 4 - FORMULA: expected tax split (supplier state vs place of supply)
    # -------------------------------------------------------------------------------------------------
    con.execute("""
        CREATE TABLE with_expected_tax AS
        SELECT *,
            CASE WHEN pos_state_code <> left(supplier_gstin, 2)
                 THEN round(taxable_value * gst_rate / 100, 2) ELSE 0 END           AS expected_igst,
            CASE WHEN pos_state_code <> left(supplier_gstin, 2)
                 THEN 0 ELSE round(taxable_value * gst_rate / 200, 2) END           AS expected_cgst_sgst
        FROM cleaned
    """)

    # -------------------------------------------------------------------------------------------------
    # STEP 5 - FORMULA: one TRUE/FALSE column per rule (TRUE = rule passed; missing value = failed)
    # -------------------------------------------------------------------------------------------------
    swiggy      = ", ".join(f"'{g}'" for g in SWIGGY_GSTINS)
    state_codes = ", ".join(f"'{c}'" for c in VALID_STATE_CODES)
    gst_rates   = ", ".join(str(r) for r in VALID_GST_RATES)
    itc_values  = ", ".join(f"'{v}'" for v in VALID_ITC_ELIGIBILITY)
    tol         = TOLERANCE_RS

    con.execute(f"""
        CREATE TABLE checked AS
        SELECT *,
            COALESCE(record_id IS NOT NULL AND count(*) OVER (PARTITION BY record_id) = 1, false) AS p01_ok,
            COALESCE(recipient_gstin IN ({swiggy}), false)                                      AS p02_ok,
            COALESCE(is_valid_gstin(supplier_gstin), false)                                     AS p03_ok,
            COALESCE(document_type IN ('INV', 'CRN', 'DBN') AND
                     CASE WHEN invoice_type = 'CDNR' THEN document_type IN ('CRN', 'DBN')
                          ELSE document_type = 'INV' END, false)                                 AS p04_ok,
            COALESCE(invoice_type IN ('B2B', 'CDNR'), false)                                    AS p05_ok,
            COALESCE(length(invoice_number) <= 16
                     AND regexp_full_match(invoice_number, '[A-Za-z0-9/-]+'), false)             AS p06_ok,
            COALESCE(invoice_date <= DATE '{period_end}', false)                                AS p07_ok,
            COALESCE(pos_state_code IN ({state_codes})
                     AND regexp_full_match(place_of_supply, '[0-9]{{2}}-.+'), false)             AS p08_ok,
            COALESCE(reverse_charge IN ('Y', 'N'), false)                                       AS p09_ok,
            COALESCE(regexp_full_match(hsn_sac, '[0-9]{{4}}|[0-9]{{6}}|[0-9]{{8}}'), false)      AS p10_ok,
            COALESCE(gst_rate IN ({gst_rates}), false)                                          AS p11_ok,
            COALESCE(taxable_value > 0, false)                                                  AS p12_ok,
            COALESCE(abs(igst_amount - expected_igst)      <= {tol}
                 AND abs(cgst_amount - expected_cgst_sgst) <= {tol}
                 AND abs(sgst_amount - expected_cgst_sgst) <= {tol}, false)                      AS p13_ok,
            COALESCE(abs(total_invoice_value - (taxable_value + igst_amount + cgst_amount
                                                + sgst_amount + cess_amount)) <= {tol}, false)   AS p14_ok,
            COALESCE(upper(itc_eligibility) IN ({itc_values}), false)                           AS p15_ok,
            COALESCE(CASE WHEN upper(itc_eligibility) = 'INELIGIBLE'
                          THEN itc_igst + itc_cgst + itc_sgst + itc_cess = 0
                          ELSE itc_igst <= igst_amount + {tol} AND itc_cgst <= cgst_amount + {tol}
                           AND itc_sgst <= sgst_amount + {tol} AND itc_cess <= cess_amount + {tol}
                     END, false)                                                                 AS p16_ok,
            COALESCE(invoice_type <> 'CDNR'
                     OR (original_invoice_number IS NOT NULL AND original_invoice_date IS NOT NULL), false) AS p17_ok
        FROM with_expected_tax
    """)

    # -------------------------------------------------------------------------------------------------
    # STEP 6 - FORMULA: status, error count and a remark naming every failed rule
    # -------------------------------------------------------------------------------------------------
    con.execute("""
        CREATE TABLE validated AS
        SELECT *,
            CAST(NOT p01_ok AS INTEGER) + CAST(NOT p02_ok AS INTEGER) + CAST(NOT p03_ok AS INTEGER)
          + CAST(NOT p04_ok AS INTEGER) + CAST(NOT p05_ok AS INTEGER) + CAST(NOT p06_ok AS INTEGER)
          + CAST(NOT p07_ok AS INTEGER) + CAST(NOT p08_ok AS INTEGER) + CAST(NOT p09_ok AS INTEGER)
          + CAST(NOT p10_ok AS INTEGER) + CAST(NOT p11_ok AS INTEGER) + CAST(NOT p12_ok AS INTEGER)
          + CAST(NOT p13_ok AS INTEGER) + CAST(NOT p14_ok AS INTEGER) + CAST(NOT p15_ok AS INTEGER)
          + CAST(NOT p16_ok AS INTEGER) + CAST(NOT p17_ok AS INTEGER)                    AS error_count,
            concat_ws(' | ',
                CASE WHEN NOT p01_ok THEN 'P01 Record ID missing or duplicated' END,
                CASE WHEN NOT p02_ok THEN 'P02 Recipient GSTIN is not a Swiggy registration: ' || coalesce(recipient_gstin, '(blank)') END,
                CASE WHEN NOT p03_ok THEN 'P03 Invalid supplier GSTIN: ' || coalesce(supplier_gstin, '(blank)') END,
                CASE WHEN NOT p04_ok THEN 'P04 Document type ' || coalesce(document_type, '(blank)') || ' does not match invoice type' END,
                CASE WHEN NOT p05_ok THEN 'P05 Invalid invoice type: ' || coalesce(invoice_type, '(blank)') END,
                CASE WHEN NOT p06_ok THEN 'P06 Invalid invoice number: ' || coalesce(invoice_number, '(blank)') END,
                CASE WHEN NOT p07_ok THEN 'P07 Invoice date missing or after return period: ' || coalesce(invoice_date_text, '(blank)') END,
                CASE WHEN NOT p08_ok THEN 'P08 Invalid place of supply: ' || coalesce(place_of_supply, '(blank)') END,
                CASE WHEN NOT p09_ok THEN 'P09 Reverse charge must be Y or N' END,
                CASE WHEN NOT p10_ok THEN 'P10 HSN/SAC must be 4, 6 or 8 digits: ' || coalesce(hsn_sac, '(blank)') END,
                CASE WHEN NOT p11_ok THEN 'P11 Invalid GST rate: ' || coalesce(CAST(gst_rate AS VARCHAR), '(blank)') END,
                CASE WHEN NOT p12_ok THEN 'P12 Taxable value must be positive: ' || coalesce(CAST(taxable_value AS VARCHAR), '(blank)') END,
                CASE WHEN NOT p13_ok THEN 'P13 Tax split wrong for supplier state / place of supply (expected IGST '
                                          || CAST(expected_igst AS VARCHAR) || ', CGST/SGST '
                                          || CAST(expected_cgst_sgst AS VARCHAR) || ')' END,
                CASE WHEN NOT p14_ok THEN 'P14 Total invoice value does not equal taxable value + taxes' END,
                CASE WHEN NOT p15_ok THEN 'P15 Invalid ITC eligibility: ' || coalesce(itc_eligibility, '(blank)') END,
                CASE WHEN NOT p16_ok THEN 'P16 ITC claimed is more than tax charged (or claimed on an ineligible line)' END,
                CASE WHEN NOT p17_ok THEN 'P17 Original invoice number/date missing on credit/debit note' END
            ) AS failed_rules
        FROM checked
    """)

    # -------------------------------------------------------------------------------------------------
    # STEP 7 - SELECT: final output columns, fixed order and types (Athena table pr_validated)
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
    # STEP 8 - OUTPUT
    # -------------------------------------------------------------------------------------------------
    store.write_parquet(result, output_key)
    result_df = result.to_pandas()
    errors_df = result_df[result_df["validation_status"] == "Error"].copy()
    for column in ["invoice_date", "voucher_date"]:
        errors_df[column] = pd.to_datetime(errors_df[column]).dt.strftime("%d/%m/%Y")
    errors_df["processed_at"] = errors_df["processed_at"].dt.strftime("%d/%m/%Y %H:%M")
    store.write_excel({"Errors": errors_df}, error_key)

    summary = {"lines": len(result_df),
               "success": int((result_df["validation_status"] == "Success").sum()),
               "errors": len(errors_df)}
    print(f"  Result: {summary['success']} success, {summary['errors']} errors")
    print(f"  Output: {output_key}")
    print(f"          {error_key}")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate the purchase register for one month")
    parser.add_argument("--period", required=True, help="return period YYYYMM, e.g. 202506")
    parser.add_argument("--local-dir", help="use this local folder instead of S3")
    args = parser.parse_args()
    run(args.period, args.local_dir)
