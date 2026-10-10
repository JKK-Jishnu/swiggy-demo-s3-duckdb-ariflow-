"""
=====================================================================================================
JOB      : GSTR-1 Validation                                                      (Activity 1)
CLIENT   : Swiggy
RUNS     : Once per batch (B1, B2, ... as listed in the month's manifest), in two stages:
             stage "raw"       - on the sales register exactly as the client uploaded it
             stage "corrected" - again, after Activity 2 has applied the client's corrections
-----------------------------------------------------------------------------------------------------
WHAT IT DOES
  Checks every line of the sales register against the GST rules listed below. Each line is
  marked "Success" or "Error", and gets a remark that names every rule it failed.

ALTERYX EQUIVALENT
  Input Data -> Select (rename + types) -> Data Cleansing -> Formula (one per rule)
  -> Formula (status + remark) -> Filter (Error rows) -> Output Data (x2)

INPUT
  stage raw       : raw/<period>/gstr1/<batch>/gstr1_<batch>.xlsx            (sheet "Sales Register")
  stage corrected : intermediate/gstr1_corrected/period=<period>/<batch>.parquet   (from Activity 2)

OUTPUT
  stage raw       : intermediate/gstr1_validated/period=<period>/<batch>.parquet   every line + status
                    final/<period>/gstr1/<batch>_errors_for_client.xlsx            error lines only
  stage corrected : intermediate/gstr1_final/period=<period>/<batch>.parquet       every line + status
                    final/<period>/gstr1/<batch>_errors_after_correction.xlsx      error lines only

RECORD ID
  Every line arrives with a Record ID such as  sales-202506-b2b-1  (source-period-type-number).
  The ID is never changed, so the same line can be looked up in every later file and in Athena.

VALIDATION RULES  (a line must pass all of them to be "Success")
  R01  Record ID present and unique in the file
  R02  Supplier GSTIN valid (15 characters, correct pattern, correct check digit)
  R03  Document type is INV / CRN / DBN and matches the invoice type (credit/debit notes = CRN/DBN)
  R04  Invoice type is one of B2B, B2C, CDNR, CDNUR, EXP
  R05  Invoice number present, max 16 characters, only letters, digits, "/" and "-"
  R06  Invoice date is a valid date inside the return period
  R07  Receiver GSTIN valid for B2B / CDNR, and blank for B2C / CDNUR / EXP
  R08  Receiver name present for B2B / CDNR
  R09  Place of supply is a valid state code ("29-Karnataka"); exports must use 96
  R10  Reverse charge is Y or N
  R11  HSN/SAC is 6 or 8 digits (mandatory 6+ digits, turnover above Rs.5 crore)
  R12  UQC is a valid unit code; goods (HSN not starting 99) must have quantity > 0
  R13  GST rate is one of the notified rates
  R14  Taxable value is greater than zero
  R15  Tax split matches place of supply: inter-state = IGST, intra-state = CGST + SGST (Rs.1 tolerance)
  R16  Total invoice value = taxable value + all taxes + cess (Rs.1 tolerance)
  R17  Credit / debit notes carry the original invoice number and date
  R18  Exports carry export type (WPAY / WOPAY) and a 6 character port code

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
import pyarrow as pa
import pyarrow.parquet as pq
from duckdb.typing import BOOLEAN, VARCHAR


# =====================================================================================================
# CONFIGURATION
# =====================================================================================================

# Sales register column headers (as the client sends them)  ->  column names used inside this job.
# Works like the Alteryx Select tool: rename every field once, at the start.
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

# Extra columns that only exist in stage "corrected" (added by Activity 2 - Error Fixing)
CORRECTION_COLUMN_MAP = {
    "Correction Applied": "correction_applied",
    "Corrected Fields":   "corrected_fields",
    "Correction Remark":  "correction_remark",
    "First Pass Status":  "first_pass_status",
    "First Pass Remark":  "first_pass_remark",
}

VALID_INVOICE_TYPES = ["B2B", "B2C", "CDNR", "CDNUR", "EXP"]
VALID_GST_RATES     = [0, 0.1, 0.25, 1, 1.5, 3, 5, 6, 7.5, 12, 18, 28]
VALID_UQC           = ["NA", "NOS", "PCS", "KGS", "GMS", "LTR", "MTR", "BOX", "SET", "UNT", "OTH"]
VALID_STATE_CODES   = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13",
                       "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "26", "27",
                       "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "96", "97"]
TOLERANCE_RS        = 1.0   # rounding tolerance for tax and total checks

STAGES = {
    #  stage        input file                                                   output table          client error file
    "raw":       ("raw/{period}/gstr1/{batch}/gstr1_{batch}.xlsx",               "gstr1_validated",   "{batch}_errors_for_client.xlsx"),
    "corrected": ("intermediate/gstr1_corrected/period={period}/{batch}.parquet", "gstr1_final",       "{batch}_errors_after_correction.xlsx"),
}


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
# THE JOB
# =====================================================================================================
def run(period: str, batch_id: str, stage: str = "raw", local_dir: str = None) -> dict:
    store = Storage(local_dir)
    input_key, output_table, error_file = STAGES[stage]
    input_key  = input_key.format(period=period, batch=batch_id)
    output_key = f"intermediate/{output_table}/period={period}/{batch_id}.parquet"
    error_key  = f"final/{period}/gstr1/" + error_file.format(batch=batch_id)

    # Return period window, e.g. 202506 -> 2025-06-01 .. 2025-06-30
    year, month  = int(period[:4]), int(period[4:])
    period_start = date(year, month, 1)
    period_end   = date(year, month, calendar.monthrange(year, month)[1])

    print(f"[GSTR1 validation] period={period} batch={batch_id} stage={stage}")
    print(f"  Input : {input_key}")

    # -------------------------------------------------------------------------------------------------
    # STEP 1 - INPUT: read the sales register (all cells as text)
    # -------------------------------------------------------------------------------------------------
    if stage == "raw":
        register = store.read_excel_as_text(input_key, sheet_name="Sales Register")
    else:
        register = store.read_parquet(input_key).to_pandas()
    print(f"  Lines read: {len(register)}")

    # -------------------------------------------------------------------------------------------------
    # STEP 2 - SELECT: check every mandatory column is present, then rename to job column names
    # -------------------------------------------------------------------------------------------------
    missing = [header for header in COLUMN_MAP if header not in register.columns]
    if missing:
        raise ValueError(f"Sales register is missing mandatory columns: {missing}")

    # The correction columns only exist after Activity 2. On the first run they are added empty,
    # so both stages go through exactly the same steps below.
    for header in CORRECTION_COLUMN_MAP:
        if header not in register.columns:
            register[header] = ""
    column_map = {**COLUMN_MAP, **CORRECTION_COLUMN_MAP}
    register = register[list(column_map)].rename(columns=column_map)

    con = duckdb.connect()
    con.create_function("is_valid_gstin", is_valid_gstin, [VARCHAR], BOOLEAN, null_handling="special")
    con.register("register", register)

    # -------------------------------------------------------------------------------------------------
    # STEP 3 - DATA CLEANSING + TYPES
    #   * trim spaces, blank text -> NULL, codes in upper case
    #   * numbers: TRY_CAST returns NULL (instead of failing) when the text is not a number
    #   * dates : accept dd/mm/yyyy or yyyy-mm-dd; anything else becomes NULL
    # -------------------------------------------------------------------------------------------------
    con.execute("""
        CREATE TABLE cleaned AS
        SELECT
            CAST(row_number() OVER () + 1 AS INTEGER)            AS source_row_number,   -- Excel row (row 1 = headers)
            NULLIF(trim(record_id), '')                          AS record_id,
            upper(NULLIF(trim(supplier_gstin), ''))              AS supplier_gstin,
            upper(NULLIF(trim(document_type), ''))               AS document_type,
            upper(NULLIF(trim(invoice_type), ''))                AS invoice_type,
            NULLIF(trim(invoice_number), '')                     AS invoice_number,
            NULLIF(trim(invoice_date_text), '')                  AS invoice_date_text,
            CAST(COALESCE(try_strptime(trim(invoice_date_text), '%d/%m/%Y'),
                          try_strptime(trim(invoice_date_text), '%Y-%m-%d')) AS DATE) AS invoice_date,
            upper(NULLIF(trim(receiver_gstin), ''))              AS receiver_gstin,
            NULLIF(trim(receiver_name), '')                      AS receiver_name,
            NULLIF(trim(place_of_supply), '')                    AS place_of_supply,
            left(trim(place_of_supply), 2)                       AS pos_state_code,
            upper(NULLIF(trim(reverse_charge), ''))              AS reverse_charge,
            upper(NULLIF(trim(ecommerce_gstin), ''))             AS ecommerce_gstin,
            NULLIF(trim(hsn_sac), '')                            AS hsn_sac,
            NULLIF(trim(item_description), '')                   AS item_description,
            upper(NULLIF(trim(uqc), ''))                         AS uqc,
            TRY_CAST(NULLIF(trim(quantity), '') AS DOUBLE)            AS quantity,
            TRY_CAST(NULLIF(trim(gst_rate), '') AS DOUBLE)            AS gst_rate,
            TRY_CAST(NULLIF(trim(taxable_value), '') AS DOUBLE)       AS taxable_value,
            COALESCE(TRY_CAST(NULLIF(trim(igst_amount), '') AS DOUBLE), 0) AS igst_amount,
            COALESCE(TRY_CAST(NULLIF(trim(cgst_amount), '') AS DOUBLE), 0) AS cgst_amount,
            COALESCE(TRY_CAST(NULLIF(trim(sgst_amount), '') AS DOUBLE), 0) AS sgst_amount,
            COALESCE(TRY_CAST(NULLIF(trim(cess_amount), '') AS DOUBLE), 0) AS cess_amount,
            TRY_CAST(NULLIF(trim(total_invoice_value), '') AS DOUBLE) AS total_invoice_value,
            NULLIF(trim(original_invoice_number), '')            AS original_invoice_number,
            NULLIF(trim(original_invoice_date), '')              AS original_invoice_date,
            upper(NULLIF(trim(export_type), ''))                 AS export_type,
            upper(NULLIF(trim(port_code), ''))                   AS port_code,
            NULLIF(trim(shipping_bill_number), '')               AS shipping_bill_number,
            NULLIF(trim(shipping_bill_date), '')                 AS shipping_bill_date,
            -- correction history (empty on the first run)
            (trim(correction_applied) = 'Y')                     AS correction_applied,
            NULLIF(trim(corrected_fields), '')                   AS corrected_fields,
            NULLIF(trim(correction_remark), '')                  AS correction_remark,
            NULLIF(trim(first_pass_status), '')                  AS first_pass_status,
            NULLIF(trim(first_pass_remark), '')                  AS first_pass_remark
        FROM register
    """)

    # -------------------------------------------------------------------------------------------------
    # STEP 4 - FORMULA: the tax we EXPECT on each line, worked out from place of supply and rate
    #   inter-state (place of supply state <> supplier state, or export)  -> all tax is IGST
    #   intra-state (same state)                                         -> half CGST + half SGST
    #   export without payment of tax (WOPAY)                            -> no tax
    # -------------------------------------------------------------------------------------------------
    con.execute("""
        CREATE TABLE with_expected_tax AS
        SELECT *,
            (invoice_type = 'EXP' OR pos_state_code <> left(supplier_gstin, 2))       AS is_inter_state,
            CASE WHEN export_type = 'WOPAY' THEN 0
                 WHEN invoice_type = 'EXP' OR pos_state_code <> left(supplier_gstin, 2)
                      THEN round(taxable_value * gst_rate / 100, 2)
                 ELSE 0 END                                                           AS expected_igst,
            CASE WHEN invoice_type = 'EXP' OR pos_state_code <> left(supplier_gstin, 2) THEN 0
                 ELSE round(taxable_value * gst_rate / 200, 2) END                    AS expected_cgst_sgst
        FROM cleaned
    """)

    # -------------------------------------------------------------------------------------------------
    # STEP 5 - FORMULA: one TRUE/FALSE column per rule (TRUE = rule passed).
    #   COALESCE(..., false): if a value is missing the rule counts as failed.
    # -------------------------------------------------------------------------------------------------
    state_codes = ", ".join(f"'{code}'" for code in VALID_STATE_CODES)
    inv_types   = ", ".join(f"'{t}'" for t in VALID_INVOICE_TYPES)
    gst_rates   = ", ".join(str(r) for r in VALID_GST_RATES)
    uqc_codes   = ", ".join(f"'{u}'" for u in VALID_UQC)

    con.execute(f"""
        CREATE TABLE checked AS
        SELECT *,
            COALESCE(record_id IS NOT NULL AND count(*) OVER (PARTITION BY record_id) = 1, false) AS r01_ok,
            COALESCE(is_valid_gstin(supplier_gstin), false)                                     AS r02_ok,
            COALESCE(document_type IN ('INV', 'CRN', 'DBN') AND
                     CASE WHEN invoice_type IN ('CDNR', 'CDNUR') THEN document_type IN ('CRN', 'DBN')
                          ELSE document_type = 'INV' END, false)                                 AS r03_ok,
            COALESCE(invoice_type IN ({inv_types}), false)                                      AS r04_ok,
            COALESCE(length(invoice_number) <= 16
                     AND regexp_full_match(invoice_number, '[A-Za-z0-9/-]+'), false)             AS r05_ok,
            COALESCE(invoice_date BETWEEN DATE '{period_start}' AND DATE '{period_end}', false)  AS r06_ok,
            COALESCE(CASE WHEN invoice_type IN ('B2B', 'CDNR') THEN is_valid_gstin(receiver_gstin)
                          ELSE receiver_gstin IS NULL END, false)                                AS r07_ok,
            COALESCE(invoice_type NOT IN ('B2B', 'CDNR') OR receiver_name IS NOT NULL, false)    AS r08_ok,
            COALESCE(pos_state_code IN ({state_codes})
                     AND regexp_full_match(place_of_supply, '[0-9]{{2}}-.+')
                     AND (invoice_type <> 'EXP' OR pos_state_code = '96'), false)                AS r09_ok,
            COALESCE(reverse_charge IN ('Y', 'N'), false)                                       AS r10_ok,
            COALESCE(regexp_full_match(hsn_sac, '[0-9]{{6}}|[0-9]{{8}}'), false)                 AS r11_ok,
            COALESCE(uqc IN ({uqc_codes}) AND (hsn_sac LIKE '99%' OR quantity > 0), false)       AS r12_ok,
            COALESCE(gst_rate IN ({gst_rates}), false)                                          AS r13_ok,
            COALESCE(taxable_value > 0, false)                                                  AS r14_ok,
            COALESCE(abs(igst_amount - expected_igst)      <= {TOLERANCE_RS}
                 AND abs(cgst_amount - expected_cgst_sgst) <= {TOLERANCE_RS}
                 AND abs(sgst_amount - expected_cgst_sgst) <= {TOLERANCE_RS}, false)             AS r15_ok,
            COALESCE(abs(total_invoice_value - (taxable_value + igst_amount + cgst_amount
                                                + sgst_amount + cess_amount)) <= {TOLERANCE_RS}, false) AS r16_ok,
            COALESCE(invoice_type NOT IN ('CDNR', 'CDNUR')
                     OR (original_invoice_number IS NOT NULL AND original_invoice_date IS NOT NULL), false) AS r17_ok,
            COALESCE(invoice_type <> 'EXP'
                     OR (export_type IN ('WPAY', 'WOPAY') AND length(port_code) = 6), false)     AS r18_ok
        FROM with_expected_tax
    """)

    # -------------------------------------------------------------------------------------------------
    # STEP 6 - FORMULA: status, error count and a remark naming every failed rule
    # -------------------------------------------------------------------------------------------------
    con.execute("""
        CREATE TABLE validated AS
        SELECT *,
            CAST(NOT r01_ok AS INTEGER) + CAST(NOT r02_ok AS INTEGER) + CAST(NOT r03_ok AS INTEGER)
          + CAST(NOT r04_ok AS INTEGER) + CAST(NOT r05_ok AS INTEGER) + CAST(NOT r06_ok AS INTEGER)
          + CAST(NOT r07_ok AS INTEGER) + CAST(NOT r08_ok AS INTEGER) + CAST(NOT r09_ok AS INTEGER)
          + CAST(NOT r10_ok AS INTEGER) + CAST(NOT r11_ok AS INTEGER) + CAST(NOT r12_ok AS INTEGER)
          + CAST(NOT r13_ok AS INTEGER) + CAST(NOT r14_ok AS INTEGER) + CAST(NOT r15_ok AS INTEGER)
          + CAST(NOT r16_ok AS INTEGER) + CAST(NOT r17_ok AS INTEGER) + CAST(NOT r18_ok AS INTEGER) AS error_count,
            concat_ws(' | ',
                CASE WHEN NOT r01_ok THEN 'R01 Record ID missing or duplicated' END,
                CASE WHEN NOT r02_ok THEN 'R02 Invalid supplier GSTIN: ' || coalesce(supplier_gstin, '(blank)') END,
                CASE WHEN NOT r03_ok THEN 'R03 Document type ' || coalesce(document_type, '(blank)') || ' does not match invoice type' END,
                CASE WHEN NOT r04_ok THEN 'R04 Invalid invoice type: ' || coalesce(invoice_type, '(blank)') END,
                CASE WHEN NOT r05_ok THEN 'R05 Invalid invoice number: ' || coalesce(invoice_number, '(blank)') END,
                CASE WHEN NOT r06_ok THEN 'R06 Invoice date missing or outside return period: ' || coalesce(invoice_date_text, '(blank)') END,
                CASE WHEN NOT r07_ok THEN 'R07 Invalid receiver GSTIN for ' || coalesce(invoice_type, '?') || ': ' || coalesce(receiver_gstin, '(blank)') END,
                CASE WHEN NOT r08_ok THEN 'R08 Receiver name missing' END,
                CASE WHEN NOT r09_ok THEN 'R09 Invalid place of supply: ' || coalesce(place_of_supply, '(blank)') END,
                CASE WHEN NOT r10_ok THEN 'R10 Reverse charge must be Y or N' END,
                CASE WHEN NOT r11_ok THEN 'R11 HSN/SAC must be 6 or 8 digits: ' || coalesce(hsn_sac, '(blank)') END,
                CASE WHEN NOT r12_ok THEN 'R12 Invalid UQC or quantity' END,
                CASE WHEN NOT r13_ok THEN 'R13 Invalid GST rate: ' || coalesce(CAST(gst_rate AS VARCHAR), '(blank)') END,
                CASE WHEN NOT r14_ok THEN 'R14 Taxable value must be positive: ' || coalesce(CAST(taxable_value AS VARCHAR), '(blank)') END,
                CASE WHEN NOT r15_ok THEN 'R15 Tax split wrong for place of supply (expected IGST '
                                          || CAST(expected_igst AS VARCHAR) || ', CGST/SGST '
                                          || CAST(expected_cgst_sgst AS VARCHAR) || ')' END,
                CASE WHEN NOT r16_ok THEN 'R16 Total invoice value does not equal taxable value + taxes' END,
                CASE WHEN NOT r17_ok THEN 'R17 Original invoice number/date missing on credit/debit note' END,
                CASE WHEN NOT r18_ok THEN 'R18 Export type or port code missing' END
            ) AS failed_rules
        FROM checked
    """)

    # -------------------------------------------------------------------------------------------------
    # STEP 7 - SELECT: final output columns in a fixed order with fixed types
    #   (the Athena tables gstr1_validated / sales_combined expect exactly these columns)
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
            -- correction history; on the first run the first-pass result is this result
            COALESCE(correction_applied, false)                           AS correction_applied,
            corrected_fields,
            correction_remark,
            COALESCE(first_pass_status,
                     CASE WHEN error_count = 0 THEN 'Success' ELSE 'Error' END)  AS first_pass_status,
            COALESCE(first_pass_remark,
                     CASE WHEN error_count = 0 THEN 'All checks passed' ELSE failed_rules END) AS first_pass_remark,
            CAST(now() AS TIMESTAMP)                                      AS processed_at
        FROM validated
        ORDER BY source_row_number
    """).arrow()
    con.close()

    # -------------------------------------------------------------------------------------------------
    # STEP 8 - OUTPUT: every line to parquet (pipeline + Athena), error lines to Excel (client)
    # -------------------------------------------------------------------------------------------------
    store.write_parquet(result, output_key)
    result_df = result.to_pandas()
    errors_df = result_df[result_df["validation_status"] == "Error"]
    store.write_excel({"Errors": excel_friendly(errors_df)}, error_key)

    summary = {"lines": len(result_df),
               "success": int((result_df["validation_status"] == "Success").sum()),
               "errors": len(errors_df)}
    print(f"  Result: {summary['success']} success, {summary['errors']} errors")
    print(f"  Output: {output_key}")
    print(f"          {error_key}")
    return summary


def excel_friendly(df: pd.DataFrame) -> pd.DataFrame:
    """Dates as dd/mm/yyyy text and no time zone info, so the Excel file reads cleanly."""
    out = df.copy()
    for column in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[column]):
            out[column] = out[column].dt.strftime("%d/%m/%Y %H:%M" if column == "processed_at" else "%d/%m/%Y")
        elif out[column].dtype == object and out[column].map(lambda v: isinstance(v, date)).any():
            out[column] = out[column].map(lambda v: v.strftime("%d/%m/%Y") if isinstance(v, date) else v)
    return out


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GSTR-1 validation for one batch")
    parser.add_argument("--period", required=True, help="return period YYYYMM, e.g. 202506")
    parser.add_argument("--batch", required=True, help="batch id, e.g. B1")
    parser.add_argument("--stage", default="raw", choices=list(STAGES))
    parser.add_argument("--local-dir", help="use this local folder instead of S3")
    args = parser.parse_args()
    run(args.period, args.batch, args.stage, args.local_dir)
