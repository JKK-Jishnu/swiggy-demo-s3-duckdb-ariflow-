"""
transformation.py — Swiggy Purchase Register Validation
Activity : 7 — validate PR rows before 2B reconciliation
Stack    : DuckDB + pandas
"""
from datetime import datetime

import duckdb
import pandas as pd
from duckdb.typing import BOOLEAN, VARCHAR

from activities.common.gst_utils import clean_text, parse_dates, validate_gstin_format

COLUMN_MAP = {
    "Recipient GSTIN": "recipient_gstin", "Supplier GSTIN": "supplier_gstin",
    "Supplier Name": "supplier_name", "Invoice No": "invoice_number",
    "Invoice Date": "invoice_date", "Invoice Type": "invoice_type",
    "Taxable Value": "taxable_value", "IGST": "igst", "CGST": "cgst", "SGST": "sgst",
    "Total Invoice Value": "total_invoice_value", "ITC Eligible": "itc_eligible",
    "ITC Claimed": "itc_claimed",
}
NUMERIC = ["taxable_value", "igst", "cgst", "sgst", "total_invoice_value", "itc_claimed"]


def run(df: pd.DataFrame, period: str = None) -> dict:
    """
    Output: {"result": all_rows, "success": clean_rows, "errors": error_rows}
    """
    print(f"[PR {period}] Input rows: {len(df)}")
    df = df.rename(columns=COLUMN_MAP).copy()
    for col in COLUMN_MAP.values():
        if col not in df.columns:
            df[col] = None
    for col in NUMERIC:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    for col in ["recipient_gstin", "supplier_gstin", "invoice_type"]:
        df[col] = clean_text(df[col], upper=True)
    df["invoice_number"]      = clean_text(df["invoice_number"])
    df["invoice_date_parsed"] = parse_dates(df["invoice_date"])

    con = duckdb.connect()
    con.create_function("is_valid_gstin", validate_gstin_format, [VARCHAR], BOOLEAN)
    con.register("pr", df)

    result = con.execute("""
        WITH checks AS (
            SELECT *,
                COALESCE(is_valid_gstin(supplier_gstin), false)                AS supplier_ok,
                COALESCE(is_valid_gstin(recipient_gstin), false)               AS recipient_ok,
                invoice_type IN ('B2B', 'CDNR')                                AS type_ok,
                taxable_value > 0                                              AS amount_ok,
                invoice_number <> ''                                           AS invno_ok,
                COALESCE(CAST(invoice_date_parsed AS DATE) <= current_date, false) AS date_ok,
                itc_claimed <= igst + cgst + sgst + 1                          AS itc_ok
            FROM pr
        )
        SELECT * EXCLUDE (supplier_ok, recipient_ok, type_ok, amount_ok, invno_ok, date_ok, itc_ok,
                          invoice_date_parsed),
            CASE WHEN supplier_ok AND recipient_ok AND type_ok AND amount_ok AND invno_ok AND date_ok AND itc_ok
                 THEN 'Success' ELSE 'Error' END AS validation_status,
            CASE WHEN supplier_ok AND recipient_ok AND type_ok AND amount_ok AND invno_ok AND date_ok AND itc_ok
                 THEN 'All checks passed'
                 ELSE concat_ws(' | ',
                    CASE WHEN NOT supplier_ok  THEN 'Invalid supplier GSTIN: ' || supplier_gstin END,
                    CASE WHEN NOT recipient_ok THEN 'Invalid recipient GSTIN: ' || recipient_gstin END,
                    CASE WHEN NOT type_ok      THEN 'Invalid invoice type: ' || invoice_type END,
                    CASE WHEN NOT amount_ok    THEN 'Taxable value must be positive: ' || taxable_value END,
                    CASE WHEN NOT invno_ok     THEN 'Missing invoice number' END,
                    CASE WHEN NOT date_ok      THEN 'Invalid or future date: ' || CAST(invoice_date AS VARCHAR) END,
                    CASE WHEN NOT itc_ok       THEN 'ITC claimed ' || itc_claimed || ' exceeds tax ' || (igst + cgst + sgst) END
                 ) END AS validation_remark
        FROM checks
    """).df()
    con.close()

    result["period"]       = period
    result["processed_at"] = datetime.now().isoformat()
    success = result[result["validation_status"] == "Success"].copy()
    errors  = result[result["validation_status"] == "Error"].copy()
    print(f"[PR {period}] Success: {len(success)} | Errors: {len(errors)}")
    return {"result": result, "success": success, "errors": errors}
