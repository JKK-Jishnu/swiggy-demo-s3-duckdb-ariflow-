"""
transformation.py — Swiggy GSTR1 Validation
Client   : Swiggy
Activity : GSTR1 Validation (per batch)
Stack    : DuckDB + pandas
"""
from datetime import datetime

import duckdb
import pandas as pd
from duckdb.typing import BOOLEAN, VARCHAR

from activities.common.gst_utils import parse_dates, validate_gstin_format

VALID_INVOICE_TYPES = ("B2B", "B2C", "CDNR", "CDNUR", "EXP")


def run(df: pd.DataFrame, batch_id: str = "B1", period: str = None, logger=None) -> dict:
    """
    Main validation function. Uses DuckDB for SQL-based processing.

    Input : DataFrame of GSTR1 rows
    Output: {"result": all_rows, "success": clean_rows, "errors": error_rows}
    """
    if logger:
        logger.info(f"Starting GSTR1 validation for batch {batch_id}")

    print(f"[{batch_id}] Input rows: {len(df)}")
    df = df.copy()

    # Clean column names
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

    # Ensure required columns exist
    required = ["receiver_gstin", "invoice_number", "invoice_date",
                "invoice_type", "taxable_value"]
    for col in required:
        if col not in df.columns:
            df[col] = None

    # Convert types
    df["taxable_value"]  = pd.to_numeric(df["taxable_value"], errors="coerce").fillna(0)
    for col in ["receiver_gstin", "invoice_type", "invoice_number"]:
        df[col] = df[col].fillna("").astype(str).str.strip()
    df["receiver_gstin"] = df["receiver_gstin"].str.upper()
    df["invoice_type"]   = df["invoice_type"].str.upper()
    df["invoice_date_parsed"] = parse_dates(df["invoice_date"])

    # Register GSTIN validator as DuckDB UDF and the DataFrame as a table
    con = duckdb.connect()
    con.create_function("is_valid_gstin", validate_gstin_format, [VARCHAR], BOOLEAN)
    con.register("gstr1_input", df)

    # Run all validations in SQL
    valid_types = ", ".join(f"'{t}'" for t in VALID_INVOICE_TYPES)
    result_df = con.execute(f"""
        WITH checks AS (
            SELECT
                *,
                COALESCE(is_valid_gstin(receiver_gstin), false)          AS gstin_valid,
                invoice_type IN ({valid_types})                          AS type_valid,
                taxable_value > 0                                        AS amount_valid,
                invoice_number <> '' AND receiver_gstin <> ''            AS required_valid,
                COALESCE(CAST(invoice_date_parsed AS DATE) <= current_date, false)   AS date_valid
            FROM gstr1_input
        )
        SELECT
            * EXCLUDE (gstin_valid, type_valid, amount_valid, required_valid,
                       date_valid, invoice_date_parsed),
            CASE WHEN gstin_valid AND type_valid AND amount_valid
                      AND required_valid AND date_valid
                 THEN 'Success' ELSE 'Error' END                         AS validation_status,
            CASE WHEN gstin_valid AND type_valid AND amount_valid
                      AND required_valid AND date_valid
                 THEN 'All checks passed'
                 ELSE concat_ws(' | ',
                    CASE WHEN NOT gstin_valid    THEN 'Invalid GSTIN: ' || receiver_gstin END,
                    CASE WHEN NOT type_valid     THEN 'Invalid invoice type: ' || invoice_type END,
                    CASE WHEN NOT amount_valid   THEN 'Taxable value must be positive: ' || taxable_value END,
                    CASE WHEN NOT required_valid THEN 'Missing required fields' END,
                    CASE WHEN NOT date_valid     THEN 'Invalid or future date: ' || CAST(invoice_date AS VARCHAR) END
                 ) END                                                   AS validation_remark
        FROM checks
    """).df()
    con.close()

    result_df["period"]       = period
    result_df["batch_id"]     = batch_id
    result_df["processed_at"] = datetime.now().isoformat()

    success_df = result_df[result_df["validation_status"] == "Success"].copy()
    error_df   = result_df[result_df["validation_status"] == "Error"].copy()

    print(f"[{batch_id}] Success: {len(success_df)} | Errors: {len(error_df)}")

    return {
        "result" : result_df,
        "success": success_df,
        "errors" : error_df,
    }
