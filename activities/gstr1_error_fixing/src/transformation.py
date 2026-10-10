"""
transformation.py — Swiggy GSTR1 Error Fixing
Activity : 2 — apply client corrections to validation errors and revalidate
Stack    : pandas + DuckDB (via gstr1_validation)
"""
import pandas as pd

from activities.common.gst_utils import clean_text
from activities.gstr1_validation.src.transformation import run as validate

CORRECTABLE = ["receiver_gstin", "invoice_date", "invoice_type", "taxable_value",
               "igst_amount", "cgst_amount", "sgst_amount", "total_invoice_value"]
VALIDATION_COLS = ["validation_status", "validation_remark", "processed_at", "batch_id", "period"]


def run(validated_df: pd.DataFrame, corrections_df: pd.DataFrame = None,
        batch_id: str = "B1", period: str = None) -> dict:
    """
    Input : validated rows (output of gstr1_validation) + client corrections keyed by invoice_number
            (blank cells in the corrections file mean "keep the original value")
    Output: {"result": all rows after fix, "fixed": error rows now passing, "still_errors": remaining errors}
    """
    before = validated_df.copy()
    status_before = before.set_index("invoice_number")["validation_status"]

    if corrections_df is None or corrections_df.empty:
        print(f"[{batch_id}] No corrections received — passing rows through")
        result = before
        result["correction_applied"] = False
        result["correction_remark"]  = ""
    else:
        corr = corrections_df.copy()
        corr.columns = [str(c).strip().lower().replace(" ", "_") for c in corr.columns]
        corr["invoice_number"] = clean_text(corr["invoice_number"])
        corr = corr.drop_duplicates("invoice_number", keep="last").set_index("invoice_number")

        # Overwrite only the fields the client filled in
        patched = before.drop(columns=VALIDATION_COLS, errors="ignore").set_index("invoice_number")
        cols    = [c for c in CORRECTABLE if c in corr.columns]
        patched.update(corr[cols])
        patched = patched.reset_index()

        result = validate(patched, batch_id=batch_id, period=period)["result"]
        result["correction_applied"] = result["invoice_number"].isin(corr.index)
        result["correction_remark"]  = result["invoice_number"].map(
            corr.get("correction_remark", pd.Series(dtype=str))).fillna("")
        unmatched = set(corr.index) - set(result["invoice_number"])
        if unmatched:
            print(f"[{batch_id}] WARNING: {len(unmatched)} corrections reference unknown invoices: {sorted(unmatched)}")

    result["status_before_fix"] = result["invoice_number"].map(status_before)
    fixed        = result[(result["status_before_fix"] == "Error") & (result["validation_status"] == "Success")].copy()
    still_errors = result[result["validation_status"] == "Error"].copy()

    print(f"[{batch_id}] Errors before: {int((status_before == 'Error').sum())} | "
          f"Fixed: {len(fixed)} | Still errors: {len(still_errors)}")
    return {"result": result, "fixed": fixed, "still_errors": still_errors}
