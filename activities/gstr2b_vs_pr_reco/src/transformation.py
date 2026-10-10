"""
transformation.py — Swiggy GSTR-2B vs Purchase Register Reconciliation
Activity : 9 — match vendor-reported 2B against Swiggy's books
Ladder   : exact → GSTIN+inv# (amount differs) → GSTIN+amount (inv# differs) → unmatched
"""
import pandas as pd

from activities.common.ladder_match import ladder_match


def _std(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "gstin"   : df["supplier_gstin"].values,
        "inv_no"  : df["invoice_number"].values,
        "inv_date": df["invoice_date"].values,
        "amount"  : df["taxable_value"].values,
        "ref"     : df["recipient_gstin"].values,
    })


def run(gstr2b_df: pd.DataFrame, pr_df: pd.DataFrame, period: str = None) -> dict:
    """
    Input : consolidated 2B rows + validated PR rows (only 'Success' PR rows are reconciled)
    Output: {"result", "summary"}; columns prefixed gstr2b_ / pr_
    """
    pr_ok = pr_df[pr_df["validation_status"] == "Success"]
    out   = ladder_match(_std(gstr2b_df), _std(pr_ok), left_label="2B", right_label="PR")

    result = out["result"].rename(columns=lambda c: c.replace("left_", "gstr2b_").replace("right_", "pr_"))
    result = result.rename(columns={"gstr2b_ref": "gstr2b_recipient_gstin", "pr_ref": "pr_recipient_gstin"})
    result.insert(0, "period", period)
    summary = out["summary"].rename(columns={"left_amount": "gstr2b_amount", "right_amount": "pr_amount"})
    summary.insert(0, "period", period)

    print(f"[{period}] 2B vs PR: {len(gstr2b_df)} 2B rows, {len(pr_ok)} PR rows")
    print(summary[["match_level", "records"]].to_string(index=False))
    return {"result": result, "summary": summary}
