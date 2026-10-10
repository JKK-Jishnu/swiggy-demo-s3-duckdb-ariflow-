"""
transformation.py — Swiggy EINV vs Sales Reconciliation
Activity : 5 — e-invoice (IRN) data from Bolton vs combined sales
Ladder   : exact → GSTIN+invoice (amount differs) → GSTIN+amount (invoice no differs) → unmatched
"""
import pandas as pd

from activities.common.sales_reco import reco_against_sales

EINV_MAPPING = {
    "gstin"   : "Buyer GSTIN",
    "inv_no"  : "Document No",
    "inv_date": "Document Date",
    "amount"  : "Taxable Value",
    "ref"     : "IRN",
}

# Only registered-buyer documents need an e-invoice
SALES_FILTER = "invoice_type IN ('B2B', 'CDNR', 'CDNUR', 'EXP')"


def run(einv_df: pd.DataFrame, sales_df: pd.DataFrame, period: str = None) -> dict:
    return reco_against_sales(einv_df, sales_df, EINV_MAPPING, "EINV", SALES_FILTER, period)
