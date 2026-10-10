"""
transformation.py — Swiggy EWB vs Sales Reconciliation
Activity : 6 — e-way bill data from Bolton vs combined sales (runs in parallel with EINV reco)
Ladder   : exact → GSTIN+invoice (amount differs) → GSTIN+amount (invoice no differs) → unmatched
"""
import pandas as pd

from activities.common.sales_reco import reco_against_sales

EWB_MAPPING = {
    "gstin"   : "To GSTIN",
    "inv_no"  : "Doc No",
    "inv_date": "Doc Date",
    "amount"  : "Assessable Value",
    "ref"     : "EWB No",
}

# E-way bill is mandatory only above Rs.50,000 consignment value
SALES_FILTER = "invoice_type IN ('B2B', 'CDNR') AND total_invoice_value > 50000"


def run(ewb_df: pd.DataFrame, sales_df: pd.DataFrame, period: str = None) -> dict:
    return reco_against_sales(ewb_df, sales_df, EWB_MAPPING, "EWB", SALES_FILTER, period)
