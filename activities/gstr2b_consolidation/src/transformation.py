"""
transformation.py — Swiggy GSTR-2B Consolidation
Activity : 8 — many Bolton 2B workbooks (one per Swiggy GSTIN), each with B2B / CDNR sheets
           whose headers differ → one consolidated table
Stack    : pandas + DuckDB
"""
import os
import re

import duckdb
import pandas as pd

HEADER_MAP = {
    "GSTIN of supplier": "supplier_gstin", "Trade/Legal name": "supplier_name",
    # B2B sheet
    "Invoice number": "invoice_number", "Invoice Date": "invoice_date",
    "Invoice Value": "invoice_value", "Invoice type": "document_subtype",
    # CDNR sheet
    "Note number": "invoice_number", "Note date": "invoice_date",
    "Note Value": "invoice_value", "Note type": "document_subtype",
    # common amounts
    "Taxable Value": "taxable_value", "Integrated Tax": "igst", "Central Tax": "cgst",
    "State/UT Tax": "sgst", "ITC Availability": "itc_availability",
}
OUTPUT_COLS = ["recipient_gstin", "doc_type", "document_subtype", "supplier_gstin", "supplier_name",
               "invoice_number", "invoice_date", "invoice_value", "taxable_value", "igst", "cgst",
               "sgst", "itc_availability", "source_file"]
GSTIN_IN_NAME = re.compile(r"([0-9]{2}[A-Z0-9]{13})")


def run(files: dict, period: str = None) -> dict:
    """
    Input : {file_name: {sheet_name: DataFrame}}
    Output: {"result": consolidated rows, "summary": rows/taxable per file+sheet}
    """
    frames = []
    for file_name, sheets in files.items():
        base  = os.path.basename(file_name)
        match = GSTIN_IN_NAME.search(base)
        recipient = match.group(1) if match else ""
        for sheet_name, df in sheets.items():
            if df.empty:
                continue
            unknown = [c for c in df.columns if c not in HEADER_MAP]
            if unknown:
                print(f"  {base}/{sheet_name}: ignoring unmapped columns {unknown}")
            d = df.rename(columns=HEADER_MAP)
            d = d[[c for c in OUTPUT_COLS if c in d.columns]].copy()
            d["recipient_gstin"] = recipient
            d["doc_type"]        = sheet_name.upper()
            d["source_file"]     = base
            frames.append(d.reindex(columns=OUTPUT_COLS))

    result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=OUTPUT_COLS)
    for col in ["invoice_value", "taxable_value", "igst", "cgst", "sgst"]:
        result[col] = pd.to_numeric(result[col], errors="coerce").fillna(0.0)
    result["period"] = period

    con = duckdb.connect()
    con.register("b2", result)
    summary = con.execute("""
        SELECT source_file, doc_type, CAST(count(*) AS INTEGER) AS rows,
               round(sum(taxable_value), 2) AS taxable_value,
               round(sum(igst + cgst + sgst), 2) AS total_tax
        FROM b2 GROUP BY source_file, doc_type ORDER BY source_file, doc_type
    """).df()
    con.close()

    print(f"[2B {period}] Consolidated {len(result)} rows from {len(files)} files")
    return {"result": result, "summary": summary}
