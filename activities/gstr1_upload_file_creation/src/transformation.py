"""
transformation.py — Swiggy GSTR1 Upload File Creation
Activity : 3 — convert validated rows into GSTN offline-tool sheets (B2B, B2CS, CDNR, CDNUR, EXP)
Stack    : DuckDB + pandas
"""
import duckdb
import pandas as pd

from activities.common.gst_utils import parse_dates, place_of_supply

GST_RATE = 18.0

B2B_COLS   = ["GSTIN/UIN of Recipient", "Receiver Name", "Invoice Number", "Invoice date", "Invoice Value",
              "Place Of Supply", "Reverse Charge", "Applicable % of Tax Rate", "Invoice Type",
              "E-Commerce GSTIN", "Rate", "Taxable Value", "Cess Amount"]
B2CS_COLS  = ["Type", "Place Of Supply", "Rate", "Applicable % of Tax Rate", "Taxable Value",
              "Cess Amount", "E-Commerce GSTIN"]
CDNR_COLS  = ["GSTIN/UIN of Recipient", "Receiver Name", "Note Number", "Note Date", "Note Type",
              "Place Of Supply", "Reverse Charge", "Note Supply Type", "Note Value",
              "Applicable % of Tax Rate", "Rate", "Taxable Value", "Cess Amount"]
CDNUR_COLS = ["UR Type", "Note Number", "Note Date", "Note Type", "Place Of Supply", "Note Value",
              "Applicable % of Tax Rate", "Rate", "Taxable Value", "Cess Amount"]
EXP_COLS   = ["Export Type", "Invoice Number", "Invoice date", "Invoice Value", "Port Code",
              "Shipping Bill Number", "Shipping Bill Date", "Rate", "Taxable Value", "Cess Amount"]


def run(df: pd.DataFrame, batch_id: str = "B1", period: str = None) -> dict:
    """
    Input : rows after validation/error fixing (only validation_status == 'Success' are uploaded)
    Output: {"sheets": {sheet_name: df}, "summary": rows/taxable per sheet}
    """
    ok = df[df["validation_status"] == "Success"].copy()
    ok["pos"]      = ok["receiver_gstin"].map(place_of_supply)
    ok["doc_date"] = parse_dates(ok["invoice_date"]).dt.strftime("%d-%b-%Y")
    ok["rate"]     = GST_RATE

    con = duckdb.connect()
    con.register("ok", ok)

    b2b = con.execute("""
        SELECT receiver_gstin AS "GSTIN/UIN of Recipient", '' AS "Receiver Name",
               invoice_number AS "Invoice Number", doc_date AS "Invoice date",
               total_invoice_value AS "Invoice Value", pos AS "Place Of Supply",
               'N' AS "Reverse Charge", NULL AS "Applicable % of Tax Rate",
               'Regular B2B' AS "Invoice Type", '' AS "E-Commerce GSTIN",
               rate AS "Rate", taxable_value AS "Taxable Value", 0 AS "Cess Amount"
        FROM ok WHERE invoice_type = 'B2B' ORDER BY invoice_number
    """).df()

    # B2C small: aggregated by place of supply + rate, no invoice detail
    b2cs = con.execute("""
        SELECT 'OE' AS "Type", pos AS "Place Of Supply", rate AS "Rate",
               NULL AS "Applicable % of Tax Rate", round(sum(taxable_value), 2) AS "Taxable Value",
               0 AS "Cess Amount", '' AS "E-Commerce GSTIN"
        FROM ok WHERE invoice_type = 'B2C' GROUP BY pos, rate ORDER BY pos
    """).df()

    cdnr = con.execute("""
        SELECT receiver_gstin AS "GSTIN/UIN of Recipient", '' AS "Receiver Name",
               invoice_number AS "Note Number", doc_date AS "Note Date", 'C' AS "Note Type",
               pos AS "Place Of Supply", 'N' AS "Reverse Charge", 'Regular B2B' AS "Note Supply Type",
               total_invoice_value AS "Note Value", NULL AS "Applicable % of Tax Rate",
               rate AS "Rate", taxable_value AS "Taxable Value", 0 AS "Cess Amount"
        FROM ok WHERE invoice_type = 'CDNR' ORDER BY invoice_number
    """).df()

    cdnur = con.execute("""
        SELECT 'B2CL' AS "UR Type", invoice_number AS "Note Number", doc_date AS "Note Date",
               'C' AS "Note Type", pos AS "Place Of Supply", total_invoice_value AS "Note Value",
               NULL AS "Applicable % of Tax Rate", rate AS "Rate",
               taxable_value AS "Taxable Value", 0 AS "Cess Amount"
        FROM ok WHERE invoice_type = 'CDNUR' ORDER BY invoice_number
    """).df()

    exp = con.execute("""
        SELECT 'WPAY' AS "Export Type", invoice_number AS "Invoice Number", doc_date AS "Invoice date",
               total_invoice_value AS "Invoice Value", '' AS "Port Code", '' AS "Shipping Bill Number",
               '' AS "Shipping Bill Date", rate AS "Rate", taxable_value AS "Taxable Value",
               0 AS "Cess Amount"
        FROM ok WHERE invoice_type = 'EXP' ORDER BY invoice_number
    """).df()
    con.close()

    sheets = {"b2b": b2b, "b2cs": b2cs, "cdnr": cdnr, "cdnur": cdnur, "exp": exp}
    summary = pd.DataFrame([
        {"period": period, "batch_id": batch_id, "sheet": name, "rows": len(s),
         "taxable_value": round(float(s["Taxable Value"].sum()), 2) if len(s) else 0.0}
        for name, s in sheets.items()
    ])
    print(f"[{batch_id}] Upload file: " + ", ".join(f"{n}={len(s)}" for n, s in sheets.items()))
    return {"sheets": sheets, "summary": summary}
