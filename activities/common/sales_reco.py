"""
sales_reco.py — shared plumbing for reconciling a Bolton source (EINV / EWB) against combined sales.
"""
import duckdb
import pandas as pd

from activities.common.ladder_match import ladder_match


def map_columns(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """mapping: {standard_col: source_col}"""
    missing = [src for src in mapping.values() if src not in df.columns]
    if missing:
        raise ValueError(f"Source file is missing columns: {missing}")
    return pd.DataFrame({std: df[src].values for std, src in mapping.items()})


def reco_against_sales(source_df: pd.DataFrame, sales_df: pd.DataFrame, source_mapping: dict,
                       source_label: str, sales_filter_sql: str, period: str = None) -> dict:
    """
    LEFT = Bolton source, RIGHT = sales rows that should appear in it (filtered with sales_filter_sql).
    Returns {"result", "summary"} with left_/right_ columns renamed to <label>_/sales_.
    """
    con = duckdb.connect()
    con.register("sales", sales_df)
    sales = con.execute(f"""
        SELECT receiver_gstin AS gstin, invoice_number AS inv_no, invoice_date AS inv_date,
               taxable_value AS amount, batch_id AS ref
        FROM sales WHERE validation_status = 'Success' AND ({sales_filter_sql})
    """).df()
    con.close()

    left = map_columns(source_df, source_mapping)
    out  = ladder_match(left, sales, left_label=source_label, right_label="Sales")

    prefix = source_label.lower()
    result = out["result"].rename(columns=lambda c: c.replace("left_", f"{prefix}_").replace("right_", "sales_"))
    result.insert(0, "period", period)
    summary = out["summary"].rename(columns={"left_amount": f"{prefix}_amount", "right_amount": "sales_amount"})
    summary.insert(0, "period", period)

    print(f"[{period}] {source_label} vs Sales: {len(left)} {source_label} rows, {len(sales)} sales rows")
    print(summary[["match_level", "records"]].to_string(index=False))
    return {"result": result, "summary": summary}
