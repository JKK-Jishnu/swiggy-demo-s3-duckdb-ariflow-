"""
transformation.py — Swiggy Combine Batches
Activity : 4 — combine all batch outputs (B1..Bx) into one sales dataset
Stack    : DuckDB
"""
import duckdb
import pandas as pd


def run(dfs: list, period: str = None) -> dict:
    """
    Input : list of per-batch DataFrames (after error fixing)
    Output: {"result": combined rows, "duplicates": invoice numbers seen in more than one row,
             "summary": per batch counts}
    """
    con = duckdb.connect()
    combined = pd.concat(dfs, ignore_index=True)
    con.register("combined", combined)

    duplicates = con.execute("""
        SELECT invoice_number, CAST(count(*) AS INTEGER) AS occurrences,
               string_agg(batch_id, ', ' ORDER BY batch_id) AS batches
        FROM combined GROUP BY invoice_number HAVING count(*) > 1
        ORDER BY invoice_number
    """).df()

    summary = con.execute("""
        SELECT batch_id,
               CAST(count(*) AS INTEGER)                               AS total_rows,
               CAST(count_if(validation_status = 'Success') AS INTEGER) AS success,
               CAST(count_if(validation_status = 'Error') AS INTEGER)   AS errors,
               round(sum(taxable_value), 2)                            AS taxable_value
        FROM combined GROUP BY batch_id ORDER BY batch_id
    """).df()
    con.close()

    print(f"[{period}] Combined {len(combined)} rows from {len(dfs)} batches, "
          f"{len(duplicates)} duplicate invoice numbers")
    return {"result": combined, "duplicates": duplicates, "summary": summary}
