"""
steps.py — S3-backed pipeline steps, one function per task.
Each step reads its inputs from S3, calls an activity's transformation.run(), writes outputs
back to S3 and returns a small JSON-able dict (used as Airflow XCom and by run_demo.py).
Steps only exchange data through S3, so they can run in any process / Airflow worker.
"""
import json

import duckdb
import pandas as pd

from pipeline.write_to_s3 import (BUCKET, exists, list_keys, read_excel, read_parquet, s3_client,
                                  write_excel, write_parquet)


# ---------------------------------------------------------------- paths
def raw(p, *parts):          return "/".join(["raw", p, *parts])
def intermediate(p, *parts): return "/".join(["intermediate", p, *parts])
def final(p, *parts):        return "/".join(["final", p, *parts])


def read_manifest(period: str) -> dict:
    obj = s3_client().get_object(Bucket=BUCKET, Key=raw(period, "gstr1", "manifest.json"))
    return json.loads(obj["Body"].read())


# ---------------------------------------------------------------- GSTR1 per batch (DAG 1)
def validate_batch(period: str, batch_id: str) -> dict:
    from activities.gstr1_validation.src.transformation import run

    df  = read_excel(raw(period, "gstr1", batch_id, f"gstr1_{batch_id}.xlsx"))
    out = run(df, batch_id=batch_id, period=period)
    write_parquet(out["result"], intermediate(period, "gstr1", f"{batch_id}_validated.parquet"))
    # Activity 2a: export errors so the client can send corrections
    write_excel({"Errors": out["errors"]}, final(period, "gstr1", f"{batch_id}_errors_for_client.xlsx"))
    return {"rows": len(out["result"]), "success": len(out["success"]), "errors": len(out["errors"])}


def fix_batch(period: str, batch_id: str) -> dict:
    from activities.gstr1_error_fixing.src.transformation import run

    validated = read_parquet(intermediate(period, "gstr1", f"{batch_id}_validated.parquet"))
    corr_key  = raw(period, "gstr1", batch_id, f"corrections_{batch_id}.xlsx")
    corrections = read_excel(corr_key) if exists(corr_key) else None

    out = run(validated, corrections, batch_id=batch_id, period=period)
    write_parquet(out["result"], intermediate(period, "gstr1", f"{batch_id}_fixed.parquet"))
    return {"fixed": len(out["fixed"]), "still_errors": len(out["still_errors"])}


def create_upload_file(period: str, batch_id: str) -> dict:
    from activities.gstr1_upload_file_creation.src.transformation import run

    df  = read_parquet(intermediate(period, "gstr1", f"{batch_id}_fixed.parquet"))
    out = run(df, batch_id=batch_id, period=period)
    write_excel(out["sheets"], final(period, "gstr1", f"{batch_id}_upload_file.xlsx"))
    return {name: len(s) for name, s in out["sheets"].items()}


def mark_batch_done(period: str, batch_id: str) -> None:
    s3_client().put_object(Bucket=BUCKET, Key=f"batch_status/{period}/{batch_id}.done", Body=b"done")


def batch_progress(period: str) -> tuple:
    done  = len(list_keys(f"batch_status/{period}/", ".done"))
    total = read_manifest(period)["total_batches"]
    return done, total


# ---------------------------------------------------------------- GSTR1 combine + reco (DAG 2)
def combine(period: str) -> dict:
    from activities.combine_batches.src.transformation import run

    keys = list_keys(intermediate(period, "gstr1") + "/", "_fixed.parquet")
    out  = run([read_parquet(k) for k in keys], period=period)
    write_parquet(out["result"], intermediate(period, "combined_sales.parquet"))
    return {"batches": len(keys), "rows": len(out["result"]), "duplicates": len(out["duplicates"])}


def _sales_reco(period: str, name: str, raw_key: str, run) -> dict:
    source = read_excel(raw_key)
    write_parquet(source, intermediate(period, f"{name}.parquet"))
    sales = read_parquet(intermediate(period, "combined_sales.parquet"))
    out   = run(source, sales, period=period)
    write_parquet(out["result"], intermediate(period, "reco", f"{name}_vs_sales.parquet"))
    write_excel({"Summary": out["summary"], "Reco": out["result"]}, final(period, f"{name}_vs_sales_reco.xlsx"))
    return dict(zip(out["summary"]["match_level"], out["summary"]["records"].astype(int).tolist()))


def einv_reco(period: str) -> dict:
    from activities.einv_vs_sales_reco.src.transformation import run
    return _sales_reco(period, "einv", raw(period, "einv_bolton", "einv_bolton.xlsx"), run)


def ewb_reco(period: str) -> dict:
    from activities.ewb_vs_sales_reco.src.transformation import run
    return _sales_reco(period, "ewb", raw(period, "ewb_bolton", "ewb_bolton.xlsx"), run)


def _reco_summary(con, table: str) -> pd.DataFrame:
    return con.execute(f"""
        SELECT match_level, CAST(count(*) AS INTEGER) AS records, round(sum(amount_diff), 2) AS amount_diff
        FROM {table} GROUP BY match_level ORDER BY match_level
    """).df()


def gstr1_summary(period: str) -> dict:
    combined = read_parquet(intermediate(period, "combined_sales.parquet"))
    einv     = read_parquet(intermediate(period, "reco", "einv_vs_sales.parquet"))
    ewb      = read_parquet(intermediate(period, "reco", "ewb_vs_sales.parquet"))

    con = duckdb.connect()
    for name, df in [("combined", combined), ("einv", einv), ("ewb", ewb)]:
        con.register(name, df)
    validation = con.execute("""
        SELECT batch_id,
               CAST(count(*) AS INTEGER)                                AS total,
               CAST(count_if(status_before_fix = 'Error') AS INTEGER)   AS errors_found,
               CAST(count_if(status_before_fix = 'Error'
                             AND validation_status = 'Success') AS INTEGER) AS fixed_by_client,
               CAST(count_if(validation_status = 'Error') AS INTEGER)   AS open_errors,
               round(sum(taxable_value), 2)                             AS taxable_value
        FROM combined GROUP BY batch_id ORDER BY batch_id
    """).df()
    sheets = {
        "Validation"  : validation,
        "EINV Reco"   : _reco_summary(con, "einv"),
        "EWB Reco"    : _reco_summary(con, "ewb"),
        "Open Errors" : combined[combined["validation_status"] == "Error"],
    }
    con.close()
    write_excel(sheets, final(period, "summaries", "gstr1_summary.xlsx"))
    print(validation.to_string(index=False))
    return {"rows": len(combined), "open_errors": int(validation["open_errors"].sum())}


# ---------------------------------------------------------------- PR / 2B track (DAG 3)
def pr_validation(period: str) -> dict:
    from activities.pr_validation.src.transformation import run

    out = run(read_excel(raw(period, "pr", "purchase_register.xlsx")), period=period)
    write_parquet(out["result"], intermediate(period, "pr_validated.parquet"))
    write_excel({"Errors": out["errors"]}, final(period, "pr_errors_for_client.xlsx"))
    return {"rows": len(out["result"]), "success": len(out["success"]), "errors": len(out["errors"])}


def gstr2b_consolidation(period: str) -> dict:
    from activities.gstr2b_consolidation.src.transformation import run

    keys  = list_keys(raw(period, "2b_bolton") + "/", ".xlsx")
    files = {k: read_excel(k, sheet_name=None) for k in keys}
    out   = run(files, period=period)
    write_parquet(out["result"], intermediate(period, "2b_consolidated.parquet"))
    return {"files": len(keys), "rows": len(out["result"])}


def gstr2b_pr_reco(period: str) -> dict:
    from activities.gstr2b_vs_pr_reco.src.transformation import run

    out = run(read_parquet(intermediate(period, "2b_consolidated.parquet")),
              read_parquet(intermediate(period, "pr_validated.parquet")), period=period)
    write_parquet(out["result"], intermediate(period, "reco", "2b_vs_pr.parquet"))
    write_excel({"Summary": out["summary"], "Reco": out["result"]}, final(period, "2b_vs_pr_reco.xlsx"))
    return dict(zip(out["summary"]["match_level"], out["summary"]["records"].astype(int).tolist()))


def pr_2b_summary(period: str) -> dict:
    pr   = read_parquet(intermediate(period, "pr_validated.parquet"))
    reco = read_parquet(intermediate(period, "reco", "2b_vs_pr.parquet"))

    con = duckdb.connect()
    con.register("pr", pr)
    con.register("reco", reco)
    validation = con.execute("""
        SELECT recipient_gstin,
               CAST(count(*) AS INTEGER)                              AS total,
               CAST(count_if(validation_status = 'Error') AS INTEGER) AS errors,
               round(sum(itc_claimed), 2)                             AS itc_claimed
        FROM pr GROUP BY recipient_gstin ORDER BY recipient_gstin
    """).df()
    # ITC at risk = tax on invoices booked in PR but not reported by the vendor in 2B
    at_risk = con.execute("""
        SELECT r.pr_inv_no AS invoice_number, r.pr_gstin AS supplier_gstin,
               p.supplier_name, p.itc_claimed
        FROM reco r JOIN pr p ON p.invoice_number = r.pr_inv_no AND p.supplier_gstin = r.pr_gstin
        WHERE r.match_level = 'Only in PR'
    """).df()
    sheets = {"PR Validation": validation, "2B Reco": _reco_summary(con, "reco"), "ITC at Risk": at_risk}
    con.close()
    write_excel(sheets, final(period, "summaries", "pr_2b_summary.xlsx"))
    print(validation.to_string(index=False))
    return {"pr_errors": int(validation["errors"].sum()),
            "itc_at_risk": round(float(at_risk["itc_claimed"].sum()), 2)}


# ---------------------------------------------------------------- history across months
def available_periods() -> list:
    return sorted({k.split("/")[1] for k in list_keys("intermediate/", "combined_sales.parquet")})


def cross_month_analysis(periods: list = None) -> pd.DataFrame:
    """One DuckDB query across every month's parquet output in S3."""
    periods = periods or available_periods()
    con = duckdb.connect()
    for table, key in [("sales", "combined_sales.parquet"), ("einv", "reco/einv_vs_sales.parquet"),
                       ("ewb", "reco/ewb_vs_sales.parquet"), ("pr", "pr_validated.parquet"),
                       ("b2", "reco/2b_vs_pr.parquet")]:
        frames = [read_parquet(intermediate(p, key)) for p in periods if exists(intermediate(p, key))]
        con.register(table, pd.concat(frames, ignore_index=True))

    return con.execute("""
        WITH s AS (
            SELECT period, CAST(count(DISTINCT batch_id) AS INTEGER) AS batches, CAST(count(*) AS INTEGER) AS invoices,
                   CAST(count_if(status_before_fix = 'Error') AS INTEGER) AS errors_found,
                   CAST(count_if(validation_status = 'Error') AS INTEGER) AS open_errors,
                   round(sum(taxable_value) / 1e5, 2) AS taxable_lakhs
            FROM sales GROUP BY period),
        e AS (SELECT period, round(100.0 * count_if(match_level = 'L1 Exact') / count(*), 1) AS einv_exact_pct
              FROM einv GROUP BY period),
        w AS (SELECT period, round(100.0 * count_if(match_level = 'L1 Exact') / count(*), 1) AS ewb_exact_pct
              FROM ewb GROUP BY period),
        p AS (SELECT period, CAST(count(*) AS INTEGER) AS pr_rows, CAST(count_if(validation_status = 'Error') AS INTEGER) AS pr_errors
              FROM pr GROUP BY period),
        b AS (SELECT period, round(100.0 * count_if(match_level = 'L1 Exact') / count(*), 1) AS gstr2b_exact_pct,
                     CAST(count_if(match_level = 'Only in PR') AS INTEGER) AS missing_in_2b
              FROM b2 GROUP BY period)
        SELECT * FROM s
        LEFT JOIN e USING (period) LEFT JOIN w USING (period)
        LEFT JOIN p USING (period) LEFT JOIN b USING (period)
        ORDER BY period
    """).df()
