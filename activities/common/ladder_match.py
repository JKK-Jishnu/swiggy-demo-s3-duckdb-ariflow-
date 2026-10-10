"""
ladder_match.py — multi-level (ladder) reconciliation in DuckDB.

Both sides are mapped by the caller to standard columns:
    gstin, inv_no, inv_date, amount, ref (optional free-text reference, e.g. IRN / EWB no)

Each level joins only rows still unmatched after the previous levels, so a pair is
always reported at the strictest level it qualifies for. Within a level, matching is
1-to-1: each side picks its closest-amount candidate and only mutual picks are kept.
"""
import duckdb
import pandas as pd

from activities.common.gst_utils import normalise_invoice_no

STD_COLS = ["gstin", "inv_no", "inv_date", "amount", "ref"]

DEFAULT_LEVELS = [
    {"name": "L1 Exact",                    "keys": ["gstin", "inv_norm"], "amount_tol": 1.0},
    {"name": "L2 GSTIN+Invoice (amt diff)", "keys": ["gstin", "inv_norm"], "amount_tol": None},
    {"name": "L3 GSTIN+Amount (inv diff)",  "keys": ["gstin"],             "amount_tol": 1.0},
]


def _prepare(df: pd.DataFrame, id_col: str) -> pd.DataFrame:
    out = df.copy()
    if "ref" not in out.columns:
        out["ref"] = ""
    out = out[STD_COLS].reset_index(drop=True)
    out["gstin"]    = out["gstin"].fillna("").astype(str).str.strip().str.upper()
    out["inv_no"]   = out["inv_no"].fillna("").astype(str).str.strip()
    out["inv_date"] = out["inv_date"].fillna("").astype(str)
    out["ref"]      = out["ref"].fillna("").astype(str)
    out["amount"]   = pd.to_numeric(out["amount"], errors="coerce").fillna(0.0).astype(float)
    out["inv_norm"] = normalise_invoice_no(out["inv_no"])
    out[id_col]     = range(len(out))
    return out


def ladder_match(left: pd.DataFrame, right: pd.DataFrame, levels: list = None,
                 left_label: str = "LEFT", right_label: str = "RIGHT") -> dict:
    """
    Returns {"result": reco rows, "summary": counts per match_level}
    result columns: match_level, left_*, right_*, amount_diff, remark
    """
    levels = levels or DEFAULT_LEVELS
    con = duckdb.connect()
    con.register("l", _prepare(left, "_lid"))
    con.register("r", _prepare(right, "_rid"))
    con.execute("CREATE TABLE matches (_lid BIGINT, _rid BIGINT, match_level VARCHAR, lvl INTEGER)")

    for i, level in enumerate(levels, start=1):
        on = " AND ".join(f"l.{k} = r.{k}" for k in level["keys"])
        if level.get("amount_tol") is not None:
            on += f" AND abs(l.amount - r.amount) <= {float(level['amount_tol'])}"
        con.execute(f"""
            INSERT INTO matches
            SELECT _lid, _rid, '{level["name"]}', {i} FROM (
                SELECT l._lid, r._rid,
                       row_number() OVER (PARTITION BY l._lid ORDER BY abs(l.amount - r.amount), r._rid) AS rn_l,
                       row_number() OVER (PARTITION BY r._rid ORDER BY abs(l.amount - r.amount), l._lid) AS rn_r
                FROM l JOIN r ON {on}
                WHERE l._lid NOT IN (SELECT _lid FROM matches)
                  AND r._rid NOT IN (SELECT _rid FROM matches)
            ) WHERE rn_l = 1 AND rn_r = 1
        """)

    only_left  = f"Only in {left_label}"
    only_right = f"Only in {right_label}"
    result = con.execute(f"""
        WITH paired AS (
            SELECT m.match_level, m.lvl,
                   l.gstin AS left_gstin, l.inv_no AS left_inv_no, l.inv_date AS left_inv_date,
                   l.amount AS left_amount, l.ref AS left_ref,
                   r.gstin AS right_gstin, r.inv_no AS right_inv_no, r.inv_date AS right_inv_date,
                   r.amount AS right_amount, r.ref AS right_ref
            FROM matches m JOIN l USING (_lid) JOIN r USING (_rid)
            UNION ALL
            SELECT '{only_left}', 98,
                   gstin, inv_no, inv_date, amount, ref,
                   NULL, NULL, NULL, NULL, NULL
            FROM l WHERE _lid NOT IN (SELECT _lid FROM matches)
            UNION ALL
            SELECT '{only_right}', 99,
                   NULL, NULL, NULL, NULL, NULL,
                   gstin, inv_no, inv_date, amount, ref
            FROM r WHERE _rid NOT IN (SELECT _rid FROM matches)
        )
        SELECT * EXCLUDE (lvl),
               round(coalesce(left_amount, 0) - coalesce(right_amount, 0), 2) AS amount_diff,
               CASE
                   WHEN lvl = 1  THEN 'Matched'
                   WHEN lvl = 98 THEN 'Missing in {right_label}'
                   WHEN lvl = 99 THEN 'Missing in {left_label}'
                   WHEN left_inv_no <> right_inv_no AND upper(regexp_replace(left_inv_no, '[^0-9A-Za-z]', '', 'g'))
                                                     <> upper(regexp_replace(right_inv_no, '[^0-9A-Za-z]', '', 'g'))
                        THEN 'Invoice no differs: ' || left_inv_no || ' vs ' || right_inv_no
                   ELSE 'Amount differs by ' || CAST(round(left_amount - right_amount, 2) AS VARCHAR)
               END AS remark
        FROM paired
        ORDER BY lvl, coalesce(left_inv_no, right_inv_no)
    """).df()

    summary = con.execute("""
        SELECT match_level,
               CAST(count(*) AS INTEGER)                  AS records,
               round(sum(coalesce(left_amount, 0)), 2)    AS left_amount,
               round(sum(coalesce(right_amount, 0)), 2)   AS right_amount,
               round(sum(amount_diff), 2)                 AS amount_diff
        FROM result GROUP BY match_level ORDER BY match_level
    """).df()
    con.close()
    return {"result": result, "summary": summary}
