-- Share of lines matched exactly (L1) per reconciliation and month.
SELECT
    period,
    reconciliation,
    sum(records)                                                         AS total_rows,
    sum(CASE WHEN level_no = 1 THEN records ELSE 0 END)                  AS exact_matches,
    round(100.0 * sum(CASE WHEN level_no = 1 THEN records ELSE 0 END) / sum(records), 1) AS exact_match_pct,
    round(sum(itc_at_risk), 2)                                           AS itc_at_risk
FROM swiggy_gst.v_reco_monthly_summary
GROUP BY period, reconciliation
ORDER BY reconciliation, period
