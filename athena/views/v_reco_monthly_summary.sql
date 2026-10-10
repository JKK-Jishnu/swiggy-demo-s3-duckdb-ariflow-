-- =====================================================================================================
-- VIEW   : swiggy_gst.v_reco_monthly_summary
-- WHAT   : One row per month x reconciliation x match level, for all three reconciliations
-- =====================================================================================================
CREATE OR REPLACE VIEW swiggy_gst.v_reco_monthly_summary AS
SELECT period, 'EINV vs Sales' AS reconciliation, level_no, match_level,
       count(*) AS records, round(sum(taxable_value_difference), 2) AS taxable_value_difference,
       CAST(0 AS double) AS itc_at_risk
FROM swiggy_gst.einv_vs_sales_reco
GROUP BY period, level_no, match_level
UNION ALL
SELECT period, 'EWB vs Sales', level_no, match_level,
       count(*), round(sum(taxable_value_difference), 2), CAST(0 AS double)
FROM swiggy_gst.ewb_vs_sales_reco
GROUP BY period, level_no, match_level
UNION ALL
SELECT period, 'GSTR-2B vs PR', level_no, match_level,
       count(*), round(sum(taxable_value_difference), 2), round(sum(itc_at_risk), 2)
FROM swiggy_gst.gstr2b_vs_pr_reco
GROUP BY period, level_no, match_level
