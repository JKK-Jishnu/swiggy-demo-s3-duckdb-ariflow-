-- =====================================================================================================
-- VIEW   : swiggy_gst.v_itc_at_risk
-- WHAT   : Every purchase line whose ITC is not (fully) supported by GSTR-2B
-- =====================================================================================================
CREATE OR REPLACE VIEW swiggy_gst.v_itc_at_risk AS
SELECT
    period, pr_record_id, gstr2b_record_id, recipient_gstin, supplier_gstin, supplier_name,
    pr_invoice_number, pr_invoice_date, pr_taxable_value, pr_itc_claimed, gstr2b_tax,
    itc_at_risk, match_level, remark
FROM swiggy_gst.gstr2b_vs_pr_reco
WHERE itc_at_risk > 0
