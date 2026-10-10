-- =====================================================================================================
-- VIEW   : swiggy_gst.v_purchase_record_trace
-- WHAT   : One row per purchase register Record ID: validation result and its GSTR-2B match
-- USE    : SELECT * FROM swiggy_gst.v_purchase_record_trace WHERE record_id = 'pr-202506-1'
-- =====================================================================================================
CREATE OR REPLACE VIEW swiggy_gst.v_purchase_record_trace AS
SELECT
    p.period, p.record_id, p.source_row_number, p.recipient_gstin, p.supplier_gstin,
    p.supplier_legal_name, p.invoice_number, p.invoice_date, p.taxable_value, p.itc_total,
    p.validation_status, p.validation_remark,
    r.match_level         AS gstr2b_match_level,
    r.gstr2b_record_id, r.gstr2b_invoice_number, r.gstr2b_taxable_value, r.gstr2b_tax,
    r.itc_at_risk, r.remark AS reco_remark
FROM swiggy_gst.pr_validated p
LEFT JOIN swiggy_gst.gstr2b_vs_pr_reco r
       ON r.period = p.period AND r.pr_record_id = p.record_id
