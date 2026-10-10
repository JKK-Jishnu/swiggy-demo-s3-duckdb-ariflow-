-- =====================================================================================================
-- VIEW   : swiggy_gst.v_sales_record_trace
-- WHAT   : One row per sales Record ID showing its whole journey:
--          first validation -> client correction -> final status -> upload file section
--          -> EINV match -> EWB match
-- USE    : SELECT * FROM swiggy_gst.v_sales_record_trace WHERE record_id = 'sales-202506-b2b-1'
-- =====================================================================================================
CREATE OR REPLACE VIEW swiggy_gst.v_sales_record_trace AS
SELECT
    s.period, s.record_id, s.batch_id, s.source_row_number,
    s.invoice_type, s.invoice_number, s.invoice_date, s.receiver_gstin,
    s.taxable_value, s.total_invoice_value,
    v.validation_status   AS first_validation_status,
    v.validation_remark   AS first_validation_remark,
    s.correction_applied, s.corrected_fields, s.correction_remark,
    s.validation_status   AS final_status,
    s.validation_remark   AS final_remark,
    u.section             AS upload_section,
    u.record_ids          AS upload_row_record_ids,
    e.match_level         AS einv_match_level,
    e.einv_record_id, e.irn,
    w.match_level         AS ewb_match_level,
    w.ewb_record_id, w.ewb_no
FROM swiggy_gst.sales_combined s
LEFT JOIN swiggy_gst.gstr1_validated v
       ON v.period = s.period AND v.record_id = s.record_id
LEFT JOIN swiggy_gst.gstr1_upload_file u
       ON u.period = s.period
      AND u.section NOT IN ('hsn', 'docs')
      AND contains(split(u.record_ids, ' | '), s.record_id)
LEFT JOIN swiggy_gst.einv_vs_sales_reco e
       ON e.period = s.period AND e.sales_record_id = s.record_id
LEFT JOIN swiggy_gst.ewb_vs_sales_reco w
       ON w.period = s.period AND w.sales_record_id = s.record_id
