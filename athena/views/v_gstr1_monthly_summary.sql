-- =====================================================================================================
-- VIEW   : swiggy_gst.v_gstr1_monthly_summary
-- WHAT   : One row per month - GSTR-1 lines, errors found, corrected, still open, value uploaded
-- =====================================================================================================
CREATE OR REPLACE VIEW swiggy_gst.v_gstr1_monthly_summary AS
SELECT
    period,
    count(DISTINCT batch_id)                                                     AS batches,
    count(*)                                                                     AS sales_lines,
    sum(CASE WHEN first_pass_status = 'Error' THEN 1 ELSE 0 END)                 AS errors_found,
    sum(CASE WHEN correction_applied THEN 1 ELSE 0 END)                          AS lines_corrected,
    sum(CASE WHEN validation_status = 'Error' THEN 1 ELSE 0 END)                 AS still_in_error,
    CAST(sum(CASE WHEN validation_status = 'Success' THEN taxable_value ELSE 0 END) AS decimal(18, 2)) AS taxable_value_uploaded,
    CAST(sum(CASE WHEN validation_status = 'Success'
                  THEN igst_amount + cgst_amount + sgst_amount + cess_amount ELSE 0 END) AS decimal(18, 2)) AS tax_uploaded
FROM swiggy_gst.sales_combined
GROUP BY period
