-- Sales lines still in error after client corrections (these were not uploaded to GSTN).
SELECT period, batch_id, record_id, source_row_number, invoice_number, invoice_type,
       taxable_value, validation_remark
FROM swiggy_gst.sales_combined
WHERE validation_status = 'Error'
ORDER BY period, batch_id, source_row_number
