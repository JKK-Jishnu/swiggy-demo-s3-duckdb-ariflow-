-- Follow one sales line from the client file to the upload file and the reconciliations.
-- Change the Record ID to the one you are looking for.
SELECT *
FROM swiggy_gst.v_sales_record_trace
WHERE record_id = 'sales-202506-b2b-1'
