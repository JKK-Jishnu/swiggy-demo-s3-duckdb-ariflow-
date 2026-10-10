-- Follow one purchase register line: validation result and its GSTR-2B match.
SELECT *
FROM swiggy_gst.v_purchase_record_trace
WHERE record_id = 'pr-202506-1'
