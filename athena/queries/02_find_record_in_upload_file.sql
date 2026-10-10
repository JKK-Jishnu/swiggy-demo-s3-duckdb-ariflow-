-- Which upload file rows contain this Record ID? Summed sections (b2cs, hsn, docs) list many IDs
-- in record_ids, so search inside the list.
SELECT period, batch_id, section, document_number, place_of_supply, hsn, rate, taxable_value, record_ids
FROM swiggy_gst.gstr1_upload_file
WHERE contains(split(record_ids, ' | '), 'sales-202506-b2c-3')
ORDER BY period, section
