-- Suppliers whose invoices are booked but not (fully) reported in GSTR-2B, across all months.
SELECT
    supplier_gstin,
    supplier_name,
    count(*)                                    AS invoices,
    round(sum(itc_at_risk), 2)                  AS itc_at_risk,
    array_join(array_agg(pr_record_id), ' | ')  AS pr_record_ids
FROM swiggy_gst.v_itc_at_risk
GROUP BY supplier_gstin, supplier_name
ORDER BY itc_at_risk DESC
