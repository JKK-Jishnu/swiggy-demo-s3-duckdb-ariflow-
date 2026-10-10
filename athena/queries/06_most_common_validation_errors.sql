-- Which GSTR-1 rules fail most often in the first upload of each month.
SELECT
    period,
    split_part(failed_rule, ' ', 1)  AS rule_code,
    count(*)                         AS lines
FROM swiggy_gst.gstr1_validated
CROSS JOIN UNNEST(split(validation_remark, ' | ')) AS t(failed_rule)
WHERE validation_status = 'Error'
GROUP BY period, split_part(failed_rule, ' ', 1)
ORDER BY period, lines DESC
