-- =====================================================================================================
-- TABLE  : swiggy_gst.ewb_vs_sales_reco
-- SOURCE : Activity 6 - intermediate/ewb_vs_sales_reco/period=<YYYYMM>/*.parquet
-- WHAT   : E-way bill vs sales reconciliation, one row per matched pair or unmatched line
-- PERIOD : partition column "period" (YYYYMM) comes from the folder name; new months appear
--          automatically (partition projection) - no crawler or MSCK REPAIR needed.
-- =====================================================================================================
CREATE EXTERNAL TABLE swiggy_gst.ewb_vs_sales_reco (
    match_level               string     COMMENT 'L1 Exact, L2 Amount mismatch, L3 Invoice no mismatch, Only in <side>',
    level_no                  int        COMMENT '1-3 = matched at that ladder level, 4 = only in left source, 5 = only in right source',
    ewb_record_id             string     COMMENT 'Record ID of the EWB line (ewb-<period>-n)',
    sales_record_id           string     COMMENT 'Record ID of the sales line (sales-<period>-<type>-n)',
    sales_batch_id            string,
    ewb_no                    string,
    ewb_to_gstin              string,
    sales_receiver_gstin      string,
    ewb_document_number       string,
    sales_invoice_number      string,
    ewb_document_date         date,
    sales_invoice_date        date,
    ewb_taxable_value         double,
    sales_taxable_value       double,
    taxable_value_difference  double     COMMENT 'Left taxable value minus right taxable value',
    remark                    string     COMMENT 'Plain-English explanation of the match result'
)
COMMENT 'E-way bill vs sales reconciliation, one row per matched pair or unmatched line'
PARTITIONED BY (period string)
STORED AS PARQUET
LOCATION 's3://<BUCKET>/intermediate/ewb_vs_sales_reco/'
TBLPROPERTIES (
    'projection.enabled'              = 'true',
    'projection.period.type'          = 'date',
    'projection.period.format'        = 'yyyyMM',
    'projection.period.range'         = '202504,NOW',
    'projection.period.interval'      = '1',
    'projection.period.interval.unit' = 'MONTHS',
    'storage.location.template'       = 's3://<BUCKET>/intermediate/ewb_vs_sales_reco/period=${period}/'
)
