-- =====================================================================================================
-- TABLE  : swiggy_gst.gstr2b_vs_pr_reco
-- SOURCE : Activity 9 - intermediate/gstr2b_vs_pr_reco/period=<YYYYMM>/*.parquet
-- WHAT   : GSTR-2B vs purchase register reconciliation with ITC at risk
-- PERIOD : partition column "period" (YYYYMM) comes from the folder name; new months appear
--          automatically (partition projection) - no crawler or MSCK REPAIR needed.
-- =====================================================================================================
CREATE EXTERNAL TABLE swiggy_gst.gstr2b_vs_pr_reco (
    match_level               string     COMMENT 'L1 Exact, L2 Amount mismatch, L3 Invoice no mismatch, Only in <side>',
    level_no                  int        COMMENT '1-3 = matched at that ladder level, 4 = only in left source, 5 = only in right source',
    gstr2b_record_id          string     COMMENT 'Record ID of the GSTR-2B line (2b-<period>-n)',
    pr_record_id              string     COMMENT 'Record ID of the purchase register line (pr-<period>-n)',
    recipient_gstin           string     COMMENT 'Swiggy GSTIN that received the supply',
    supplier_gstin            string,
    supplier_name             string,
    gstr2b_invoice_number     string,
    pr_invoice_number         string,
    gstr2b_invoice_date       date,
    pr_invoice_date           date,
    gstr2b_taxable_value      double,
    pr_taxable_value          double,
    taxable_value_difference  double     COMMENT 'Left taxable value minus right taxable value',
    gstr2b_tax                double,
    pr_itc_claimed            double,
    itc_at_risk               double     COMMENT 'ITC claimed that is not supported by GSTR-2B',
    remark                    string     COMMENT 'Plain-English explanation of the match result'
)
COMMENT 'GSTR-2B vs purchase register reconciliation with ITC at risk'
PARTITIONED BY (period string)
STORED AS PARQUET
LOCATION 's3://<BUCKET>/intermediate/gstr2b_vs_pr_reco/'
TBLPROPERTIES (
    'projection.enabled'              = 'true',
    'projection.period.type'          = 'date',
    'projection.period.format'        = 'yyyyMM',
    'projection.period.range'         = '202504,NOW',
    'projection.period.interval'      = '1',
    'projection.period.interval.unit' = 'MONTHS',
    'storage.location.template'       = 's3://<BUCKET>/intermediate/gstr2b_vs_pr_reco/period=${period}/'
)
