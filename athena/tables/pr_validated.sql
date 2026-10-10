-- =====================================================================================================
-- TABLE  : swiggy_gst.pr_validated
-- SOURCE : Activity 7 - intermediate/pr_validated/period=<YYYYMM>/*.parquet
-- WHAT   : Purchase register lines with validation result, one row per line
-- PERIOD : partition column "period" (YYYYMM) comes from the folder name; new months appear
--          automatically (partition projection) - no crawler or MSCK REPAIR needed.
-- =====================================================================================================
CREATE EXTERNAL TABLE swiggy_gst.pr_validated (
    record_id                string     COMMENT 'Unique line ID carried from the raw file, e.g. sales-202506-b2b-1 / pr-202506-1 / 2b-202506-10',
    source_row_number        int        COMMENT 'Row number in the Excel file sent by the client (row 1 = headers)',
    recipient_gstin          string     COMMENT 'Swiggy GSTIN that received the supply',
    supplier_gstin           string,
    supplier_legal_name      string,
    document_type            string,
    invoice_type             string,
    invoice_number           string,
    invoice_date             date,
    invoice_date_text        string     COMMENT 'Invoice date exactly as received (text)',
    place_of_supply          string,
    reverse_charge           string,
    hsn_sac                  string,
    item_description         string,
    gst_rate                 double,
    taxable_value            double,
    igst_amount              double,
    cgst_amount              double,
    sgst_amount              double,
    cess_amount              double,
    total_tax                double,
    total_invoice_value      double,
    itc_eligibility          string,
    itc_igst                 double,
    itc_cgst                 double,
    itc_sgst                 double,
    itc_cess                 double,
    itc_total                double,
    voucher_number           string,
    voucher_date             date,
    original_invoice_number  string,
    original_invoice_date    string,
    validation_status        string     COMMENT 'Success or Error',
    validation_remark        string     COMMENT 'Every failed rule, separated by a pipe, e.g. R07 Invalid receiver GSTIN ... | R15 Tax split wrong ...',
    error_count              int        COMMENT 'Number of rules failed',
    processed_at             timestamp  COMMENT 'When the job produced this row'
)
COMMENT 'Purchase register lines with validation result, one row per line'
PARTITIONED BY (period string)
STORED AS PARQUET
LOCATION 's3://<BUCKET>/intermediate/pr_validated/'
TBLPROPERTIES (
    'projection.enabled'              = 'true',
    'projection.period.type'          = 'date',
    'projection.period.format'        = 'yyyyMM',
    'projection.period.range'         = '202504,NOW',
    'projection.period.interval'      = '1',
    'projection.period.interval.unit' = 'MONTHS',
    'storage.location.template'       = 's3://<BUCKET>/intermediate/pr_validated/period=${period}/'
)
