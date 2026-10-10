-- =====================================================================================================
-- TABLE  : swiggy_gst.sales_combined
-- SOURCE : Activity 4 - intermediate/sales_combined/period=<YYYYMM>/*.parquet
-- WHAT   : GSTR-1 sales lines of all batches after correction and re-validation, one row per line
-- PERIOD : partition column "period" (YYYYMM) comes from the folder name; new months appear
--          automatically (partition projection) - no crawler or MSCK REPAIR needed.
-- =====================================================================================================
CREATE EXTERNAL TABLE swiggy_gst.sales_combined (
    record_id                string     COMMENT 'Unique line ID carried from the raw file, e.g. sales-202506-b2b-1 / pr-202506-1 / 2b-202506-10',
    batch_id                 string     COMMENT 'Client upload batch, e.g. B1',
    source_row_number        int        COMMENT 'Row number in the Excel file sent by the client (row 1 = headers)',
    supplier_gstin           string,
    document_type            string,
    invoice_type             string,
    invoice_number           string,
    invoice_date             date,
    invoice_date_text        string     COMMENT 'Invoice date exactly as received (text)',
    receiver_gstin           string,
    receiver_name            string,
    place_of_supply          string,
    reverse_charge           string,
    ecommerce_gstin          string,
    hsn_sac                  string,
    item_description         string,
    uqc                      string,
    quantity                 double,
    gst_rate                 double,
    taxable_value            double,
    igst_amount              double,
    cgst_amount              double,
    sgst_amount              double,
    cess_amount              double,
    total_invoice_value      double,
    original_invoice_number  string,
    original_invoice_date    string,
    export_type              string,
    port_code                string,
    shipping_bill_number     string,
    shipping_bill_date       string,
    validation_status        string     COMMENT 'Success or Error',
    validation_remark        string     COMMENT 'Every failed rule, separated by a pipe, e.g. R07 Invalid receiver GSTIN ... | R15 Tax split wrong ...',
    error_count              int        COMMENT 'Number of rules failed',
    correction_applied       boolean    COMMENT 'TRUE when the client corrected this line',
    corrected_fields         string     COMMENT 'Fields the client corrected, comma separated',
    correction_remark        string,
    first_pass_status        string     COMMENT 'Status in the first validation, before client corrections',
    first_pass_remark        string     COMMENT 'Failed rules in the first validation, before client corrections',
    processed_at             timestamp  COMMENT 'When the job produced this row',
    is_duplicate_record_id   boolean    COMMENT 'Same Record ID appears in more than one line of the month',
    is_duplicate_invoice     boolean    COMMENT 'Same document type + invoice number appears in more than one line of the month'
)
COMMENT 'GSTR-1 sales lines of all batches after correction and re-validation, one row per line'
PARTITIONED BY (period string)
STORED AS PARQUET
LOCATION 's3://<BUCKET>/intermediate/sales_combined/'
TBLPROPERTIES (
    'projection.enabled'              = 'true',
    'projection.period.type'          = 'date',
    'projection.period.format'        = 'yyyyMM',
    'projection.period.range'         = '202504,NOW',
    'projection.period.interval'      = '1',
    'projection.period.interval.unit' = 'MONTHS',
    'storage.location.template'       = 's3://<BUCKET>/intermediate/sales_combined/period=${period}/'
)
