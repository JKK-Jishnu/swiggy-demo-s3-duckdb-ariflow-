-- =====================================================================================================
-- TABLE  : swiggy_gst.gstr1_upload_file
-- SOURCE : Activity 3 - intermediate/gstr1_upload_file/period=<YYYYMM>/*.parquet
-- WHAT   : GSTR-1 upload file, all sections (b2b, b2cl, b2cs, cdnr, cdnur, exp, hsn, docs) in one table
-- PERIOD : partition column "period" (YYYYMM) comes from the folder name; new months appear
--          automatically (partition projection) - no crawler or MSCK REPAIR needed.
-- =====================================================================================================
CREATE EXTERNAL TABLE swiggy_gst.gstr1_upload_file (
    batch_id                 string     COMMENT 'Client upload batch, e.g. B1',
    section                  string     COMMENT 'GSTR-1 section: b2b, b2cl, b2cs, cdnr, cdnur, exp, hsn, docs',
    record_ids               string     COMMENT 'Record ID of the line, or for summed sections (b2cs, hsn, docs) every Record ID added up, joined with a pipe ( | )',
    recipient_gstin          string     COMMENT 'Swiggy GSTIN that received the supply',
    receiver_name            string,
    document_number          string,
    document_date            date,
    document_value           double,
    place_of_supply          string,
    reverse_charge           string,
    applicable_tax_rate_pct  double,
    gstn_invoice_type        string,
    ecommerce_gstin          string,
    note_type                string,
    note_supply_type         string,
    ur_type                  string,
    b2cs_type                string,
    export_type              string,
    port_code                string,
    shipping_bill_number     string,
    shipping_bill_date       string,
    hsn                      string,
    description              string,
    uqc                      string,
    total_quantity           double,
    rate                     double,
    taxable_value            double,
    igst_amount              double,
    cgst_amount              double,
    sgst_amount              double,
    cess_amount              double,
    doc_nature               string,
    sr_from                  string,
    sr_to                    string,
    total_number             int,
    cancelled                int
)
COMMENT 'GSTR-1 upload file, all sections (b2b, b2cl, b2cs, cdnr, cdnur, exp, hsn, docs) in one table'
PARTITIONED BY (period string)
STORED AS PARQUET
LOCATION 's3://<BUCKET>/intermediate/gstr1_upload_file/'
TBLPROPERTIES (
    'projection.enabled'              = 'true',
    'projection.period.type'          = 'date',
    'projection.period.format'        = 'yyyyMM',
    'projection.period.range'         = '202504,NOW',
    'projection.period.interval'      = '1',
    'projection.period.interval.unit' = 'MONTHS',
    'storage.location.template'       = 's3://<BUCKET>/intermediate/gstr1_upload_file/period=${period}/'
)
