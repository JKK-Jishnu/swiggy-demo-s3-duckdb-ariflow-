-- =====================================================================================================
-- TABLE  : swiggy_gst.gstr2b_consolidated
-- SOURCE : Activity 8 - intermediate/gstr2b_consolidated/period=<YYYYMM>/*.parquet
-- WHAT   : GSTR-2B lines of all Swiggy GSTINs (B2B + CDNR sheets), one row per line
-- PERIOD : partition column "period" (YYYYMM) comes from the folder name; new months appear
--          automatically (partition projection) - no crawler or MSCK REPAIR needed.
-- =====================================================================================================
CREATE EXTERNAL TABLE swiggy_gst.gstr2b_consolidated (
    record_id                  string     COMMENT 'Unique line ID carried from the raw file, e.g. sales-202506-b2b-1 / pr-202506-1 / 2b-202506-10',
    recipient_gstin            string     COMMENT 'Swiggy GSTIN that received the supply',
    section                    string     COMMENT 'GSTR-1 section: b2b, b2cl, b2cs, cdnr, cdnur, exp, hsn, docs',
    document_type              string,
    supplier_gstin             string,
    supplier_name              string,
    document_number            string,
    document_date              date,
    supply_type                string,
    note_type                  string,
    document_value             double,
    place_of_supply            string,
    reverse_charge             string,
    gst_rate                   double,
    taxable_value              double,
    igst_amount                double,
    cgst_amount                double,
    sgst_amount                double,
    cess_amount                double,
    supplier_filing_period     string,
    supplier_filing_date       date,
    itc_availability           string,
    itc_unavailability_reason  string,
    applicable_tax_rate_pct    double,
    source                     string,
    irn                        string,
    irn_date                   date,
    source_file                string     COMMENT 'GSTR-2B workbook the line came from'
)
COMMENT 'GSTR-2B lines of all Swiggy GSTINs (B2B + CDNR sheets), one row per line'
PARTITIONED BY (period string)
STORED AS PARQUET
LOCATION 's3://<BUCKET>/intermediate/gstr2b_consolidated/'
TBLPROPERTIES (
    'projection.enabled'              = 'true',
    'projection.period.type'          = 'date',
    'projection.period.format'        = 'yyyyMM',
    'projection.period.range'         = '202504,NOW',
    'projection.period.interval'      = '1',
    'projection.period.interval.unit' = 'MONTHS',
    'storage.location.template'       = 's3://<BUCKET>/intermediate/gstr2b_consolidated/period=${period}/'
)
