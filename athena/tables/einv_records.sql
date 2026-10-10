-- =====================================================================================================
-- TABLE  : swiggy_gst.einv_records
-- SOURCE : Activity 5 - intermediate/einv_records/period=<YYYYMM>/*.parquet
-- WHAT   : E-invoice (IRN) records downloaded through Bolton, cleaned
-- PERIOD : partition column "period" (YYYYMM) comes from the folder name; new months appear
--          automatically (partition projection) - no crawler or MSCK REPAIR needed.
-- =====================================================================================================
CREATE EXTERNAL TABLE swiggy_gst.einv_records (
    record_id            string     COMMENT 'Unique line ID carried from the raw file, e.g. sales-202506-b2b-1 / pr-202506-1 / 2b-202506-10',
    irn                  string,
    ack_no               string,
    ack_date             date,
    document_type        string,
    document_number      string,
    document_date        date,
    supply_type          string,
    supplier_gstin       string,
    buyer_gstin          string,
    buyer_legal_name     string,
    place_of_supply      string,
    reverse_charge       string,
    assessable_value     double,
    igst_amount          double,
    cgst_amount          double,
    sgst_amount          double,
    cess_amount          double,
    total_invoice_value  double,
    irn_status           string,
    ewb_no               string
)
COMMENT 'E-invoice (IRN) records downloaded through Bolton, cleaned'
PARTITIONED BY (period string)
STORED AS PARQUET
LOCATION 's3://<BUCKET>/intermediate/einv_records/'
TBLPROPERTIES (
    'projection.enabled'              = 'true',
    'projection.period.type'          = 'date',
    'projection.period.format'        = 'yyyyMM',
    'projection.period.range'         = '202504,NOW',
    'projection.period.interval'      = '1',
    'projection.period.interval.unit' = 'MONTHS',
    'storage.location.template'       = 's3://<BUCKET>/intermediate/einv_records/period=${period}/'
)
