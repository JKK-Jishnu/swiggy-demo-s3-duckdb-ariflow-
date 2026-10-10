-- =====================================================================================================
-- TABLE  : swiggy_gst.ewb_records
-- SOURCE : Activity 6 - intermediate/ewb_records/period=<YYYYMM>/*.parquet
-- WHAT   : E-way bill records downloaded through Bolton, cleaned
-- PERIOD : partition column "period" (YYYYMM) comes from the folder name; new months appear
--          automatically (partition projection) - no crawler or MSCK REPAIR needed.
-- =====================================================================================================
CREATE EXTERNAL TABLE swiggy_gst.ewb_records (
    record_id            string     COMMENT 'Unique line ID carried from the raw file, e.g. sales-202506-b2b-1 / pr-202506-1 / 2b-202506-10',
    ewb_no               string,
    ewb_date             date,
    supply_type          string,
    sub_type             string,
    document_type        string,
    document_number      string,
    document_date        date,
    from_gstin           string,
    from_trade_name      string,
    from_pincode         string,
    from_state_code      string,
    to_gstin             string,
    to_trade_name        string,
    to_pincode           string,
    to_state_code        string,
    hsn_code             string,
    taxable_value        double,
    igst_amount          double,
    cgst_amount          double,
    sgst_amount          double,
    cess_amount          double,
    total_invoice_value  double,
    transport_mode       string,
    distance_km          int,
    transporter_id       string,
    vehicle_no           string,
    valid_upto           date,
    ewb_status           string
)
COMMENT 'E-way bill records downloaded through Bolton, cleaned'
PARTITIONED BY (period string)
STORED AS PARQUET
LOCATION 's3://<BUCKET>/intermediate/ewb_records/'
TBLPROPERTIES (
    'projection.enabled'              = 'true',
    'projection.period.type'          = 'date',
    'projection.period.format'        = 'yyyyMM',
    'projection.period.range'         = '202504,NOW',
    'projection.period.interval'      = '1',
    'projection.period.interval.unit' = 'MONTHS',
    'storage.location.template'       = 's3://<BUCKET>/intermediate/ewb_records/period=${period}/'
)
