"""
=====================================================================================================
JOB      : GSTR-1 Upload File Creation                                            (Activity 3)
CLIENT   : Swiggy
RUNS     : Once per batch, after the second validation (Activity 1, stage "corrected")
-----------------------------------------------------------------------------------------------------
WHAT IT DOES
  Takes the lines that passed validation and arranges them into the sections of the GSTN
  GSTR-1 offline tool, one Excel sheet per section, with the official column headers.

  Section rules
    b2b    invoice type B2B                                         one row per invoice
    b2cl   B2C, inter-state, invoice value above Rs.1,00,000        one row per invoice
    b2cs   all other B2C                                            SUMMED by place of supply + rate
    cdnr   credit/debit notes to registered buyers (CDNR)           one row per note
    cdnur  credit/debit notes to unregistered buyers (CDNUR)        one row per note
    exp    exports (EXP)                                            one row per invoice
    hsn    HSN-wise summary (Table 12), credit notes subtract       SUMMED by HSN + UQC + rate
    docs   documents issued (Table 13)                              SUMMED by document type + series

  Lines that are still "Error" after correction are NOT uploaded (they stay in the error file).

ALTERYX EQUIVALENT
  Input Data -> Filter (Success) -> Formula (section) -> Filter per section
  -> Summarize (b2cs, hsn, docs) -> Select (GSTN headers) -> Output Data (one sheet per section)

INPUT
  intermediate/gstr1_final/period=<period>/<batch>.parquet

OUTPUT
  final/<period>/gstr1/<batch>_gstr1_upload.xlsx                     one sheet per section
  intermediate/gstr1_upload_file/period=<period>/<batch>.parquet     all sections in one table
                                                                     (Athena table gstr1_upload_file)

RECORD ID
  Every sheet has a last column "Record ID(s)".
    * line sections (b2b, b2cl, cdnr, cdnur, exp): the one Record ID of that line
    * summed sections (b2cs, hsn, docs): every Record ID that was added up, joined with " | "
  So any figure in the upload file can be traced back to the sales register lines behind it.
  The GSTN offline tool ignores columns it does not know; remove the column if your tool version
  rejects it.

HOW TO RUN
  python -m activities.gstr1_upload_file_creation.job --period 202506 --batch B1
  Add  --local-dir <folder>  to read/write the same paths in a local folder instead of S3.
=====================================================================================================
"""
import argparse
import io
import os

import boto3
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# =====================================================================================================
# CONFIGURATION
# =====================================================================================================
B2CL_LIMIT_RS = 100000      # B2C inter-state invoices above this value are reported invoice-wise (b2cl)

STATE_NAMES = {
    "01": "Jammu and Kashmir", "02": "Himachal Pradesh", "03": "Punjab", "04": "Chandigarh",
    "05": "Uttarakhand", "06": "Haryana", "07": "Delhi", "08": "Rajasthan", "09": "Uttar Pradesh",
    "10": "Bihar", "11": "Sikkim", "12": "Arunachal Pradesh", "13": "Nagaland", "14": "Manipur",
    "15": "Mizoram", "16": "Tripura", "17": "Meghalaya", "18": "Assam", "19": "West Bengal",
    "20": "Jharkhand", "21": "Odisha", "22": "Chhattisgarh", "23": "Madhya Pradesh", "24": "Gujarat",
    "26": "Dadra and Nagar Haveli and Daman and Diu", "27": "Maharashtra", "29": "Karnataka",
    "30": "Goa", "31": "Lakshadweep", "32": "Kerala", "33": "Tamil Nadu", "34": "Puducherry",
    "35": "Andaman and Nicobar Islands", "36": "Telangana", "37": "Andhra Pradesh", "38": "Ladakh",
    "96": "Other Countries", "97": "Other Territory",
}

# For each section: (column in the upload table, header in the GSTN offline tool)
SECTION_LAYOUT = {
    "b2b": [
        ("recipient_gstin", "GSTIN/UIN of Recipient"), ("receiver_name", "Receiver Name"),
        ("document_number", "Invoice Number"), ("document_date_text", "Invoice date"),
        ("document_value", "Invoice Value"), ("place_of_supply", "Place Of Supply"),
        ("reverse_charge", "Reverse Charge"), ("applicable_tax_rate_pct", "Applicable % of Tax Rate"),
        ("gstn_invoice_type", "Invoice Type"), ("ecommerce_gstin", "E-Commerce GSTIN"),
        ("rate", "Rate"), ("taxable_value", "Taxable Value"), ("cess_amount", "Cess Amount"),
    ],
    "b2cl": [
        ("document_number", "Invoice Number"), ("document_date_text", "Invoice date"),
        ("document_value", "Invoice Value"), ("place_of_supply", "Place Of Supply"),
        ("applicable_tax_rate_pct", "Applicable % of Tax Rate"), ("rate", "Rate"),
        ("taxable_value", "Taxable Value"), ("cess_amount", "Cess Amount"),
        ("ecommerce_gstin", "E-Commerce GSTIN"),
    ],
    "b2cs": [
        ("b2cs_type", "Type"), ("place_of_supply", "Place Of Supply"),
        ("applicable_tax_rate_pct", "Applicable % of Tax Rate"), ("rate", "Rate"),
        ("taxable_value", "Taxable Value"), ("cess_amount", "Cess Amount"),
        ("ecommerce_gstin", "E-Commerce GSTIN"),
    ],
    "cdnr": [
        ("recipient_gstin", "GSTIN/UIN of Recipient"), ("receiver_name", "Receiver Name"),
        ("document_number", "Note Number"), ("document_date_text", "Note Date"),
        ("note_type", "Note Type"), ("place_of_supply", "Place Of Supply"),
        ("reverse_charge", "Reverse Charge"), ("note_supply_type", "Note Supply Type"),
        ("document_value", "Note Value"), ("applicable_tax_rate_pct", "Applicable % of Tax Rate"),
        ("rate", "Rate"), ("taxable_value", "Taxable Value"), ("cess_amount", "Cess Amount"),
    ],
    "cdnur": [
        ("ur_type", "UR Type"), ("document_number", "Note Number"), ("document_date_text", "Note Date"),
        ("note_type", "Note Type"), ("place_of_supply", "Place Of Supply"),
        ("document_value", "Note Value"), ("applicable_tax_rate_pct", "Applicable % of Tax Rate"),
        ("rate", "Rate"), ("taxable_value", "Taxable Value"), ("cess_amount", "Cess Amount"),
    ],
    "exp": [
        ("export_type", "Export Type"), ("document_number", "Invoice Number"),
        ("document_date_text", "Invoice date"), ("document_value", "Invoice Value"),
        ("port_code", "Port Code"), ("shipping_bill_number", "Shipping Bill Number"),
        ("shipping_bill_date", "Shipping Bill Date"), ("rate", "Rate"),
        ("taxable_value", "Taxable Value"), ("cess_amount", "Cess Amount"),
    ],
    "hsn": [
        ("hsn", "HSN"), ("description", "Description"), ("uqc", "UQC"),
        ("total_quantity", "Total Quantity"), ("document_value", "Total Value"), ("rate", "Rate"),
        ("taxable_value", "Taxable Value"), ("igst_amount", "Integrated Tax Amount"),
        ("cgst_amount", "Central Tax Amount"), ("sgst_amount", "State/UT Tax Amount"),
        ("cess_amount", "Cess Amount"),
    ],
    "docs": [
        ("doc_nature", "Nature of Document"), ("sr_from", "Sr. No. From"), ("sr_to", "Sr. No. To"),
        ("total_number", "Total Number"), ("cancelled", "Cancelled"),
    ],
}


# Column types of the upload table (Athena table gstr1_upload_file uses the same list)
UPLOAD_SCHEMA = pa.schema([
    ("batch_id", pa.string()), ("section", pa.string()), ("record_ids", pa.string()),
    ("recipient_gstin", pa.string()), ("receiver_name", pa.string()),
    ("document_number", pa.string()), ("document_date", pa.date32()), ("document_value", pa.float64()),
    ("place_of_supply", pa.string()), ("reverse_charge", pa.string()),
    ("applicable_tax_rate_pct", pa.float64()), ("gstn_invoice_type", pa.string()),
    ("ecommerce_gstin", pa.string()), ("note_type", pa.string()), ("note_supply_type", pa.string()),
    ("ur_type", pa.string()), ("b2cs_type", pa.string()), ("export_type", pa.string()),
    ("port_code", pa.string()), ("shipping_bill_number", pa.string()), ("shipping_bill_date", pa.string()),
    ("hsn", pa.string()), ("description", pa.string()), ("uqc", pa.string()),
    ("total_quantity", pa.float64()), ("rate", pa.float64()), ("taxable_value", pa.float64()),
    ("igst_amount", pa.float64()), ("cgst_amount", pa.float64()), ("sgst_amount", pa.float64()),
    ("cess_amount", pa.float64()), ("doc_nature", pa.string()), ("sr_from", pa.string()),
    ("sr_to", pa.string()), ("total_number", pa.int32()), ("cancelled", pa.int32()),
])


# =====================================================================================================
# STORAGE  (Input Data / Output Data tools)
# Reads and writes files in S3. With --local-dir the same paths are used inside a local folder.
# =====================================================================================================
class Storage:
    def __init__(self, local_dir: str = None):
        self.bucket    = os.environ.get("S3_BUCKET", "swiggy-idt-poc-bucket")
        self.local_dir = local_dir
        self.s3        = None if local_dir else boto3.client("s3", region_name=os.environ.get("AWS_DEFAULT_REGION", "ap-south-1"))

    def read_bytes(self, key: str) -> bytes:
        if self.local_dir:
            with open(os.path.join(self.local_dir, key), "rb") as f:
                return f.read()
        return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def write_bytes(self, key: str, data: bytes) -> str:
        if self.local_dir:
            path = os.path.join(self.local_dir, key)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(data)
            return path
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=data)
        return f"s3://{self.bucket}/{key}"

    def read_parquet(self, key: str):
        return pq.read_table(io.BytesIO(self.read_bytes(key)))

    def write_parquet(self, table, key: str) -> str:
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        return self.write_bytes(key, buffer.getvalue())

    def write_excel(self, sheets: dict, key: str) -> str:
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            for sheet_name, df in sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        return self.write_bytes(key, buffer.getvalue())


# =====================================================================================================
# THE JOB
# =====================================================================================================
def run(period: str, batch_id: str, local_dir: str = None) -> dict:
    store = Storage(local_dir)
    input_key   = f"intermediate/gstr1_final/period={period}/{batch_id}.parquet"
    parquet_key = f"intermediate/gstr1_upload_file/period={period}/{batch_id}.parquet"
    excel_key   = f"final/{period}/gstr1/{batch_id}_gstr1_upload.xlsx"

    print(f"[GSTR1 upload file] period={period} batch={batch_id}")

    # -------------------------------------------------------------------------------------------------
    # STEP 1 - INPUT + FILTER: only lines that passed validation are uploaded
    # -------------------------------------------------------------------------------------------------
    con = duckdb.connect()
    con.register("final_lines", store.read_parquet(input_key))
    con.register("state_names", pd.DataFrame(list(STATE_NAMES.items()), columns=["code", "name"]))

    con.execute(f"""
        CREATE TABLE lines AS
        SELECT f.*,
            -- GSTN expects place of supply as "29-Karnataka"
            left(f.place_of_supply, 2) || '-' || s.name                         AS pos_gstn,
            left(f.place_of_supply, 2) <> left(f.supplier_gstin, 2)             AS is_inter_state,
            -- STEP 2 - FORMULA: which GSTR-1 section does this line belong to?
            CASE
                WHEN f.invoice_type = 'B2B'   THEN 'b2b'
                WHEN f.invoice_type = 'B2C'
                     AND left(f.place_of_supply, 2) <> left(f.supplier_gstin, 2)
                     AND f.total_invoice_value > {B2CL_LIMIT_RS}                 THEN 'b2cl'
                WHEN f.invoice_type = 'B2C'   THEN 'b2cs'
                WHEN f.invoice_type = 'CDNR'  THEN 'cdnr'
                WHEN f.invoice_type = 'CDNUR' THEN 'cdnur'
                WHEN f.invoice_type = 'EXP'   THEN 'exp'
            END                                                                AS section,
            -- credit notes reduce the HSN summary
            CASE WHEN f.document_type = 'CRN' THEN -1 ELSE 1 END                AS hsn_sign
        FROM final_lines f
        LEFT JOIN state_names s ON s.code = left(f.place_of_supply, 2)
        WHERE f.validation_status = 'Success'
    """)
    lines_uploaded = con.execute("SELECT count(*) FROM lines").fetchone()[0]

    # -------------------------------------------------------------------------------------------------
    # STEP 3 - build every section as rows of ONE table with the same columns.
    #   Columns a section does not use stay NULL. "record_ids" carries the traceability.
    # -------------------------------------------------------------------------------------------------
    upload = con.execute("""
        -- line sections: one row per invoice / note --------------------------------------------------
        SELECT
            section,
            record_id                                         AS record_ids,
            receiver_gstin                                    AS recipient_gstin,
            receiver_name,
            invoice_number                                    AS document_number,
            invoice_date                                      AS document_date,
            total_invoice_value                               AS document_value,
            pos_gstn                                          AS place_of_supply,
            reverse_charge,
            CAST(NULL AS DOUBLE)                              AS applicable_tax_rate_pct,
            CASE WHEN section = 'b2b' THEN 'Regular B2B' END  AS gstn_invoice_type,
            ecommerce_gstin,
            CASE WHEN section IN ('cdnr', 'cdnur')
                 THEN CASE WHEN document_type = 'CRN' THEN 'C' ELSE 'D' END END AS note_type,
            CASE WHEN section = 'cdnr' THEN 'Regular B2B' END AS note_supply_type,
            CASE WHEN section = 'cdnur' THEN 'B2CL' END       AS ur_type,
            CAST(NULL AS VARCHAR)                             AS b2cs_type,
            export_type, port_code, shipping_bill_number, shipping_bill_date,
            CAST(NULL AS VARCHAR) AS hsn, CAST(NULL AS VARCHAR) AS description, CAST(NULL AS VARCHAR) AS uqc,
            CAST(NULL AS DOUBLE)  AS total_quantity,
            gst_rate                                          AS rate,
            taxable_value, igst_amount, cgst_amount, sgst_amount, cess_amount,
            CAST(NULL AS VARCHAR) AS doc_nature, CAST(NULL AS VARCHAR) AS sr_from,
            CAST(NULL AS VARCHAR) AS sr_to, CAST(NULL AS INTEGER) AS total_number,
            CAST(NULL AS INTEGER) AS cancelled
        FROM lines
        WHERE section IN ('b2b', 'b2cl', 'cdnr', 'cdnur', 'exp')

        UNION ALL
        -- b2cs: B2C small, summed by place of supply + rate (SUMMARIZE tool) ----------------------------
        SELECT
            'b2cs',
            string_agg(record_id, ' | ' ORDER BY record_id),
            NULL, NULL, NULL, NULL, NULL,
            pos_gstn, NULL, NULL, NULL, ecommerce_gstin, NULL, NULL, NULL,
            'OE',                                             -- OE = other than e-commerce
            NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL,
            gst_rate,
            round(sum(taxable_value), 2), round(sum(igst_amount), 2), round(sum(cgst_amount), 2),
            round(sum(sgst_amount), 2), round(sum(cess_amount), 2),
            NULL, NULL, NULL, NULL, NULL
        FROM lines
        WHERE section = 'b2cs'
        GROUP BY pos_gstn, gst_rate, ecommerce_gstin

        UNION ALL
        -- hsn: HSN-wise summary, credit notes subtract (SUMMARIZE tool) --------------------------------
        SELECT
            'hsn',
            string_agg(record_id, ' | ' ORDER BY record_id),
            NULL, NULL, NULL, NULL,
            round(sum(hsn_sign * total_invoice_value), 2),
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL,
            hsn_sac, max(item_description), uqc,
            sum(hsn_sign * quantity),
            gst_rate,
            round(sum(hsn_sign * taxable_value), 2), round(sum(hsn_sign * igst_amount), 2),
            round(sum(hsn_sign * cgst_amount), 2), round(sum(hsn_sign * sgst_amount), 2),
            round(sum(hsn_sign * cess_amount), 2),
            NULL, NULL, NULL, NULL, NULL
        FROM lines
        GROUP BY hsn_sac, uqc, gst_rate

        UNION ALL
        -- docs: documents issued, by document type and invoice series, e.g. SWG/..., SEX/... (SUMMARIZE)
        SELECT
            'docs',
            string_agg(record_id, ' | ' ORDER BY record_id),
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            CASE document_type WHEN 'INV' THEN 'Invoices for outward supply'
                               WHEN 'CRN' THEN 'Credit Note'
                               WHEN 'DBN' THEN 'Debit Note' END,
            min(invoice_number), max(invoice_number),
            CAST(count(*) AS INTEGER), 0
        FROM lines
        GROUP BY document_type, regexp_extract(invoice_number, '^[A-Za-z]+')

        ORDER BY section, document_number, place_of_supply, hsn, doc_nature
    """).df()
    con.close()

    upload.insert(0, "batch_id", batch_id)
    upload["document_date_text"] = pd.to_datetime(upload["document_date"]).dt.strftime("%d-%b-%Y")

    # -------------------------------------------------------------------------------------------------
    # STEP 4 - OUTPUT (Excel): one sheet per section, GSTN headers, Record ID(s) as last column
    # -------------------------------------------------------------------------------------------------
    sheets = {}
    for section, layout in SECTION_LAYOUT.items():
        rows = upload[upload["section"] == section]
        sheet = rows[[column for column, _ in layout] + ["record_ids"]]
        sheets[section] = sheet.rename(columns={**dict(layout), "record_ids": "Record ID(s)"})
    store.write_excel(sheets, excel_key)

    # -------------------------------------------------------------------------------------------------
    # STEP 5 - OUTPUT (parquet): all sections in one table for Athena (no Excel-only text columns)
    # -------------------------------------------------------------------------------------------------
    table = pa.Table.from_pandas(upload.drop(columns=["document_date_text"]), schema=UPLOAD_SCHEMA,
                                 preserve_index=False)
    store.write_parquet(table, parquet_key)

    summary = {"lines_uploaded": int(lines_uploaded),
               **{section: int((upload["section"] == section).sum()) for section in SECTION_LAYOUT}}
    print(f"  Lines uploaded: {lines_uploaded} | rows per sheet: "
          + ", ".join(f"{s}={summary[s]}" for s in SECTION_LAYOUT))
    print(f"  Output: {excel_key}")
    print(f"          {parquet_key}")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build the GSTR-1 upload file for one batch")
    parser.add_argument("--period", required=True, help="return period YYYYMM, e.g. 202506")
    parser.add_argument("--batch", required=True, help="batch id, e.g. B1")
    parser.add_argument("--local-dir", help="use this local folder instead of S3")
    args = parser.parse_args()
    run(args.period, args.batch, args.local_dir)
