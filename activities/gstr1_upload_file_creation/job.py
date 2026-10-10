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

STEPS
  1  Read the validated lines, keep only "Success", decide the section of each line
  2  Build each section with its own query (SECTION_QUERIES)
  3  Write the Excel file: one sheet per section with the GSTN headers (SECTION_HEADERS)
  4  Write all sections together as one parquet table (for Athena)

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
  If your version of the GSTN offline tool rejects unknown columns, delete this column before import.

QUICK CHANGES
  Which lines go to which section ...... STEP 1 (the CASE that sets "section") and B2CL_LIMIT_RS
  Change one sheet ..................... its query in SECTION_QUERIES + its list in SECTION_HEADERS
  New column in a sheet ................ add it to the query and to SECTION_HEADERS; if it is a new
                                         column name, also add it to STEP 4 and to
                                         athena/tables/gstr1_upload_file.sql

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
import pyarrow.parquet as pq


# =====================================================================================================
# CONFIGURATION
# =====================================================================================================
B2CL_LIMIT_RS = 100000      # B2C inter-state invoices above this value are reported one by one (b2cl)

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

# -----------------------------------------------------------------------------------------------------
# One query per section. Each query reads the table "lines" (see STEP 1) and returns the columns
# that section needs, using the column names of the Athena table gstr1_upload_file.
# Write NULL columns as CAST(NULL AS <type>) so every section has the same column types.
# -----------------------------------------------------------------------------------------------------
SECTION_QUERIES = {

    "b2b": """
        SELECT 'b2b' AS section, record_id AS record_ids,
               receiver_gstin AS recipient_gstin, receiver_name,
               invoice_number AS document_number, invoice_date AS document_date,
               total_invoice_value AS document_value, pos_gstn AS place_of_supply, reverse_charge,
               CAST(NULL AS DOUBLE) AS applicable_tax_rate_pct, 'Regular B2B' AS gstn_invoice_type,
               ecommerce_gstin, gst_rate AS rate, taxable_value,
               igst_amount, cgst_amount, sgst_amount, cess_amount
        FROM lines WHERE section = 'b2b'
        ORDER BY invoice_number""",

    "b2cl": """
        SELECT 'b2cl' AS section, record_id AS record_ids,
               invoice_number AS document_number, invoice_date AS document_date,
               total_invoice_value AS document_value, pos_gstn AS place_of_supply, reverse_charge,
               CAST(NULL AS DOUBLE) AS applicable_tax_rate_pct, gst_rate AS rate, taxable_value,
               igst_amount, cgst_amount, sgst_amount, cess_amount, ecommerce_gstin
        FROM lines WHERE section = 'b2cl'
        ORDER BY invoice_number""",

    # summed by place of supply + rate; OE = sales other than through an e-commerce operator
    "b2cs": """
        SELECT 'b2cs' AS section, string_agg(record_id, ' | ' ORDER BY record_id) AS record_ids,
               'OE' AS b2cs_type, pos_gstn AS place_of_supply,
               CAST(NULL AS DOUBLE) AS applicable_tax_rate_pct, gst_rate AS rate,
               round(sum(taxable_value), 2) AS taxable_value,
               round(sum(igst_amount), 2)   AS igst_amount,
               round(sum(cgst_amount), 2)   AS cgst_amount,
               round(sum(sgst_amount), 2)   AS sgst_amount,
               round(sum(cess_amount), 2)   AS cess_amount,
               ecommerce_gstin
        FROM lines WHERE section = 'b2cs'
        GROUP BY pos_gstn, gst_rate, ecommerce_gstin
        ORDER BY pos_gstn, gst_rate""",

    "cdnr": """
        SELECT 'cdnr' AS section, record_id AS record_ids,
               receiver_gstin AS recipient_gstin, receiver_name,
               invoice_number AS document_number, invoice_date AS document_date,
               CASE WHEN document_type = 'CRN' THEN 'C' ELSE 'D' END AS note_type,
               pos_gstn AS place_of_supply, reverse_charge, 'Regular B2B' AS note_supply_type,
               total_invoice_value AS document_value,
               CAST(NULL AS DOUBLE) AS applicable_tax_rate_pct, gst_rate AS rate, taxable_value,
               igst_amount, cgst_amount, sgst_amount, cess_amount
        FROM lines WHERE section = 'cdnr'
        ORDER BY invoice_number""",

    "cdnur": """
        SELECT 'cdnur' AS section, record_id AS record_ids,
               'B2CL' AS ur_type, invoice_number AS document_number, invoice_date AS document_date,
               CASE WHEN document_type = 'CRN' THEN 'C' ELSE 'D' END AS note_type,
               pos_gstn AS place_of_supply, reverse_charge, total_invoice_value AS document_value,
               CAST(NULL AS DOUBLE) AS applicable_tax_rate_pct, gst_rate AS rate, taxable_value,
               igst_amount, cgst_amount, sgst_amount, cess_amount
        FROM lines WHERE section = 'cdnur'
        ORDER BY invoice_number""",

    "exp": """
        SELECT 'exp' AS section, record_id AS record_ids,
               export_type, invoice_number AS document_number, invoice_date AS document_date,
               total_invoice_value AS document_value, pos_gstn AS place_of_supply, reverse_charge,
               port_code, shipping_bill_number, shipping_bill_date, gst_rate AS rate, taxable_value,
               igst_amount, cgst_amount, sgst_amount, cess_amount
        FROM lines WHERE section = 'exp'
        ORDER BY invoice_number""",

    # HSN summary of all uploaded lines; credit notes are subtracted (sign = -1)
    "hsn": """
        SELECT 'hsn' AS section, string_agg(record_id, ' | ' ORDER BY record_id) AS record_ids,
               hsn_sac AS hsn, max(item_description) AS description, uqc,
               sum(sign * quantity)                        AS total_quantity,
               round(sum(sign * total_invoice_value), 2)   AS document_value,
               gst_rate AS rate,
               round(sum(sign * taxable_value), 2)         AS taxable_value,
               round(sum(sign * igst_amount), 2)           AS igst_amount,
               round(sum(sign * cgst_amount), 2)           AS cgst_amount,
               round(sum(sign * sgst_amount), 2)           AS sgst_amount,
               round(sum(sign * cess_amount), 2)           AS cess_amount
        FROM lines
        GROUP BY hsn_sac, uqc, gst_rate
        ORDER BY hsn_sac, gst_rate""",

    # documents issued, per document type and invoice series (the letters before the first "/")
    "docs": """
        SELECT 'docs' AS section, string_agg(record_id, ' | ' ORDER BY record_id) AS record_ids,
               CASE document_type WHEN 'INV' THEN 'Invoices for outward supply'
                                  WHEN 'CRN' THEN 'Credit Note'
                                  WHEN 'DBN' THEN 'Debit Note' END AS doc_nature,
               min(invoice_number)        AS sr_from,
               max(invoice_number)        AS sr_to,
               CAST(count(*) AS INTEGER)  AS total_number,
               0                          AS cancelled
        FROM lines
        GROUP BY document_type, split_part(invoice_number, '/', 1)
        ORDER BY doc_nature, sr_from""",
}

# Excel sheets: for each section, (column from the query, header in the GSTN offline tool)
SECTION_HEADERS = {
    "b2b": [
        ("recipient_gstin", "GSTIN/UIN of Recipient"), ("receiver_name", "Receiver Name"),
        ("document_number", "Invoice Number"), ("document_date", "Invoice date"),
        ("document_value", "Invoice Value"), ("place_of_supply", "Place Of Supply"),
        ("reverse_charge", "Reverse Charge"), ("applicable_tax_rate_pct", "Applicable % of Tax Rate"),
        ("gstn_invoice_type", "Invoice Type"), ("ecommerce_gstin", "E-Commerce GSTIN"),
        ("rate", "Rate"), ("taxable_value", "Taxable Value"), ("cess_amount", "Cess Amount"),
    ],
    "b2cl": [
        ("document_number", "Invoice Number"), ("document_date", "Invoice date"),
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
        ("document_number", "Note Number"), ("document_date", "Note Date"),
        ("note_type", "Note Type"), ("place_of_supply", "Place Of Supply"),
        ("reverse_charge", "Reverse Charge"), ("note_supply_type", "Note Supply Type"),
        ("document_value", "Note Value"), ("applicable_tax_rate_pct", "Applicable % of Tax Rate"),
        ("rate", "Rate"), ("taxable_value", "Taxable Value"), ("cess_amount", "Cess Amount"),
    ],
    "cdnur": [
        ("ur_type", "UR Type"), ("document_number", "Note Number"), ("document_date", "Note Date"),
        ("note_type", "Note Type"), ("place_of_supply", "Place Of Supply"),
        ("document_value", "Note Value"), ("applicable_tax_rate_pct", "Applicable % of Tax Rate"),
        ("rate", "Rate"), ("taxable_value", "Taxable Value"), ("cess_amount", "Cess Amount"),
    ],
    "exp": [
        ("export_type", "Export Type"), ("document_number", "Invoice Number"),
        ("document_date", "Invoice date"), ("document_value", "Invoice Value"),
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


# =====================================================================================================
# STORAGE - read and write files in S3, or in a local folder when --local-dir is given
# =====================================================================================================
class Storage:
    def __init__(self, local_dir: str = None):
        self.bucket    = os.environ.get("S3_BUCKET", "swiggy-idt-poc-bucket")
        self.local_dir = local_dir
        if local_dir:
            self.s3 = None
        else:
            self.s3 = boto3.client("s3", region_name=os.environ.get("AWS_DEFAULT_REGION", "ap-south-1"))

    def read_bytes(self, key: str) -> bytes:
        if self.local_dir:
            with open(os.path.join(self.local_dir, key), "rb") as f:
                return f.read()
        return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def write_bytes(self, key: str, data: bytes):
        if self.local_dir:
            path = os.path.join(self.local_dir, key)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(data)
        else:
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=data)

    def read_parquet(self, key: str):
        """Returns a pyarrow table (keeps the exact column types, also for empty columns)."""
        return pq.read_table(io.BytesIO(self.read_bytes(key)))

    def write_parquet(self, table, key: str):
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        self.write_bytes(key, buffer.getvalue())

    def write_excel(self, sheets: dict, key: str):
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            for sheet_name, df in sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        self.write_bytes(key, buffer.getvalue())


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
    # STEP 1 - Read the validated lines, keep only "Success" and decide each line's section
    # -------------------------------------------------------------------------------------------------
    con = duckdb.connect()
    con.register("final_lines", store.read_parquet(input_key))
    con.register("state_names", pd.DataFrame(list(STATE_NAMES.items()), columns=["code", "name"]))

    con.execute(f"""
        CREATE TABLE lines AS
        SELECT f.*,
            left(f.place_of_supply, 2) || '-' || s.name                AS pos_gstn,  -- "29-Karnataka"
            CASE
                WHEN f.invoice_type = 'B2B'   THEN 'b2b'
                WHEN f.invoice_type = 'B2C'
                     AND left(f.place_of_supply, 2) <> left(f.supplier_gstin, 2)
                     AND f.total_invoice_value > {B2CL_LIMIT_RS}        THEN 'b2cl'
                WHEN f.invoice_type = 'B2C'   THEN 'b2cs'
                WHEN f.invoice_type = 'CDNR'  THEN 'cdnr'
                WHEN f.invoice_type = 'CDNUR' THEN 'cdnur'
                WHEN f.invoice_type = 'EXP'   THEN 'exp'
            END                                                       AS section,
            CASE WHEN f.document_type = 'CRN' THEN -1 ELSE 1 END       AS sign       -- credit notes subtract in hsn
        FROM final_lines f
        LEFT JOIN state_names s ON s.code = left(f.place_of_supply, 2)
        WHERE f.validation_status = 'Success'
    """)
    lines_uploaded = con.execute("SELECT count(*) FROM lines").fetchone()[0]

    # -------------------------------------------------------------------------------------------------
    # STEP 2 - Build each section as its own table: section_b2b, section_b2cl, ...
    # -------------------------------------------------------------------------------------------------
    for section, query in SECTION_QUERIES.items():
        con.execute(f"CREATE TABLE section_{section} AS {query}")

    # -------------------------------------------------------------------------------------------------
    # STEP 3 - Excel: one sheet per section, GSTN headers, "Record ID(s)" as the last column
    # -------------------------------------------------------------------------------------------------
    sheets      = {}
    rows_in_tab = {}
    for section, headers in SECTION_HEADERS.items():
        sheet = con.execute(f"SELECT * FROM section_{section}").df()
        if "document_date" in sheet.columns:
            sheet["document_date"] = pd.to_datetime(sheet["document_date"]).dt.strftime("%d-%b-%Y")

        columns     = [column for column, header in headers] + ["record_ids"]
        new_headers = {column: header for column, header in headers}
        new_headers["record_ids"] = "Record ID(s)"
        sheets[section]      = sheet[columns].rename(columns=new_headers)
        rows_in_tab[section] = len(sheet)
    store.write_excel(sheets, excel_key)

    # -------------------------------------------------------------------------------------------------
    # STEP 4 - Parquet: all sections stacked in one table (Athena table gstr1_upload_file).
    #   UNION ALL BY NAME matches columns by name; a column a section does not have becomes NULL.
    #   Every column gets a fixed type here - the types of athena/tables/gstr1_upload_file.sql.
    # -------------------------------------------------------------------------------------------------
    all_sections = " UNION ALL BY NAME ".join(f"SELECT * FROM section_{s}" for s in SECTION_QUERIES)
    upload_table = con.execute(f"""
        SELECT
            CAST('{batch_id}' AS VARCHAR)            AS batch_id,
            CAST(section AS VARCHAR)                 AS section,
            CAST(record_ids AS VARCHAR)              AS record_ids,
            CAST(recipient_gstin AS VARCHAR)         AS recipient_gstin,
            CAST(receiver_name AS VARCHAR)           AS receiver_name,
            CAST(document_number AS VARCHAR)         AS document_number,
            CAST(document_date AS DATE)              AS document_date,
            CAST(document_value AS DOUBLE)           AS document_value,
            CAST(place_of_supply AS VARCHAR)         AS place_of_supply,
            CAST(reverse_charge AS VARCHAR)          AS reverse_charge,
            CAST(applicable_tax_rate_pct AS DOUBLE)  AS applicable_tax_rate_pct,
            CAST(gstn_invoice_type AS VARCHAR)       AS gstn_invoice_type,
            CAST(ecommerce_gstin AS VARCHAR)         AS ecommerce_gstin,
            CAST(note_type AS VARCHAR)               AS note_type,
            CAST(note_supply_type AS VARCHAR)        AS note_supply_type,
            CAST(ur_type AS VARCHAR)                 AS ur_type,
            CAST(b2cs_type AS VARCHAR)               AS b2cs_type,
            CAST(export_type AS VARCHAR)             AS export_type,
            CAST(port_code AS VARCHAR)               AS port_code,
            CAST(shipping_bill_number AS VARCHAR)    AS shipping_bill_number,
            CAST(shipping_bill_date AS VARCHAR)      AS shipping_bill_date,
            CAST(hsn AS VARCHAR)                     AS hsn,
            CAST(description AS VARCHAR)             AS description,
            CAST(uqc AS VARCHAR)                     AS uqc,
            CAST(total_quantity AS DOUBLE)           AS total_quantity,
            CAST(rate AS DOUBLE)                     AS rate,
            CAST(taxable_value AS DOUBLE)            AS taxable_value,
            CAST(igst_amount AS DOUBLE)              AS igst_amount,
            CAST(cgst_amount AS DOUBLE)              AS cgst_amount,
            CAST(sgst_amount AS DOUBLE)              AS sgst_amount,
            CAST(cess_amount AS DOUBLE)              AS cess_amount,
            CAST(doc_nature AS VARCHAR)              AS doc_nature,
            CAST(sr_from AS VARCHAR)                 AS sr_from,
            CAST(sr_to AS VARCHAR)                   AS sr_to,
            CAST(total_number AS INTEGER)            AS total_number,
            CAST(cancelled AS INTEGER)               AS cancelled
        FROM ({all_sections})
        ORDER BY section, document_number, place_of_supply, rate, hsn, doc_nature, sr_from
    """).arrow()
    con.close()
    store.write_parquet(upload_table, parquet_key)

    print(f"  Lines uploaded: {lines_uploaded} | rows per sheet: "
          + ", ".join(f"{section}={count}" for section, count in rows_in_tab.items()))
    print(f"  Output: {excel_key}")
    print(f"          {parquet_key}")
    return {"lines_uploaded": int(lines_uploaded), **rows_in_tab}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build the GSTR-1 upload file for one batch")
    parser.add_argument("--period", required=True, help="return period YYYYMM, e.g. 202506")
    parser.add_argument("--batch", required=True, help="batch id, e.g. B1")
    parser.add_argument("--local-dir", help="use this local folder instead of S3")
    args = parser.parse_args()
    run(args.period, args.batch, args.local_dir)
