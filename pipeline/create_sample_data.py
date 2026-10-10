"""
create_sample_data.py
Generates 3 GSTR1 batch Excel files with realistic data including intentional errors.
Uploads them to S3 raw/ folder along with manifest.json and per-batch trigger files.

Run: python pipeline\\create_sample_data.py
"""
import pandas as pd
import boto3
import json
import random
import os
from datetime import datetime, timedelta

BUCKET = os.environ.get("S3_BUCKET", "swiggy-idt-poc-bucket")
REGION = "ap-south-1"
PERIOD = "202506"
SWIGGY_GSTIN = "27AABCS1429B1ZB"  # Swiggy Maharashtra GSTIN

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_DIR = os.path.join(REPO_ROOT, "activities", "gstr1_validation", "data", "input")

# Valid buyer GSTINs
VALID_GSTINS = [
    "29AADCB2230M1ZP",
    "33AACCM3861H1ZL",
    "27AAHCP4221D1ZW",
    "07AAACG0569R1ZK",
    "19AABCM1148Q1ZV",
    "06AABCF8832M1ZY",
    "24AABCE2207R1ZA",
    "32AADCD0871A1ZW",
]

# Invalid GSTINs for error rows
INVALID_GSTINS = [
    "INVALID123",
    "29AADCB2230",        # Too short
    "ZZZZZ1234ABCDE",     # Wrong format
    "27AABCS",            # Way too short
]

INVOICE_TYPES = ["B2B", "B2B", "B2B", "B2C", "CDNR"]
WRONG_TYPES   = ["B3B", "GST", "SALES"]
HSN_CODES     = ["998311", "998399", "996311", "997211", "85171290"]

# Valid invoice dates fall inside the filing period (June 2025)
PERIOD_START = datetime(int(PERIOD[:4]), int(PERIOD[4:]), 1)
PERIOD_DAYS  = 30


def period_date() -> str:
    return (PERIOD_START + timedelta(days=random.randint(0, PERIOD_DAYS - 1))).strftime("%d/%m/%Y")


def future_date() -> str:
    return (datetime.today() + timedelta(days=random.randint(1, 30))).strftime("%d/%m/%Y")


def generate_batch(batch_id: str, num_rows: int, error_rate: float = 0.25) -> pd.DataFrame:
    rows = []

    for i in range(num_rows):
        is_error = random.random() < error_rate
        row_num  = f"{batch_id}{i+1:03d}"

        buyer_gstin  = random.choice(VALID_GSTINS)
        invoice_date = period_date()
        inv_type     = random.choice(INVOICE_TYPES)
        taxable      = round(random.uniform(1000, 500000), 2)

        if is_error:
            error_type = random.choice(["gstin", "date", "type", "amount"])
            if error_type == "gstin":
                buyer_gstin = random.choice(INVALID_GSTINS)
            elif error_type == "date":
                invoice_date = future_date()
            elif error_type == "type":
                inv_type = random.choice(WRONG_TYPES)
            else:  # amount
                taxable = round(random.uniform(-10000, -100), 2)

        igst  = round(taxable * 0.18, 2) if inv_type in ["B2B", "CDNR", "EXP"] else 0
        cgst  = round(taxable * 0.09, 2) if inv_type == "B2C" else 0
        sgst  = round(taxable * 0.09, 2) if inv_type == "B2C" else 0
        total = round(taxable + igst + cgst + sgst, 2)

        rows.append({
            "taxpayer_gstin"      : SWIGGY_GSTIN,
            "receiver_gstin"      : buyer_gstin,
            "invoice_number"      : f"SWG/{PERIOD}/{row_num}",
            "invoice_date"        : invoice_date,
            "invoice_type"        : inv_type,
            "taxable_value"       : taxable,
            "igst_amount"         : igst,
            "cgst_amount"         : cgst,
            "sgst_amount"         : sgst,
            "total_invoice_value" : total,
            "hsn_code"            : random.choice(HSN_CODES),
        })

    return pd.DataFrame(rows)


def main():
    random.seed(42)  # reproducible demo numbers
    s3 = boto3.client("s3", region_name=REGION)

    # Create manifest
    manifest = {"total_batches": 3, "period": PERIOD, "client": "swiggy"}
    manifest_key = f"raw/{PERIOD}/gstr1/manifest.json"
    s3.put_object(Bucket=BUCKET, Key=manifest_key, Body=json.dumps(manifest))
    print(f"Uploaded: s3://{BUCKET}/{manifest_key}")

    # Generate and upload batches
    batch_config = [("B1", 20), ("B2", 15), ("B3", 10)]

    for batch_id, num_rows in batch_config:
        df = generate_batch(batch_id, num_rows)

        # Save locally
        batch_dir = os.path.join(INPUT_DIR, batch_id)
        os.makedirs(batch_dir, exist_ok=True)
        local_path = os.path.join(batch_dir, f"gstr1_{batch_id}.xlsx")
        with pd.ExcelWriter(local_path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="GSTR1 Data", index=False)
        print(f"Created local: {local_path} ({len(df)} rows)")

        # Upload to S3
        s3_key = f"raw/{PERIOD}/gstr1/{batch_id}/gstr1_{batch_id}.xlsx"
        s3.upload_file(local_path, BUCKET, s3_key)
        print(f"Uploaded: s3://{BUCKET}/{s3_key}")

        # Upload trigger file
        trigger = {"batch_id": batch_id, "period": PERIOD, "timestamp": datetime.now().isoformat()}
        trigger_key = f"raw/{PERIOD}/gstr1/{batch_id}/trigger_{batch_id}.json"
        s3.put_object(Bucket=BUCKET, Key=trigger_key, Body=json.dumps(trigger))

    print("\nSample data created and uploaded successfully!")
    print(f"S3 bucket: {BUCKET}")
    print(f"Period: {PERIOD}")


if __name__ == "__main__":
    main()
