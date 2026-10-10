"""
create_sample_data.py
Generates realistic Swiggy GST input files for several months, with planted issues,
writes them to a local mirror (sample_data/raw/...) and uploads the same tree to S3 raw/.

Per period:
  gstr1/manifest.json                      {"total_batches": N}
  gstr1/Bx/gstr1_Bx.xlsx + trigger_Bx.json  sales invoices (~25% errors)
  gstr1/Bx/corrections_Bx.xlsx             client corrections for ~75% of the error rows
  einv_bolton/einv_bolton.xlsx             e-invoice (IRN) data from Bolton
  ewb_bolton/ewb_bolton.xlsx               e-way bill data from Bolton
  pr/purchase_register.xlsx                Swiggy purchase register (~20% errors)
  2b_bolton/2b_<GSTIN>.xlsx                GSTR-2B per Swiggy GSTIN, sheets B2B + CDNR

Run: python pipeline\\create_sample_data.py [--periods 202504 202505 202506] [--no-upload]
"""
import argparse
import json
import os
import random
import shutil
from datetime import datetime, timedelta

import boto3
import pandas as pd

BUCKET = os.environ.get("S3_BUCKET", "swiggy-idt-poc-bucket")
REGION = "ap-south-1"

REPO_ROOT  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLE_DIR = os.path.join(REPO_ROOT, "sample_data")

# period -> [(batch_id, rows)]; batch counts differ per month to show the dynamic manifest
PERIODS = {
    "202504": [("B1", 18), ("B2", 12)],
    "202505": [("B1", 25), ("B2", 20), ("B3", 15), ("B4", 10)],
    "202506": [("B1", 20), ("B2", 15), ("B3", 10)],
}

SWIGGY_GSTINS = {"27AABCS1429B1ZB": "Maharashtra", "29AABCS1429B1ZC": "Karnataka"}
SWIGGY_GSTIN  = "27AABCS1429B1ZB"  # GSTR1 is filed from Maharashtra

# Valid buyer GSTINs
VALID_GSTINS = [
    "29AADCB2230M1ZP", "33AACCM3861H1ZL", "27AAHCP4221D1ZW", "07AAACG0569R1ZK",
    "19AABCM1148Q1ZV", "06AABCF8832M1ZY", "24AABCE2207R1ZA", "32AADCD0871A1ZW",
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

# Vendors supplying to Swiggy (purchase side)
SUPPLIERS = [
    ("27AAACR5055K1Z7", "Reliance Retail Ltd",        "RRL"),
    ("29AAACI1681G1ZM", "Infosys BPM Ltd",            "IBPM"),
    ("27AABCU9603R1ZX", "Urban Packaging Pvt Ltd",    "UPK"),
    ("33AAACT2727Q1ZW", "TVS Logistics Services Ltd", "TVSL"),
    ("29AAGCA4975J1Z2", "Amazon Web Services India",  "AWS"),
    ("07AAACB2894G1ZP", "Bharti Airtel Ltd",          "BAL"),
    ("24AAACH7409R1ZZ", "HUL Food Solutions",         "HUL"),
    ("29AADCS0472N1ZS", "Shadowfax Technologies",     "SFX"),
]


def fmt(d: datetime) -> str:
    return d.strftime("%d/%m/%Y")


class PeriodGenerator:
    def __init__(self, period: str):
        self.period = period
        self.rng    = random.Random(int(period))  # reproducible per month
        self.start  = datetime(int(period[:4]), int(period[4:]), 1)
        self.stats  = {}

    # ------------------------------------------------------------------ helpers
    def period_date(self) -> datetime:
        return self.start + timedelta(days=self.rng.randint(0, 27))

    def future_date(self) -> datetime:
        return datetime.today() + timedelta(days=self.rng.randint(1, 30))

    def taxes(self, taxable: float, intra_state: bool) -> dict:
        if intra_state:
            half = round(taxable * 0.09, 2)
            return {"igst": 0.0, "cgst": half, "sgst": half}
        return {"igst": round(taxable * 0.18, 2), "cgst": 0.0, "sgst": 0.0}

    # ------------------------------------------------------------------ GSTR1 sales
    def gstr1_batch(self, batch_id: str, num_rows: int, error_rate: float = 0.25):
        """Returns (sales_df, corrections_df, clean_rows) — clean_rows feed EINV/EWB."""
        rng, rows, corrections, clean = self.rng, [], [], []

        for i in range(num_rows):
            buyer_gstin  = rng.choice(VALID_GSTINS)
            invoice_date = fmt(self.period_date())
            inv_type     = rng.choice(INVOICE_TYPES)
            taxable      = round(rng.uniform(1000, 500000), 2)
            invoice_no   = f"SWG/{self.period}/{batch_id}{i+1:03d}"
            error_type   = rng.choice(["gstin", "date", "type", "amount"]) if rng.random() < error_rate else None

            # the "true" values, used for the client's correction
            true_vals = {"receiver_gstin": buyer_gstin, "invoice_date": invoice_date,
                         "invoice_type": inv_type, "taxable_value": taxable}

            if error_type == "gstin":
                buyer_gstin = rng.choice(INVALID_GSTINS)
            elif error_type == "date":
                invoice_date = fmt(self.future_date())
            elif error_type == "type":
                inv_type = rng.choice(WRONG_TYPES)
            elif error_type == "amount":
                taxable = -taxable

            row = self._sales_row(invoice_no, buyer_gstin, invoice_date, inv_type, taxable)
            rows.append(row)

            if error_type is None:
                clean.append(row)
            elif rng.random() < 0.75:
                field = {"gstin": "receiver_gstin", "date": "invoice_date",
                         "type": "invoice_type", "amount": "taxable_value"}[error_type]
                fix = {"invoice_number": invoice_no, field: true_vals[field]}
                if error_type == "amount":
                    fixed_row = self._sales_row(invoice_no, buyer_gstin, invoice_date, inv_type, true_vals["taxable_value"])
                    fix.update({k: fixed_row[k] for k in ["igst_amount", "cgst_amount", "sgst_amount", "total_invoice_value"]})
                fix["correction_remark"] = f"Corrected {field.replace('_', ' ')}"
                corrections.append(fix)

        cols = ["invoice_number", "receiver_gstin", "invoice_date", "invoice_type", "taxable_value",
                "igst_amount", "cgst_amount", "sgst_amount", "total_invoice_value", "correction_remark"]
        errors = sum(1 for r in rows if r not in clean)
        self.stats[f"gstr1_{batch_id}"] = f"{len(rows)} rows, {errors} errors, {len(corrections)} corrected by client"
        return pd.DataFrame(rows), pd.DataFrame(corrections, columns=cols), clean

    def _sales_row(self, invoice_no, buyer_gstin, invoice_date, inv_type, taxable) -> dict:
        igst  = round(taxable * 0.18, 2) if inv_type in ["B2B", "CDNR", "EXP"] else 0
        cgst  = round(taxable * 0.09, 2) if inv_type == "B2C" else 0
        sgst  = round(taxable * 0.09, 2) if inv_type == "B2C" else 0
        return {
            "taxpayer_gstin"      : SWIGGY_GSTIN,
            "receiver_gstin"      : buyer_gstin,
            "invoice_number"      : invoice_no,
            "invoice_date"        : invoice_date,
            "invoice_type"        : inv_type,
            "taxable_value"       : taxable,
            "igst_amount"         : igst,
            "cgst_amount"         : cgst,
            "sgst_amount"         : sgst,
            "total_invoice_value" : round(taxable + igst + cgst + sgst, 2),
            "hsn_code"            : self.rng.choice(HSN_CODES),
        }

    # ------------------------------------------------------------------ reco sources
    def _plant(self, rows: list, name: str, drop=0.10, diff=0.08, typo=2, extra=3):
        """Shared planting logic: drop rows, change amounts, typo invoice numbers, add extras.
        Returns list of (row, inv_no, taxable) for the reco source."""
        rng = self.rng
        out, counts = [], {"dropped": 0, "amount_diff": 0, "inv_typo": 0, "format_variant": 0, "extra": extra}
        typo_left = typo
        for r in rows:
            if rng.random() < drop:
                counts["dropped"] += 1
                continue
            inv_no, taxable = r["invoice_number"], r["taxable_value"]
            roll = rng.random()
            if roll < diff:
                taxable = round(taxable + rng.choice([-1, 1]) * rng.uniform(100, 5000), 2)
                counts["amount_diff"] += 1
            elif typo_left > 0 and roll < diff + 0.15:
                inv_no += "A"
                typo_left -= 1
                counts["inv_typo"] += 1
            elif roll < 0.40:
                inv_no = inv_no.replace("/", "-")  # same invoice, different format
                counts["format_variant"] += 1
            out.append((r, inv_no, taxable))
        for j in range(extra):
            ghost = self._sales_row(f"SWG/{self.period}/X{j+1:03d}", rng.choice(VALID_GSTINS),
                                    fmt(self.period_date()), "B2B", round(rng.uniform(1000, 200000), 2))
            out.append((ghost, ghost["invoice_number"], ghost["taxable_value"]))
        self.stats[name] = ", ".join(f"{k}={v}" for k, v in counts.items())
        return out

    def einv(self, clean_rows: list) -> pd.DataFrame:
        eligible = [r for r in clean_rows if r["invoice_type"] in ("B2B", "CDNR")]
        records = []
        for r, inv_no, taxable in self._plant(eligible, "einv"):
            ack = self.period_date()
            records.append({
                "Supplier GSTIN"     : SWIGGY_GSTIN,
                "Buyer GSTIN"        : r["receiver_gstin"],
                "Document Type"      : "CRN" if r["invoice_type"] == "CDNR" else "INV",
                "Document No"        : inv_no,
                "Document Date"      : r["invoice_date"],
                "Taxable Value"      : taxable,
                "Total Invoice Value": round(taxable * 1.18, 2),
                "IRN"                : "%064x" % self.rng.getrandbits(256),
                "Ack No"             : str(self.rng.randint(10**14, 10**15 - 1)),
                "Ack Date"           : fmt(ack),
                "IRN Status"         : "ACT",
            })
        return pd.DataFrame(records)

    def ewb(self, clean_rows: list) -> pd.DataFrame:
        # E-way bill needed for goods movement above Rs.50,000
        eligible = [r for r in clean_rows
                    if r["invoice_type"] in ("B2B", "CDNR") and r["total_invoice_value"] > 50000]
        records = []
        for r, inv_no, taxable in self._plant(eligible, "ewb", diff=0.05, extra=2):
            ewb_date = datetime.strptime(r["invoice_date"], "%d/%m/%Y")
            records.append({
                "EWB No"          : str(self.rng.randint(10**11, 10**12 - 1)),
                "EWB Date"        : fmt(ewb_date),
                "Doc Type"        : "Tax Invoice",
                "Doc No"          : inv_no,
                "Doc Date"        : r["invoice_date"],
                "From GSTIN"      : SWIGGY_GSTIN,
                "To GSTIN"        : r["receiver_gstin"],
                "Assessable Value": taxable,
                "Total Value"     : round(taxable * 1.18, 2),
                "Valid Upto"      : fmt(ewb_date + timedelta(days=3)),
                "Vehicle No"      : f"MH{self.rng.randint(1, 50):02d}AB{self.rng.randint(1000, 9999)}",
            })
        return pd.DataFrame(records)

    # ------------------------------------------------------------------ purchase side
    def purchase_register(self, num_rows: int = 40, error_rate: float = 0.20):
        rng, rows, clean = self.rng, [], []
        seq = {code: 0 for _, _, code in SUPPLIERS}
        for _ in range(num_rows):
            sup_gstin, sup_name, code = rng.choice(SUPPLIERS)
            recipient = rng.choice(list(SWIGGY_GSTINS))
            seq[code] += 1
            inv_no    = f"{code}/{self.period[2:]}/{seq[code]:04d}"
            inv_type  = "CDNR" if rng.random() < 0.15 else "B2B"
            inv_date  = self.period_date()
            taxable   = round(rng.uniform(5000, 800000), 2)
            tax       = self.taxes(taxable, sup_gstin[:2] == recipient[:2])
            itc       = round(sum(tax.values()), 2)
            error     = rng.choice(["gstin", "date", "amount", "itc"]) if rng.random() < error_rate else None

            if error == "gstin":
                sup_gstin = rng.choice(INVALID_GSTINS)
            elif error == "date":
                inv_date = self.future_date()
            elif error == "amount":
                taxable = -taxable
            elif error == "itc":
                itc = round(itc * 1.5, 2)

            row = {
                "Recipient GSTIN"    : recipient,
                "Supplier GSTIN"     : sup_gstin,
                "Supplier Name"      : sup_name,
                "Invoice No"         : inv_no,
                "Invoice Date"       : fmt(inv_date),
                "Invoice Type"       : inv_type,
                "Taxable Value"      : taxable,
                "IGST"               : tax["igst"],
                "CGST"               : tax["cgst"],
                "SGST"               : tax["sgst"],
                "Total Invoice Value": round(taxable + sum(tax.values()), 2),
                "ITC Eligible"       : "Y",
                "ITC Claimed"        : itc,
            }
            rows.append(row)
            if error is None:
                clean.append(row)
        self.stats["purchase_register"] = f"{len(rows)} rows, {len(rows) - len(clean)} errors"
        return pd.DataFrame(rows), clean

    def gstr2b(self, clean_pr: list) -> dict:
        """{recipient_gstin: {"B2B": df, "CDNR": df}} — headers as in the GSTR-2B download."""
        rng = self.rng
        sales_like = [{"invoice_number": r["Invoice No"], "taxable_value": r["Taxable Value"], "_pr": r}
                      for r in clean_pr]
        planted = self._plant(sales_like, "gstr2b", diff=0.06, extra=0)

        files = {g: {"B2B": [], "CDNR": []} for g in SWIGGY_GSTINS}
        for item, inv_no, taxable in planted:
            pr  = item["_pr"]
            tax = self.taxes(taxable, pr["Supplier GSTIN"][:2] == pr["Recipient GSTIN"][:2])
            self._add_2b_row(files[pr["Recipient GSTIN"]], pr["Supplier GSTIN"], pr["Supplier Name"],
                             inv_no, pr["Invoice Date"], pr["Invoice Type"], taxable, tax)

        # vendor uploaded, Swiggy has not booked yet (only in 2B)
        for j in range(3):
            sup_gstin, sup_name, code = rng.choice(SUPPLIERS)
            recipient = rng.choice(list(SWIGGY_GSTINS))
            taxable   = round(rng.uniform(5000, 300000), 2)
            self._add_2b_row(files[recipient], sup_gstin, sup_name, f"{code}/{self.period[2:]}/9{j+1:03d}",
                             fmt(self.period_date()), "B2B", taxable,
                             self.taxes(taxable, sup_gstin[:2] == recipient[:2]))
        self.stats["gstr2b"] += ", only_in_2b=3"

        return {g: {s: pd.DataFrame(rows) for s, rows in sheets.items()} for g, sheets in files.items()}

    @staticmethod
    def _add_2b_row(sheets, sup_gstin, sup_name, inv_no, inv_date, inv_type, taxable, tax):
        value = round(taxable + sum(tax.values()), 2)
        common = {"GSTIN of supplier": sup_gstin, "Trade/Legal name": sup_name}
        amounts = {"Taxable Value": taxable, "Integrated Tax": tax["igst"],
                   "Central Tax": tax["cgst"], "State/UT Tax": tax["sgst"], "ITC Availability": "Yes"}
        if inv_type == "CDNR":
            sheets["CDNR"].append({**common, "Note number": inv_no, "Note type": "Credit Note",
                                   "Note date": inv_date, "Note Value": value, **amounts})
        else:
            sheets["B2B"].append({**common, "Invoice number": inv_no, "Invoice type": "Regular",
                                  "Invoice Date": inv_date, "Invoice Value": value, **amounts})


def write_excel(path: str, sheets: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name, index=False)


def write_json(path: str, data: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f)


def generate_period(period: str) -> str:
    gen  = PeriodGenerator(period)
    root = os.path.join(SAMPLE_DIR, "raw", period)
    shutil.rmtree(root, ignore_errors=True)

    batches = PERIODS[period]
    write_json(os.path.join(root, "gstr1", "manifest.json"),
               {"total_batches": len(batches), "period": period, "client": "swiggy"})

    all_clean = []
    for batch_id, num_rows in batches:
        sales, corrections, clean = gen.gstr1_batch(batch_id, num_rows)
        all_clean += clean
        bdir = os.path.join(root, "gstr1", batch_id)
        write_excel(os.path.join(bdir, f"gstr1_{batch_id}.xlsx"), {"GSTR1 Data": sales})
        write_excel(os.path.join(bdir, f"corrections_{batch_id}.xlsx"), {"Corrections": corrections})
        write_json(os.path.join(bdir, f"trigger_{batch_id}.json"),
                   {"batch_id": batch_id, "period": period, "timestamp": datetime.now().isoformat()})

    write_excel(os.path.join(root, "einv_bolton", "einv_bolton.xlsx"), {"EINV": gen.einv(all_clean)})
    write_excel(os.path.join(root, "ewb_bolton", "ewb_bolton.xlsx"),   {"EWB": gen.ewb(all_clean)})

    pr, clean_pr = gen.purchase_register()
    write_excel(os.path.join(root, "pr", "purchase_register.xlsx"), {"Purchase Register": pr})
    for gstin, sheets in gen.gstr2b(clean_pr).items():
        write_excel(os.path.join(root, "2b_bolton", f"2b_{gstin}.xlsx"), sheets)

    print(f"\nPeriod {period} — planted data:")
    for k, v in gen.stats.items():
        print(f"  {k:<18} {v}")
    return root


def upload_tree(local_root: str, period: str):
    s3 = boto3.client("s3", region_name=REGION)
    n  = 0
    for dirpath, _, files in os.walk(local_root):
        for name in files:
            local = os.path.join(dirpath, name)
            rel   = os.path.relpath(local, local_root).replace(os.sep, "/")
            s3.upload_file(local, BUCKET, f"raw/{period}/{rel}")
            n += 1
    print(f"  Uploaded {n} files to s3://{BUCKET}/raw/{period}/")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--periods", nargs="+", default=list(PERIODS), choices=list(PERIODS))
    parser.add_argument("--no-upload", action="store_true", help="only write the local mirror")
    args = parser.parse_args()

    for period in args.periods:
        root = generate_period(period)
        if not args.no_upload:
            upload_tree(root, period)

    print(f"\nSample data ready. Local: {os.path.join(SAMPLE_DIR, 'raw')}  S3: s3://{BUCKET}/raw/")


if __name__ == "__main__":
    main()
