"""
gst_utils.py — shared GST helpers used by all activities.
"""
import re

import pandas as pd

GSTIN_PATTERN = re.compile(r"^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}Z[0-9A-Z]{1}$")
DATE_FORMATS  = ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%y", "%d-%b-%Y"]

STATE_CODES = {
    "01": "Jammu and Kashmir", "02": "Himachal Pradesh", "03": "Punjab", "04": "Chandigarh",
    "05": "Uttarakhand", "06": "Haryana", "07": "Delhi", "08": "Rajasthan", "09": "Uttar Pradesh",
    "10": "Bihar", "18": "Assam", "19": "West Bengal", "20": "Jharkhand", "21": "Odisha",
    "22": "Chhattisgarh", "23": "Madhya Pradesh", "24": "Gujarat", "27": "Maharashtra",
    "29": "Karnataka", "30": "Goa", "32": "Kerala", "33": "Tamil Nadu", "36": "Telangana",
    "37": "Andhra Pradesh",
}


def validate_gstin_format(gstin: str) -> bool:
    """15 char GSTIN: 2 digit state + 10 char PAN + entity no. + Z + checksum char"""
    if not gstin:
        return False
    gstin = str(gstin).strip().upper()
    return len(gstin) == 15 and bool(GSTIN_PATTERN.match(gstin))


def parse_dates(series: pd.Series) -> pd.Series:
    """Try each known format in turn; unparseable values stay NaT."""
    parsed = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    for fmt in DATE_FORMATS:
        missing = parsed.isna()
        if not missing.any():
            break
        parsed[missing] = pd.to_datetime(series[missing].astype(str), format=fmt, errors="coerce")
    return parsed


def normalise_invoice_no(series: pd.Series) -> pd.Series:
    """'SWG/202506/B1001' and 'swg-202506-b1001' both become 'SWG202506B1001'."""
    return series.fillna("").astype(str).str.upper().str.replace(r"[^0-9A-Z]", "", regex=True)


def clean_text(series: pd.Series, upper: bool = False) -> pd.Series:
    s = series.fillna("").astype(str).str.strip()
    return s.str.upper() if upper else s


def place_of_supply(gstin: str) -> str:
    code = str(gstin)[:2]
    return f"{code}-{STATE_CODES.get(code, 'Unknown')}"
