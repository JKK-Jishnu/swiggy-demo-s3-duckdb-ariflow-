"""
write_to_s3.py — shared S3 helpers for scripts and Airflow DAGs.
Parquet/Excel are written via in-memory buffers, so no /tmp paths (works on Windows and in Docker).
"""
import io
import os

import boto3
import pandas as pd

BUCKET = os.environ.get("S3_BUCKET", "swiggy-idt-poc-bucket")
REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-south-1")


def s3_client():
    return boto3.client("s3", region_name=REGION)


def read_excel(key: str, sheet_name="GSTR1 Data", s3=None) -> pd.DataFrame:
    s3  = s3 or s3_client()
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_excel(io.BytesIO(obj["Body"].read()), sheet_name=sheet_name)


def read_parquet(key: str, s3=None) -> pd.DataFrame:
    s3  = s3 or s3_client()
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def write_parquet(df: pd.DataFrame, key: str, s3=None) -> str:
    s3  = s3 or s3_client()
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    return f"s3://{BUCKET}/{key}"


def write_excel(sheets: dict, key: str, s3=None) -> str:
    """sheets: {sheet_name: DataFrame}"""
    s3  = s3 or s3_client()
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name, index=False)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    return f"s3://{BUCKET}/{key}"


def list_keys(prefix: str, suffix: str = "", s3=None) -> list:
    s3    = s3 or s3_client()
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix)
    return sorted(o["Key"] for p in pages for o in p.get("Contents", []) if o["Key"].endswith(suffix))
