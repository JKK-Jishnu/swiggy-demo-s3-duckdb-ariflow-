"""
=====================================================================================================
SCRIPT   : Athena / Glue Data Catalog for the Swiggy GST history
-----------------------------------------------------------------------------------------------------
Every job writes its parquet output to  intermediate/<table>/period=<YYYYMM>/  in S3.
This script registers those folders as tables in the Glue Data Catalog (database "swiggy_gst"),
so every month ever processed can be queried with SQL in Amazon Athena.

  python athena/athena.py setup                 create workgroup, database, tables, views + check them
  python athena/athena.py query <file.sql>      run a saved query, e.g. athena/queries/03_gstr1_monthly_trend.sql
  python athena/athena.py query "SELECT ..."    run any SQL

WHAT "setup" CREATES
  workgroup  swiggy-gst              query results go to s3://<bucket>/athena-results/
  database   swiggy_gst              one table per job output, see athena/tables/*.sql
  views                              monthly summaries, ITC at risk, record traces - athena/views/*.sql

  Tables use "partition projection": the month folders (period=YYYYMM) are found automatically,
  so a new month is queryable as soon as the jobs have written it. No Glue crawler, no
  MSCK REPAIR TABLE. Setup is safe to re-run: tables are dropped and re-created
  (dropping an external table never deletes the data in S3).

IN THE AWS CONSOLE
  Athena -> Query editor -> Workgroup "swiggy-gst" -> Database "swiggy_gst"
  Glue   -> Data Catalog -> Databases -> swiggy_gst   (table and column descriptions)
=====================================================================================================
"""
import os
import sys
import time

import boto3
import pandas as pd

BUCKET    = os.environ.get("S3_BUCKET", "swiggy-idt-poc-bucket")
REGION    = os.environ.get("AWS_DEFAULT_REGION", "ap-south-1")
DATABASE  = "swiggy_gst"
WORKGROUP = "swiggy-gst"
RESULTS   = f"s3://{BUCKET}/athena-results/"
HERE      = os.path.dirname(os.path.abspath(__file__))

athena = boto3.client("athena", region_name=REGION)


# =====================================================================================================
# HELPERS
# =====================================================================================================
def read_sql(path: str) -> str:
    """SQL file -> statement: drop full-line '--' comments and fill in the bucket name."""
    with open(path, encoding="utf-8") as f:
        lines = [line for line in f if not line.lstrip().startswith("--")]
    return "".join(lines).strip().rstrip(";").replace("<BUCKET>", BUCKET)


def run_query(sql: str) -> pd.DataFrame:
    """Run one statement in Athena, wait for it, return the result rows (empty for DDL)."""
    execution_id = athena.start_query_execution(
        QueryString=sql,
        WorkGroup=WORKGROUP,
        QueryExecutionContext={"Database": DATABASE},
    )["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(QueryExecutionId=execution_id)["QueryExecution"]["Status"]
        if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if status["State"] != "SUCCEEDED":
        raise RuntimeError(f"Athena query {status['State']}: {status.get('StateChangeReason', '')}\n{sql[:400]}")

    rows = []
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=execution_id):
        for row in page["ResultSet"]["Rows"]:
            rows.append([cell.get("VarCharValue") for cell in row["Data"]])
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows[1:], columns=rows[0])          # first row holds the column names


def ensure_workgroup():
    existing = [wg["Name"] for wg in athena.list_work_groups()["WorkGroups"]]
    if WORKGROUP in existing:
        return
    athena.create_work_group(
        Name=WORKGROUP,
        Description="Swiggy GST pipelines - history queries",
        Configuration={
            "ResultConfiguration": {"OutputLocation": RESULTS},
            "EnforceWorkGroupConfiguration": False,
            "PublishCloudWatchMetricsEnabled": False,
            "BytesScannedCutoffPerQuery": 10 * 1024 ** 3,     # safety limit: 10 GB scanned per query
        },
    )
    print(f"  Created workgroup {WORKGROUP} (results in {RESULTS})")


# =====================================================================================================
# COMMANDS
# =====================================================================================================
def setup():
    print(f"[Athena setup] bucket={BUCKET} region={REGION}")
    ensure_workgroup()

    run_query(f"CREATE DATABASE IF NOT EXISTS {DATABASE} "
              f"COMMENT 'Swiggy GST pipelines - every month of job output (parquet in S3)'")
    print(f"  Database {DATABASE} ready")

    table_files = sorted(f for f in os.listdir(os.path.join(HERE, "tables")) if f.endswith(".sql"))
    for file_name in table_files:
        table = file_name[:-len(".sql")]
        run_query(f"DROP TABLE IF EXISTS {DATABASE}.{table}")
        run_query(read_sql(os.path.join(HERE, "tables", file_name)))
        print(f"  Table  {DATABASE}.{table}")

    for file_name in sorted(f for f in os.listdir(os.path.join(HERE, "views")) if f.endswith(".sql")):
        run_query(read_sql(os.path.join(HERE, "views", file_name)))
        print(f"  View   {DATABASE}.{file_name[:-len('.sql')]}")

    # check: every table can be read and shows its months
    print("\n  Rows per table and month:")
    for file_name in table_files:
        table = file_name[:-len(".sql")]
        counts = run_query(f"SELECT period, count(*) AS n FROM {DATABASE}.{table} GROUP BY period ORDER BY period")
        months = ", ".join(f"{p}={n}" for p, n in counts.itertuples(index=False)) or "(no data yet)"
        print(f"  {table:<22} {months}")


def query(sql_or_file: str):
    sql = read_sql(sql_or_file) if sql_or_file.endswith(".sql") else sql_or_file
    result = run_query(sql)
    with pd.option_context("display.width", 250, "display.max_columns", 50, "display.max_colwidth", 80):
        print(result.to_string(index=False) if not result.empty else "(no rows)")


if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1] == "setup":
        setup()
    elif len(sys.argv) >= 3 and sys.argv[1] == "query":
        query(sys.argv[2])
    else:
        print(__doc__)
        sys.exit(1)
