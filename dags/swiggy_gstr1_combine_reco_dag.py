"""
swiggy_gstr1_combine_reco_dag.py
DAG 2 — Combine all batches + summary
Auto-triggered by DAG 1 (conf={"period": ...}) when all batches complete
"""
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")


def _period(context) -> str:
    conf = context["dag_run"].conf or {}
    return conf.get("period") or context["params"]["period"]


def combine_batches(**context):
    import pandas as pd
    from pipeline.write_to_s3 import list_keys, read_parquet, write_parquet

    period = _period(context)
    keys   = list_keys(f"intermediate/{period}/gstr1/", "_validated.parquet")

    dfs = []
    for key in keys:
        dfs.append(read_parquet(key))
        print(f"Loaded: {key}")

    combined = pd.concat(dfs, ignore_index=True)
    print(f"Combined: {len(combined)} rows from {len(dfs)} batches")

    path = write_parquet(combined, f"intermediate/{period}/combined_sales.parquet")
    print(f"Written: {path}")


def generate_summary(**context):
    import duckdb
    from pipeline.write_to_s3 import read_parquet, write_excel

    period = _period(context)
    df     = read_parquet(f"intermediate/{period}/combined_sales.parquet")

    con = duckdb.connect()
    con.register("combined", df)
    summary = con.execute("""
        SELECT
            batch_id,
            COUNT(*)                                                 AS total,
            CAST(COUNT_IF(validation_status = 'Success') AS INTEGER) AS success,
            CAST(COUNT_IF(validation_status = 'Error')   AS INTEGER) AS errors,
            ROUND(SUM(taxable_value), 2)                             AS total_taxable_value
        FROM combined GROUP BY batch_id ORDER BY batch_id
    """).df()
    errors = df[df["validation_status"] == "Error"]

    path = write_excel({"Summary": summary, "Errors": errors},
                       f"final/{period}/summaries/gstr1_summary.xlsx")
    print(f"Summary written to {path}. Total rows: {len(df)}")
    print(summary.to_string(index=False))


with DAG(
    dag_id="swiggy_gstr1_combine_reco_dag",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    params={"period": "202506"},
    tags=["swiggy", "gstr1", "combine"],
) as dag:

    t1 = PythonOperator(task_id="combine_batches",  python_callable=combine_batches)
    t2 = PythonOperator(task_id="generate_summary", python_callable=generate_summary)

    t1 >> t2
