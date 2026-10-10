"""
swiggy_gstr1_batch_dag.py
DAG 1 — Per batch GSTR1 validation pipeline
Trigger: manually or via S3 event
Params : {period: "202506", batch_id: "B1"}
"""
import json
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

sys.path.insert(0, "/opt/airflow")


def _read_manifest(s3, bucket, period) -> dict:
    obj = s3.get_object(Bucket=bucket, Key=f"raw/{period}/gstr1/manifest.json")
    return json.loads(obj["Body"].read())


def read_manifest(**context):
    from pipeline.write_to_s3 import BUCKET, s3_client

    manifest = _read_manifest(s3_client(), BUCKET, context["params"]["period"])
    print(f"Manifest: total_batches={manifest['total_batches']}")
    return manifest["total_batches"]


def run_gstr1_validation(**context):
    from activities.gstr1_validation.src.transformation import run
    from pipeline.write_to_s3 import read_excel, write_parquet

    period   = context["params"]["period"]
    batch_id = context["params"]["batch_id"]

    key = f"raw/{period}/gstr1/{batch_id}/gstr1_{batch_id}.xlsx"
    df  = read_excel(key)
    print(f"Read {len(df)} rows from {key}")

    result = run(df, batch_id=batch_id)

    path = write_parquet(result["result"], f"intermediate/{period}/gstr1/{batch_id}_validated.parquet")
    print(f"Written parquet: {path}")
    return {"success": len(result["success"]), "errors": len(result["errors"])}


def mark_batch_complete(**context):
    from pipeline.write_to_s3 import BUCKET, s3_client

    period   = context["params"]["period"]
    batch_id = context["params"]["batch_id"]
    s3_client().put_object(Bucket=BUCKET, Key=f"batch_status/{period}/{batch_id}.done", Body=b"done")
    print(f"Marked {batch_id} as done")


def check_all_batches_done(**context):
    from pipeline.write_to_s3 import BUCKET, list_keys, s3_client

    period = context["params"]["period"]
    s3     = s3_client()

    done_count = len(list_keys(f"batch_status/{period}/", ".done", s3=s3))
    total      = _read_manifest(s3, BUCKET, period)["total_batches"]
    print(f"Done: {done_count} / {total}")

    if done_count >= total:
        return "trigger_combine_dag"
    return "wait_for_more_batches"


with DAG(
    dag_id="swiggy_gstr1_batch_dag",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    params={"period": "202506", "batch_id": "B1"},
    tags=["swiggy", "gstr1", "batch"],
) as dag:

    t1 = PythonOperator(task_id="read_manifest",       python_callable=read_manifest)
    t2 = PythonOperator(task_id="gstr1_validation",    python_callable=run_gstr1_validation)
    t3 = PythonOperator(task_id="mark_batch_complete", python_callable=mark_batch_complete)
    t4 = BranchPythonOperator(task_id="check_all_batches_done", python_callable=check_all_batches_done)
    t5 = TriggerDagRunOperator(
        task_id="trigger_combine_dag",
        trigger_dag_id="swiggy_gstr1_combine_reco_dag",
        conf={"period": "{{ params.period }}"},
    )
    t6 = EmptyOperator(task_id="wait_for_more_batches")

    t1 >> t2 >> t3 >> t4 >> [t5, t6]
