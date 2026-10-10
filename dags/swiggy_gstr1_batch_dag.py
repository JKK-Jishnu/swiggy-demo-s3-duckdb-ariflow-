"""
swiggy_gstr1_batch_dag.py
DAG 1 — Per batch GSTR1 pipeline (Activities 1-3)
Trigger: manually or via S3 event (trigger_Bx.json)
Params : {period: "202506", batch_id: "B1"}

read_manifest → gstr1_validation → gstr1_error_fixing → gstr1_upload_file_creation
  → mark_batch_complete → check_all_batches_done → trigger_combine_dag | wait_for_more_batches
"""
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

sys.path.insert(0, "/opt/airflow")


def _p(context):
    return context["params"]["period"], context["params"]["batch_id"]


def read_manifest(**context):
    from pipeline import steps
    manifest = steps.read_manifest(context["params"]["period"])
    print(f"Manifest: total_batches={manifest['total_batches']}")
    return manifest["total_batches"]


def gstr1_validation(**context):
    from pipeline import steps
    return steps.validate_batch(*_p(context))


def gstr1_error_fixing(**context):
    from pipeline import steps
    return steps.fix_batch(*_p(context))


def gstr1_upload_file_creation(**context):
    from pipeline import steps
    return steps.create_upload_file(*_p(context))


def mark_batch_complete(**context):
    from pipeline import steps
    steps.mark_batch_done(*_p(context))
    print(f"Marked {_p(context)[1]} as done")


def check_all_batches_done(**context):
    from pipeline import steps
    done, total = steps.batch_progress(context["params"]["period"])
    print(f"Done: {done} / {total}")
    return "trigger_combine_dag" if done >= total else "wait_for_more_batches"


with DAG(
    dag_id="swiggy_gstr1_batch_dag",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    params={"period": "202506", "batch_id": "B1"},
    tags=["swiggy", "gstr1", "batch"],
) as dag:

    t1 = PythonOperator(task_id="read_manifest",              python_callable=read_manifest)
    t2 = PythonOperator(task_id="gstr1_validation",           python_callable=gstr1_validation)
    t3 = PythonOperator(task_id="gstr1_error_fixing",         python_callable=gstr1_error_fixing)
    t4 = PythonOperator(task_id="gstr1_upload_file_creation", python_callable=gstr1_upload_file_creation)
    t5 = PythonOperator(task_id="mark_batch_complete",        python_callable=mark_batch_complete)
    t6 = BranchPythonOperator(task_id="check_all_batches_done", python_callable=check_all_batches_done)
    t7 = TriggerDagRunOperator(
        task_id="trigger_combine_dag",
        trigger_dag_id="swiggy_gstr1_combine_reco_dag",
        conf={"period": "{{ params.period }}"},
    )
    t8 = EmptyOperator(task_id="wait_for_more_batches")

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> [t7, t8]
