"""
swiggy_gstr1_combine_reco_dag.py
DAG 2 — Combine all batches + EINV/EWB reconciliation (Activities 4-6)
Auto-triggered by DAG 1 (conf={"period": ...}) when all batches complete

combine_batches → [einv_vs_sales_reco ∥ ewb_vs_sales_reco] → generate_summary
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
    from pipeline import steps
    return steps.combine(_period(context))


def einv_vs_sales_reco(**context):
    from pipeline import steps
    return steps.einv_reco(_period(context))


def ewb_vs_sales_reco(**context):
    from pipeline import steps
    return steps.ewb_reco(_period(context))


def generate_summary(**context):
    from pipeline import steps
    return steps.gstr1_summary(_period(context))


with DAG(
    dag_id="swiggy_gstr1_combine_reco_dag",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    params={"period": "202506"},
    tags=["swiggy", "gstr1", "combine", "reco"],
) as dag:

    t1 = PythonOperator(task_id="combine_batches",    python_callable=combine_batches)
    t2 = PythonOperator(task_id="einv_vs_sales_reco", python_callable=einv_vs_sales_reco)
    t3 = PythonOperator(task_id="ewb_vs_sales_reco",  python_callable=ewb_vs_sales_reco)
    t4 = PythonOperator(task_id="generate_summary",   python_callable=generate_summary)

    t1 >> [t2, t3] >> t4
