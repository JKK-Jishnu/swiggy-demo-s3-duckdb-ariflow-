"""
swiggy_2b_pr_dag.py
DAG 3 — Purchase Register / GSTR-2B track (Activities 7-9), runs after GSTR1, usually manual
Params : {period: "202506"}

[pr_validation ∥ gstr2b_consolidation] → gstr2b_vs_pr_reco → generate_summary

pr_validation also exports final/<period>/pr_errors_for_client.xlsx. In production the
team waits for client corrections before reco; for the demo the reco runs on the valid rows.
"""
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")


def _period(context) -> str:
    conf = context["dag_run"].conf or {}
    return conf.get("period") or context["params"]["period"]


def pr_validation(**context):
    from pipeline import steps
    return steps.pr_validation(_period(context))


def gstr2b_consolidation(**context):
    from pipeline import steps
    return steps.gstr2b_consolidation(_period(context))


def gstr2b_vs_pr_reco(**context):
    from pipeline import steps
    return steps.gstr2b_pr_reco(_period(context))


def generate_summary(**context):
    from pipeline import steps
    return steps.pr_2b_summary(_period(context))


with DAG(
    dag_id="swiggy_2b_pr_dag",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    params={"period": "202506"},
    tags=["swiggy", "pr", "gstr2b", "reco"],
) as dag:

    t1 = PythonOperator(task_id="pr_validation",        python_callable=pr_validation)
    t2 = PythonOperator(task_id="gstr2b_consolidation", python_callable=gstr2b_consolidation)
    t3 = PythonOperator(task_id="gstr2b_vs_pr_reco",    python_callable=gstr2b_vs_pr_reco)
    t4 = PythonOperator(task_id="generate_summary",     python_callable=generate_summary)

    [t1, t2] >> t3 >> t4
