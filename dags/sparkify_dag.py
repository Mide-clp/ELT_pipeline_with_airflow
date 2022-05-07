from airflow import DAG
from datetime import datetime
from airflow.operators.dummy import DummyOperator


default_args = {
    "owner": "Mide Clp",
    "depends_on_past": False,
    "start_date": datetime(2022, 5, 7),
    "retries": 3,
    "retry_delay": datetime(minute=1),
    "email_on_failure" : False
    "catchup": False
}


dag = DAG(
    "sparkify_ELT",
    default_args=default_args,
    schedule_interval="@monthly",
    description=" ELT pipeline for loading sparkify music data to the data warehouse"
)

begin_execution = DummyOperator(
    task_id="begin_execution",
    dag=dag
)

