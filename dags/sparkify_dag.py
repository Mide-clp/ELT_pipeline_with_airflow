from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from operators.stage_redshift import StageToRedshiftOperator

default_args = {
    "owner": "Mide Clp",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
    "catchup": False
}

with DAG(
        "sparkify_ELT",
        default_args=default_args,
        start_date=datetime(2018, 11, 1),
        end_date=datetime(2018, 11, 30),
        schedule_interval="@monthly",
        description=" ELT pipeline for loading sparkify music data to the data warehouse"
) as dag:

    begin_execution = DummyOperator(
        task_id="begin_execution",
        dag=dag
    )
    with TaskGroup("staging") as staging:

        stage_song = StageToRedshiftOperator(
            task_id="stage_song",
            destination_table="staging_songs",
            aws_credentials_id="aws_credentials",
            redshift_conn_id="redshift",
            s3_bucket="udacity-dend",
            s3_key="song_data/A/A/A",
            file_format="json",
            region="us-west-2",
            json_path="auto",

        )

        stage_event = StageToRedshiftOperator(
            task_id="stage_event",
            destination_table="staging_events",
            aws_credentials_id="aws_credentials",
            redshift_conn_id="redshift",
            s3_bucket="udacity-dend",
            s3_key="log_data/{execution_date.year}/{execution_date.month}",
            file_format="json",
            region="us-west-2",
            json_path="s3://udacity-dend/log_json_path.json",
        )

    begin_execution >> staging
