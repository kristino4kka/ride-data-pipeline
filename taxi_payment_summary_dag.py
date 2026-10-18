"""Airflow DAG for the taxi payment summary pipeline.

The DAG waits for the source Parquet file in S3 and then starts the
PySpark job on a Yandex Data Proc cluster.

Optional environment variables:
    DAG_S3_USER
    DATAPROC_CLUSTER_ID
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.providers.yandex.operators.dataproc import (
    DataprocCreatePysparkJobOperator,
)


DAG_ID = "taxi_payment_summary"
S3_BUCKET = "da-plus-dags"
S3_KEY = "project_04/taxi_data.parquet"
S3_USER = os.getenv("DAG_S3_USER", "your-user")
CLUSTER_ID = os.getenv("DATAPROC_CLUSTER_ID", "your-cluster-id")
SPARK_JOB_URI = f"s3a://{S3_BUCKET}/{S3_USER}/jobs/my_spark_job.py"


with DAG(
    dag_id=DAG_ID,
    description="Processes taxi payment data.",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["taxi", "daily"],
) as dag:
    wait_for_input = S3KeySensor(
        task_id="wait_for_input",
        bucket_name=S3_BUCKET,
        bucket_key=S3_KEY,
        mode="poke",
        aws_conn_id="s3",
        poke_interval=30,
        timeout=600,
    )

    run_pyspark = DataprocCreatePysparkJobOperator(
        name=DAG_ID,
        task_id="run_pyspark",
        cluster_id=CLUSTER_ID,
        main_python_file_uri=SPARK_JOB_URI,
    )

    wait_for_input >> run_pyspark
