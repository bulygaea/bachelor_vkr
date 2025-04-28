# -*- coding: utf-8 -*-
from airflow.providers.yandex.operators.yandexcloud_dataproc import DataprocCreatePysparkJobOperator
from airflow import DAG
from datetime import datetime
import os


# Данные вашей инфраструктуры
YC_BUCKET = os.environ['YC_BUCKET']
CLUSTER_ID = os.environ['CLUSTER_ID']

with DAG(
    dag_id='dag_elt',
    start_date=datetime(2025, 3, 1),
    schedule_interval='* 10 * * *',
    catchup=False,
    max_active_runs=1,
) as dag:
    # Извлечение из PostgreSQL и загрузка в raw
    spark_postgres_to_raw = DataprocCreatePysparkJobOperator(
        task_id='spark_postgres_to_raw',
        main_python_file_uri=f's3a://{YC_BUCKET}/spark_scripts/extract_postgres_to_raw_hdfs.py',
        cluster_id=CLUSTER_ID,
        connection_id='yandexcloud_default',
    )

    # Извлечение из raw и загрузка в dst/nonpartitioned
    spark_raw_to_dst_nonpartitioned = DataprocCreatePysparkJobOperator(
        task_id='spark_raw_to_dst_nonpartitioned',
        main_python_file_uri=f's3a://{YC_BUCKET}/spark_scripts/extract_raw_to_dst_nonpartitioned_hdfs.py',
        cluster_id=CLUSTER_ID,
        connection_id='yandexcloud_default',
    )

    spark_postgres_to_raw >> spark_raw_to_dst_nonpartitioned