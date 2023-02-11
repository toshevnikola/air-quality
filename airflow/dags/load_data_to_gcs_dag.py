import calendar
import os
import logging
import time
from datetime import datetime, timedelta, date

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from airflow import DAG
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import get_current_context

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)
import pyarrow.csv as pv
import pyarrow.parquet as pq


###FUNCTIONS


def get_previous_month_dt(dt: datetime):
    return dt - relativedelta(months=1)


def get_datetime_month_and_year(dt: datetime):
    return dt.year, dt.month


def format_year_month(year: int, month: int):
    return f"{year}-{month}"


def formatt(dt: datetime):
    dt = get_previous_month_dt(dt)
    year, month = get_datetime_month_and_year(dt)
    return format_year_month(year, month)


def download_json_data_task(
    station: str,
    parameter: str,
    bucket_name: str,
):
    context = get_current_context()
    execution_date = context["execution_date"]
    prev_month_dt = get_previous_month_dt(execution_date)
    _, prev_month_last_day = calendar.monthrange(
        prev_month_dt.year, prev_month_dt.month
    )
    begin_date = prev_month_dt.replace(day=1).strftime("%Y-%m-%d")
    end_date = prev_month_dt.replace(day=prev_month_last_day).strftime("%Y-%m-%d")

    response = requests.get(
        BASE_URL.format(
            station=station,
            parameter=parameter,
            begin_date=begin_date,
            begin_time="00:00",
            end_date=end_date,
            end_time="23:00",
        ),
        headers={"Content-Type": "application/json"},
    )
    response_json = response.json()
    response.raise_for_status()
    measurements_dict = {
        "value": response_json["measurements"][0]["data"],
        "times": response_json["times"],
    }
    df = pd.DataFrame(measurements_dict)
    df["station"] = station
    df["parameter"] = parameter
    df = df.astype({"value": float, "times": str, "parameter": str, "station": str})
    folder_name = f"{prev_month_dt.year}-{prev_month_dt.month}"
    file_name = f"raw/{folder_name}/{station}-{parameter}.parquet"

    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    bucket.blob(file_name).upload_from_string(df.to_parquet(), content_type="text")
    time.sleep(5)


########
import itertools

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

dataset_file = "yellow_tripdata_2021-01.csv"
BASE_URL = "https://air.moepp.gov.mk/graphs/site/pages/MakeGraph.php?station={station}&parameter={parameter}&beginDate={begin_date}&beginTime={begin_time}&endDate={end_date}&endTime={end_time}&lang=en"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "air_quality")
STATIONS = [
    "Centar",
    "Karpos",
    "Lisice",
    "GaziBaba",
    "Rektorat",
    "Miladinovci",
    "MobileGP",
    "Ohrid",
    "Strumica",
]
PARAMETERS = ["PM10", "PM25", "O3", "SO2", "CO", "NO2"]


default_args = {
    "owner": "airflow",
    # "start_date": days_ago(1),
    "depends_on_past": True,
    "retries": 3,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_dag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(year=2022, month=5, day=1),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=["dtc-de"],
    user_defined_macros={
        "formatt": formatt,
    },
) as dag:
    download_all_data_tasks = []
    for station, parameter in itertools.product(STATIONS, PARAMETERS):
        download_dataset_task = PythonOperator(
            task_id=f"download_json_data_task_station_{station}_parameter_{parameter}",
            pool="test_pool",
            python_callable=download_json_data_task,
            provide_context=True,
            op_kwargs={
                "station": station,
                "parameter": parameter,
                "bucket_name": BUCKET,
            },
        )
        download_all_data_tasks.append(download_dataset_task)

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        pool="test_pool",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "air_quality",
                "schema": [
                    {"name": "value", "type": "FLOAT", "mode": "REQUIRED"},
                    {"name": "times", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "station", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "parameter", "type": "STRING", "mode": "REQUIRED"},
                ],
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [
                    "gs://dtc_data_lake_festive-dolphin-366719/raw/{{ formatt(execution_date) }}/*"
                ],
            },
        },
    )

    download_all_data_tasks >> bigquery_external_table_task
