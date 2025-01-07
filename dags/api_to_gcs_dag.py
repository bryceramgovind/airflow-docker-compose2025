from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta  # Added timedelta import
from sodapy import Socrata
import pandas as pd
from google.cloud import storage

# Configuration
SOCRATA_DOMAIN = "data.cityofnewyork.us"
SOCRATA_DATASET_ID = "nc67-uf89"  # Example dataset (NYC 311 Service Requests)
GCS_BUCKET_NAME = "buckey-mcbuckface"
GCS_FILE_NAME = "data/api_data.parquet"

# Function to download data from the API
def fetch_data_from_api(**kwargs):
    client = Socrata(SOCRATA_DOMAIN, None)  # Public dataset, no token needed
    results = client.get(SOCRATA_DATASET_ID, limit=1000)  # Fetch 1000 rows
    df = pd.DataFrame.from_records(results)
    kwargs['ti'].xcom_push(key='api_data', value=df.to_dict())

# Function to save DataFrame as Parquet to GCS
def save_to_gcs(**kwargs):
    ti = kwargs['ti']
    api_data = ti.xcom_pull(task_ids='fetch_api_data', key='api_data')
    df = pd.DataFrame.from_dict(api_data)

    # Save DataFrame as Parquet
    local_file_path = "/tmp/api_data.parquet"
    df.to_parquet(local_file_path, engine="pyarrow", index=False)

    # Upload to GCS
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(GCS_FILE_NAME)
    blob.upload_from_filename(local_file_path)
    print(f"Uploaded file to GCS: gs://{GCS_BUCKET_NAME}/{GCS_FILE_NAME}")

# Define the DAG
with DAG(
    dag_id="api_to_gcs",
    default_args={"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)},
    description="Fetch data from API and save as Parquet to GCS",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    fetch_api_data = PythonOperator(
        task_id="fetch_api_data",
        python_callable=fetch_data_from_api,
    )

    save_parquet_to_gcs = PythonOperator(
        task_id="save_parquet_to_gcs",
        python_callable=save_to_gcs,
    )

    fetch_api_data >> save_parquet_to_gcs
