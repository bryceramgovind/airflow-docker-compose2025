from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from sodapy import Socrata
import pandas as pd
from google.cloud import storage


SOCRATA_DOMAIN = "data.cityofnewyork.us"
SOCRATA_DATASET_ID = "nc67-uf89" 
GCS_BUCKET_NAME = "buckey-mcbuckface"
GCS_FILE_NAME = "data/api_data.parquet"

# Define the DAG
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="api_to_gcs_taskflow",
    default_args=default_args,
    description="Fetch data from API and save as Parquet to GCS",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    @task
    def fetch_data_from_api():
        client = Socrata(SOCRATA_DOMAIN, None)  
        results = client.get(SOCRATA_DATASET_ID, limit=1000) 
        df = pd.DataFrame.from_records(results)
        return df.to_dict()

    @task
    def save_to_gcs(api_data: dict):
        df = pd.DataFrame.from_dict(api_data)

        local_file_path = "/tmp/api_data.parquet"
        df.to_parquet(local_file_path, engine="pyarrow", index=False)

        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(GCS_FILE_NAME)
        blob.upload_from_filename(local_file_path)
        print(f"Uploaded file to GCS: gs://{GCS_BUCKET_NAME}/{GCS_FILE_NAME}")

    api_data = fetch_data_from_api()
    save_to_gcs(api_data)
