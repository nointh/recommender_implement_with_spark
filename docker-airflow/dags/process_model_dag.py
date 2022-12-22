import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
import pandas as pd
import io
import json

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
CLUSTER_NAME = os.environ.get("GCP_CLUSTER_NAME", 'hadoopcluster')
PYSPARK_URI = 'gs://movie_recommenders/pyspark_jobs/mf.py'
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')
EXECUTION_TIME = "{{ data_interval_start | ts_nodash }}"

PYSPARK_JOB = {
    'reference': { 'project_id': PROJECT_ID},
    'placement': {'cluster_name': CLUSTER_NAME},
    'pyspark_job': {'main_python_file_uri': PYSPARK_URI, 
        'args': [f"--input=gs://{BUCKET}/{EXECUTION_TIME}/processed/ratings.csv",
        f"--output=gs://{BUCKET}/{EXECUTION_TIME}/model/"
        ]
        }
}

engine = create_engine('postgresql+psycopg2://postgres:noi123456@noing-db.c2qkku433l07.ap-southeast-1.rds.amazonaws.com:5432/postgres')

# def format_to_parquet(src_file):
#     if not src_file.endswith('.csv'):
#         logging.error("Can only accept source files in CSV format, for the moment")
#         return
#     table = pv.read_csv(src_file)
#     pq.write_table(table, src_file.replace('.csv', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround
    with engine.connect() as conn:
        result = conn.execute(text('select "userId", "movieId", "rating" from ratings'))
        ratings = result.all()
    df = pd.DataFrame(ratings, columns = ['userId', 'movieId', 'rating'])
    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_string(df.to_csv(index=False), 'text/csv')

def preprocess_data(bucket, raw_input_object, output_folder):
    client = storage.Client()
    print(f'Pulled down file from bucket {bucket}, file name: {raw_input_object}')
    bucket = client.bucket(bucket)
    input_blob = bucket.blob(raw_input_object)
    data = input_blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(data))
    
    user_df = df[['userId']]
    user_df = user_df.drop_duplicates(keep='last')
    user_df = user_df.sort_values(by=['userId'])
    user_df.reset_index(drop=True, level=0, inplace=True)
    user_dict = dict(zip(user_df['userId'].values ,list(user_df.index+1)))


    movie_df = df[['movieId']]
    movie_df = movie_df.drop_duplicates(keep='last')
    movie_df = movie_df.sort_values(by=['movieId'])
    movie_df.reset_index(drop=True, level=0, inplace=True)
    movie_dict = dict(zip(movie_df['movieId'].values ,list(movie_df.index+1)))

    df['movieId'] = df['movieId'].map(lambda x: movie_dict[x])
    df['userId'] = df['userId'].map(lambda x: user_dict[x])

    output_blob = bucket.blob(output_folder + 'ratings.csv')
    output_blob.upload_from_string(df.to_csv(index=False), 'text/csv')

    movie_dict = {int(key): int(val) for key, val in movie_dict.items()}
    user_dict = {int(key): int(val) for key, val in user_dict.items()}
    
    user_dict_blob = bucket.blob(output_folder + 'user_dict.json')
    user_dict_blob.upload_from_string(json.dumps(user_dict), 'application/json')

    movie_dict_blob = bucket.blob(output_folder + 'movie_dict.json')
    movie_dict_blob.upload_from_string(json.dumps(movie_dict), 'application/json')


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="process_model_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc_data_lake_taxi-data-de-project'],
) as dag:

    # download_dataset_task = BashOperator(
    #     task_id="download_dataset_task",
    #     bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}"
    # )

    # format_to_parquet_task = PythonOperator(
    #     task_id="format_to_parquet_task",
    #     python_callable=format_to_parquet,
    #     op_kwargs={
    #         "src_file": f"{path_to_local_home}/{dataset_file}",
    #     },
    # )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    retrieve_data_task = PythonOperator(
        task_id="retrieve_data_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"{EXECUTION_TIME}/raw/ratings.csv"
        },
    )
    preprocess_data_task = PythonOperator(
        task_id="preprocess_data_task",
        python_callable=preprocess_data,
        op_kwargs={
            "bucket": BUCKET,
            "raw_input_object": f"{EXECUTION_TIME}/raw/ratings.csv",
            "output_folder": f"{EXECUTION_TIME}/processed/"
        },
    )

    submit_job_task = DataprocSubmitJobOperator(
        task_id='submit_job_task',
        job=PYSPARK_JOB,
        region='us-central1',
        project_id=PROJECT_ID
    )
    # bigquery_external_table_task = BigQueryCreateExternalTableOperator(
    #     task_id="bigquery_external_table_task",
    #     table_resource={
    #         "tableReference": {
    #             "projectId": PROJECT_ID,
    #             "datasetId": BIGQUERY_DATASET,
    #             "tableId": "external_table",
    #         },
    #         "externalDataConfiguration": {
    #             "sourceFormat": "PARQUET",
    #             "sourceUris": [f"gs://{BUCKET}/raw/{dataset_file}"],
    #         },
    #     },
    # )

    retrieve_data_task >> preprocess_data_task >> submit_job_task
    # download_dataset_task  >> local_to_gcs_task >> bigquery_external_table_task
