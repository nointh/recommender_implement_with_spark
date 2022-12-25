import os
import logging
import numpy as np
import difflib

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sqlalchemy import create_engine, text
import pandas as pd
import io
import json

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
CLUSTER_NAME = os.environ.get("GCP_CLUSTER_NAME", 'hadoopcluster')
PYSPARK_URI = 'gs://movie_recommenders/pyspark_jobs/mf.py'
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'recommenders')
EXECUTION_TIME = "{{ dag_run.logical_date | ts_nodash }}"
GOOGLE_APPLICATION_CREDENTIALS  = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", '/.google/credentials/google_credentials.json')
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


def load_factor_matrix_to_json(input, folder, bucket):
    spark = SparkSession\
        .builder\
        .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
        .getOrCreate()
    spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
    # This is required if you are using service account and set true, 
    spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
    spark._jsc.hadoopConfiguration().set('google.cloud.auth.service.account.json.keyfile', f"{GOOGLE_APPLICATION_CREDENTIALS}")
    client = storage.Client()
    bucket = client.bucket(bucket)
    # user_factor_matrix = sc.wholeTextFiles(input).values().map(json.loads)
    u_mapper_blob = bucket.blob(f'{folder}processed/user_dict.json')
    u_mapper_r = json.loads(u_mapper_blob.download_as_string())
    u_mapper = {val: key for key, val in u_mapper_r.items()}
    # print(u_mapper)
    user_df = spark.read.json(f'{input}user_matrix/*')
    # user_df.printSchema()
    # user_df.show()
    user_rdd = user_df.select(col('user'), col('array')).rdd
    # print(user_rdd)
    # print(user_rdd.collect())
    user_factor_matrix = {u_mapper.get(row['user'], 0): row['array'] for row in user_rdd.collect()}
    # print(user_factor_matrix)

    u_matrix_blob = bucket.blob(f'{folder}matrix/user_matrix.json')
    u_matrix_blob.upload_from_string(json.dumps(user_factor_matrix), 'application/json')

    ## Movie save factor matrix
    m_mapper_blob = bucket.blob(f'{folder}processed/movie_dict.json')
    m_mapper_r = json.loads(m_mapper_blob.download_as_string())
    m_mapper = {val: key for key, val in m_mapper_r.items()}
    # print(m_mapper)
    movie_df = spark.read.json(f'{input}movie_matrix/*')
    movie_df.printSchema()
    movie_df.show()
    movie_rdd = movie_df.select(col('movie'), col('array')).rdd
    # print(movie_rdd)
    # print(movie_rdd.collect())
    movie_factor_matrix = {m_mapper.get(row['movie'], 0): row['array'] for row in movie_rdd.collect()}
    # print(movie_factor_matrix)

    m_matrix_blob = bucket.blob(f'{folder}matrix/movie_matrix.json')
    m_matrix_blob.upload_from_string(json.dumps(movie_factor_matrix), 'application/json')
    
    final_m_matrix_blob = bucket.blob(f'models/matrix_factorization/movie_matrix.json')
    final_m_matrix_blob.upload_from_string(json.dumps(movie_factor_matrix), 'application/json')

    final_u_matrix_blob = bucket.blob(f'models/matrix_factorization/user_matrix.json')
    final_u_matrix_blob.upload_from_string(json.dumps(user_factor_matrix), 'application/json')

    #Calculate full matrix
    full_predict_matrix = []
    for user, user_arr in user_factor_matrix.items():
        for movie, movie_arr in movie_factor_matrix.items():
            pred = np.array(user_arr).dot(np.array(movie_arr).T)
            full_predict_matrix.append((user, movie, pred))

    recommender_blob = bucket.blob('recommenders/matrix_factorization.csv')
    df = pd.DataFrame(full_predict_matrix, columns = ['userId', 'movieId', 'predict'])
    recommender_blob.upload_from_string(df.to_csv(index=False), 'text/csv')

def process_content_based_recommender():
    with engine.connect() as conn:
        result = conn.execute(text('select * from movies'))
        movies = result.all()
    movies_data = pd.DataFrame(movies)
    genres = movies_data['genres'].str.split('|')
    for i in range(len(genres)):
        genres[i] = ' '.join(map(str, genres[i]))
    movies_data['genres'] = genres
    selected_features = ['genres','mpaa']
    for feature in selected_features:
        movies_data[feature] = movies_data[feature].fillna('')
    combined_features = movies_data['genres']+' '+movies_data['mpaa']
    vectorizer = TfidfVectorizer()
    feature_vectors = vectorizer.fit_transform(combined_features)
    similarity = cosine_similarity(feature_vectors)
    
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="process_content_based_model_dag",
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
        task_id='submit_matrix_factorization_job_task',
        job=PYSPARK_JOB,
        region='us-central1',
        project_id=PROJECT_ID
    )

    load_matrix_as_json_task = PythonOperator(
        task_id="load_matrix_to_gcs",
        python_callable=load_factor_matrix_to_json,
        op_kwargs={
            "bucket": BUCKET,
            "input": f"gs://{BUCKET}/{EXECUTION_TIME}/model/",
            "folder": f"{EXECUTION_TIME}/"
        },
    )
    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="create_mf_bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "mf_recommender",
            },
            "externalDataConfiguration": {
                'autodetect': 'True',
                "sourceFormat": "CSV",
                "sourceUris": [f"gs://{BUCKET}/recommenders/matrix_factorization.csv"],
            },
        },
    )


    # gcs_2_bq_ext = BigQueryCreateExternalTableOperator(
    #     task_id=f'bq_user_factor_matrix_external_table_task',
    #     table_resource={
    #         'tableReference': {
    #             'projectId': PROJECT_ID,
    #             'datasetId': BIGQUERY_DATASET,
    #             'tableId': f'user_factor',
    #         },
    #         'externalDataConfiguration':{
    #             'autodetech': 'True',
    #             'sourceFormat': 'NEWLINE_DELIMITED_JSON',
    #             'sourceUris': [f'gs://{BUCKET}/{EXECUTION_TIME}/model/user_matrix/*.json']
    #         }
    #     }
    # )

    retrieve_data_task >> preprocess_data_task >> submit_job_task >> load_matrix_as_json_task >> bigquery_external_table_task
    # download_dataset_task  >> local_to_gcs_task >> bigquery_external_table_task
