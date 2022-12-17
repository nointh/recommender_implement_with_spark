from asyncio import tasks
import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from ingest_data import ingest_callable
from pyspark.sql import SparkSession

PG_HOST = os.getenv("PG_HOST")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_PORT = os.getenv("PG_PORT")


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
URL_TEMPLATE = URL_PREFIX + "yellow_tripdata_{{(execution_date.replace(day = 28) - macros.timedelta(days=90)).strftime(\'%Y-%m\')}}.parquet"
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + "/output_{{(execution_date.replace(day = 28) - macros.timedelta(days=90)).strftime(\'%Y-%m\')}}.parquet"
TABLE_NAME_TEMPLATE = "yellow_taxi_{{(execution_date.replace(day = 28) - macros.timedelta(days=90)).strftime(\'%Y_%m\')}}"

def convert_to_csv_callable(file_name):
    spark = SparkSession \
    .builder \
    .getOrCreate()
    df = spark.read.format("parquet").load(file_name)
    new_csv_file_name = file_name.replace('.parquet', '.csv')
    print(f'creating new csv file {new_csv_file_name}')
    df.write.format('csv').mode('overwrite').option("header","true").save(new_csv_file_name)



default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

local_workflow = DAG(
    dag_id='data_ingestion_local',
    schedule_interval='0 6 2 * *',
    default_args=default_args
)
with local_workflow:
    download_task = BashOperator(
        task_id='download',
        bash_command=f"curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}"
    )
    # convert_to_csv = PythonOperator(
    #     task_id='convert_to_csv',
    #     python_callable=convert_to_csv_callable,
    #     op_kwargs={
    #         'file_name': OUTPUT_FILE_TEMPLATE
    #     }
    # )
    ingest_task = PythonOperator(
        task_id='ingest_data',
        python_callable=ingest_callable,
        op_kwargs={
            'user': PG_USER,
            'password': PG_PASSWORD,
            "host": PG_HOST,
            "port": PG_PORT,
            "db": PG_DATABASE,
            "table_name": TABLE_NAME_TEMPLATE,
            "file_loc": OUTPUT_FILE_TEMPLATE

        }
    )
    download_task >> ingest_task