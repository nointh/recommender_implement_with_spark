from asyncio import tasks
import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import requests
from sqlalchemy import create_engine
from pyspark.sql import SparkSession

PG_HOST = os.getenv("PG_HOST")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_PORT = os.getenv("PG_PORT")


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


URL_PREFIX = "https://files.grouplens.org/datasets/movielens/"
DATASET_NAME = 'ml-latest-small'
URL = URL_PREFIX + DATASET_NAME + '.zip'
OUTPUT_ZIP_FILE_DIR = AIRFLOW_HOME + DATASET_NAME + '.zip'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + DATASET_NAME
TABLE_NAME_TEMPLATE = "yellow_taxi_{{(execution_date.replace(day = 28) - macros.timedelta(days=90)).strftime(\'%Y_%m\')}}"

engine = create_engine('postgresql+psycopg2://postgres:noi123456@noing-db.c2qkku433l07.ap-southeast-1.rds.amazonaws.com:5432/postgres')

def convert_to_csv_callable(file_name):
    spark = SparkSession \
    .builder \
    .getOrCreate()
    df = spark.read.format("parquet").load(file_name)
    new_csv_file_name = file_name.replace('.parquet', '.csv')
    print(f'creating new csv file {new_csv_file_name}')
    df.write.format('csv').mode('overwrite').option("header","true").save(new_csv_file_name)

def get_movie_data(id):
    payload = {'userName': 'noing', 'password': 'noi06122001'}
    session = requests.Session()
    session.post('https://movielens.org/api/sessions', json=payload)
    imdb = session.get(f'https://movielens.org/api/movies/{id}')
    movie_data = imdb.json()['data']['movieDetails']['movie']
    return movie_data

def load_movie_data():
    movies_df = pd.read_csv(f'{OUTPUT_FILE_TEMPLATE}/movies.csv', sep=',')
    links_df = pd.read_csv(f'{OUTPUT_FILE_TEMPLATE}/links.csv', sep=',')
    fully_movie = movies_df.merge(links_df, on='movieId', suffixes=('_1', '_2'))
    fully_movie.drop('genres', axis=1, inplace=True)

    genre_df = pd.DataFrame(columns=['movieId', 'genre'])
    language_df = pd.DataFrame(columns=['movieId', 'language'])
    director_df = pd.DataFrame(columns=['movieId', 'director'])
    actor_df = pd.DataFrame(columns=['movieId', 'actor'])

    for i in fully_movie.index:
        movie_id = fully_movie.loc[i, 'movieId']
        movie_data = get_movie_data(movie_id)
        fully_movie.loc[i, 'plotSummary'] = movie_data['plotSummary']
        fully_movie.loc[i, 'releaseYear'] = movie_data['releaseYear']
        fully_movie.loc[i, 'posterPath'] = movie_data['posterPath']
        fully_movie.loc[i, 'releaseDate'] = movie_data['releaseDate']
        fully_movie.loc[i, 'title'] = movie_data['title']
        fully_movie.loc[i, 'mpaa'] = movie_data['mpaa']
        for genre in movie_data['genres']:
            genre_df.append({'movieId': movie_id, 'genre': genre}, ignore_index=True)
        for language in movie_data['languages']:
            language_df.append({'movieId': movie_id, 'language': language}, ignore_index=True)
        for director in movie_data['directors']:
            director_df.append({'movieId': movie_id, 'director': director}, ignore_index=True)
        for actor in movie_data['actors'][:5]:
            actor_df.append({'movieId': movie_id, 'actor': actor}, ignore_index=True)
    fully_movie.to_sql('movie', engine, if_exists='replace', index=False)
    genre_df.to_sql('genre', engine, if_exists='replace', index=False)
    language_df.to_sql('language', engine, if_exists='replace', index=False)
    director_df.to_sql('director', engine, if_exists='replace', index=False)
    actor_df.to_sql('actor', engine, if_exists='replace', index=False)

def load_rating_data():
    rating_df = pd.read_csv(f'{OUTPUT_FILE_TEMPLATE}/ratings.csv', sep=',')
    rating_df.to_sql('rating', engine, if_exists='replace', index=False)

def load_tag_data():
    tag_df = pd.read_csv(f'{OUTPUT_FILE_TEMPLATE}/tags.csv', sep=',')
    tag_df.to_sql('tag', engine, if_exists='replace', index=False)

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
        #bash_command=f"curl -sSL {URL} > {OUTPUT_FILE_TEMPLATE} && unzip {OUTPUT_FILE_TEMPLATE}"
        bash_command=f"(cd {AIRFLOW_HOME} && curl -O {URL} && unzip {DATASET_NAME}.zip"
    )
    # convert_to_csv = PythonOperator(
    #     task_id='convert_to_csv',
    #     python_callable=convert_to_csv_callable,
    #     op_kwargs={
    #         'file_name': OUTPUT_FILE_TEMPLATE
    #     }
    # )
    scrap_movies_and_ingest = PythonOperator(
        task_id='scrap_movie_and_ingest',
        python_callable=load_movie_data,
        # op_kwargs={
        #     'user': PG_USER,
        #     'password': PG_PASSWORD,
        #     "host": PG_HOST,
        #     "port": PG_PORT,
        #     "db": PG_DATABASE,
        #     "table_name": TABLE_NAME_TEMPLATE,
        #     "file_loc": OUTPUT_FILE_TEMPLATE

        # }
    )
    ingest_ratings = PythonOperator(
        task_id='ingest_rating_data',
        python_callable=load_rating_data,
    )
    ingest_tags = PythonOperator(
        task_id='ingest_tag_data',
        python_callable=load_tag_data,
    )


    download_task >> scrap_movies_and_ingest
    download_task >> ingest_ratings
    download_task >> ingest_tags