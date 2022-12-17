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
OUTPUT_ZIP_FILE_DIR = AIRFLOW_HOME + '/' + DATASET_NAME + '.zip'
OUTPUT_FOlDER= AIRFLOW_HOME + '/' + DATASET_NAME
TABLE_NAME_TEMPLATE = "yellow_taxi_{{(execution_date.replace(day = 28) - macros.timedelta(days=90)).strftime(\'%Y_%m\')}}"

engine = create_engine('postgresql+psycopg2://postgres:noi123456@noing-db.c2qkku433l07.ap-southeast-1.rds.amazonaws.com:5432/postgres')
payload = {'userName': 'noing', 'password': 'noi06122001'}
session = requests.Session()
session.post('https://movielens.org/api/sessions', json=payload)


def convert_to_csv_callable(file_name):
    spark = SparkSession \
    .builder \
    .getOrCreate()
    df = spark.read.format("parquet").load(file_name)
    new_csv_file_name = file_name.replace('.parquet', '.csv')
    print(f'creating new csv file {new_csv_file_name}')
    df.write.format('csv').mode('overwrite').option("header","true").save(new_csv_file_name)

def get_movie_data(id):
    imdb = session.get(f'https://movielens.org/api/movies/{id}')
    movie_data = imdb.json()['data']['movieDetails']['movie']
    return movie_data
def get_themoviedb_data(id):
    API_KEY = 'cfa9db117214a23524eabcd3a1517fc8'
    r = requests.get(f'https://api.themoviedb.org/3/movie/{id}?api_key={API_KEY}')
    movie_data = r.json()
    movie_data['plotSummary'] = movie_data.get('overview', '')
    movie_data['releaseDate'] = movie_data.get('release_date', '')
    movie_data['posterPath'] = movie_data.get('poster_path', '')
    movie_data['languages'] = movie_data.get('spoken_languages', '')
    movie_data['genres'] = map(lambda x: x.get('name', ''), movie_data.get('genres', []))
    movie_data['releaseYear'] = movie_data.get('release_date', '')[:4]

def load_and_scrap_movie_data():
    movies_df = pd.read_csv(f'{OUTPUT_FOlDER}/movies.csv', sep=',')
    links_df = pd.read_csv(f'{OUTPUT_FOlDER}/links.csv', sep=',')
    fully_movie = movies_df.merge(links_df, on='movieId', suffixes=('_1', '_2'))
    fully_movie.drop('genres', axis=1, inplace=True)

    genre_df = pd.DataFrame(columns=['movieId', 'genre'])
    language_df = pd.DataFrame(columns=['movieId', 'language'])
    director_df = pd.DataFrame(columns=['movieId', 'director'])
    actor_df = pd.DataFrame(columns=['movieId', 'actor'])

    for i in fully_movie.index:
        movie_id = fully_movie.loc[i, 'movieId']
        print(f'========Processing {movie_id}==========')
        try:
            movie_data = get_movie_data(movie_id)
        except Exception as e:
            movie_data = get_themoviedb_data(fully_movie.loc[i, 'tmdbId'])
        print(movie_data)
        fully_movie.loc[i, 'plotSummary'] = movie_data.get('plotSummary', '')
        fully_movie.loc[i, 'releaseYear'] = movie_data.get('releaseYear', '')
        fully_movie.loc[i, 'posterPath'] = movie_data.get('posterPath', '')
        fully_movie.loc[i, 'releaseDate'] = movie_data.get('releaseDate', '')
        fully_movie.loc[i, 'title'] = movie_data.get('title', '')
        fully_movie.loc[i, 'mpaa'] = movie_data.get('mpaa','')
        for genre in movie_data.get('genres', []):
            genre_df.append({'movieId': movie_id, 'genre': genre}, ignore_index=True)
        for language in movie_data.get('languages', []):
            language_df.append({'movieId': movie_id, 'language': language}, ignore_index=True)
        for director in movie_data.get('directors', []):
            director_df.append({'movieId': movie_id, 'director': director}, ignore_index=True)
        for actor in movie_data.get('actors', []):
            actor_df.append({'movieId': movie_id, 'actor': actor}, ignore_index=True)
    fully_movie.to_sql('movies', engine, if_exists='replace', index=False)
    genre_df.to_sql('genres', engine, if_exists='replace', index=False)
    language_df.to_sql('languages', engine, if_exists='replace', index=False)
    director_df.to_sql('directors', engine, if_exists='replace', index=False)
    actor_df.to_sql('actors', engine, if_exists='replace', index=False)

def generate_users_data():
    ratings_df = pd.read_csv(f'{OUTPUT_FOlDER}/ratings.csv', sep=',')
    tags_df = pd.read_csv(f'{OUTPUT_FOlDER}/tags.csv', sep=',')
    full_df = ratings_df.join(tags_df, rsuffix='_1', on='userId', how='outer')
    users_df = full_df[['userId']]
    users_df.drop_duplicates(inplace=True, keep='last')
    users_df.reset_index(drop=True, level=0, inplace=True)
    for i in users_df.index:
        id = users_df.loc[i, 'userId']
        users_df.loc[i, 'username'] = f'user_{id}'
        users_df.loc[i, 'password'] = f'123456'
    users_df.to_sql('users', engine, if_exists='replace', index=False)

def load_movie_data():
    movies_df = pd.read_csv(f'{OUTPUT_FOlDER}/movies.csv', sep=',')
    links_df = pd.read_csv(f'{OUTPUT_FOlDER}/links.csv', sep=',')
    fully_movie = movies_df.merge(links_df, on='movieId', suffixes=('_1', '_2'))
    genre_df = pd.DataFrame(columns=['movieId', 'genre'])
    for i in fully_movie.index:
        movie_id = fully_movie.loc[i, 'movieId']
        genres = fully_movie.loc[i, 'genres'].split('|')
        for genre in genres:
            genre_df.append({'movieId': movie_id, 'genre': genre}, ignore_index=True)
    fully_movie.drop('genres', axis=1, inplace=True)
    fully_movie.to_sql('movies', engine, if_exists='replace', index=False)

def load_rating_data():
    ratings_df = pd.read_csv(f'{OUTPUT_FOlDER}/ratings.csv', sep=',')
    ratings_df['timestamp'] = pd.to_datetime(ratings_df['timestamp'], unit='s')
    ratings_df.to_sql('ratings', engine, if_exists='replace', index=False)

def load_tag_data():
    tags_df = pd.read_csv(f'{OUTPUT_FOlDER}/tags.csv', sep=',')
    tags_df['timestamp'] = pd.to_datetime(tags_df['timestamp'], unit='s')
    tags_df.to_sql('tags', engine, if_exists='replace', index=False)

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

local_workflow = DAG(
    dag_id='data_ingestion_to_database',
    schedule_interval='0 6 2 * *',
    default_args=default_args
)
with local_workflow:
    check_if_exist_zip_file = BashOperator(
        task_id='check_if_exist_zip_file',
        #bash_command=f"curl -sSL {URL} > {OUTPUT_FILE_TEMPLATE} && unzip {OUTPUT_FILE_TEMPLATE}"
        bash_command=f'if [ -f "{OUTPUT_ZIP_FILE_DIR}" ]; then\
                rm {OUTPUT_ZIP_FILE_DIR};\
            fi'
    )
    check_if_exist_folder = BashOperator(
        task_id='check_if_exist_folder',
        #bash_command=f"curl -sSL {URL} > {OUTPUT_FILE_TEMPLATE} && unzip {OUTPUT_FILE_TEMPLATE}"
        bash_command=f'if [ -d "{OUTPUT_FOlDER}" ]; then\
                rm -r {OUTPUT_FOlDER};\
            fi'
    )
    download_task = BashOperator(
        task_id='download',
        #bash_command=f"curl -sSL {URL} > {OUTPUT_FILE_TEMPLATE} && unzip {OUTPUT_FILE_TEMPLATE}"
        bash_command=f'(cd {AIRFLOW_HOME} && curl -O {URL} && unzip {DATASET_NAME}.zip)'
    )
    # convert_to_csv = PythonOperator(
    #     task_id='convert_to_csv',
    #     python_callable=convert_to_csv_callable,
    #     op_kwargs={
    #         'file_name': OUTPUT_FILE_TEMPLATE
    #     }
    # )
    ingest_and_scrap_movie_data = PythonOperator(
        task_id='ingest_and_scrap_movie',
        python_callable=load_and_scrap_movie_data,
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
    ingest_and_generate_users = PythonOperator(
        task_id='ingest_and_generate_users',
        python_callable=generate_users_data,
    )

    check_if_exist_zip_file >> download_task
    check_if_exist_folder >> download_task
    download_task >> ingest_and_scrap_movie_data
    download_task >> ingest_and_generate_users
    download_task >> ingest_ratings
    download_task >> ingest_tags