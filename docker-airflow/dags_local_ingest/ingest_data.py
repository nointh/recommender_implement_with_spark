import pandas as pd
from time import time
from sqlalchemy import create_engine
import pyarrow.parquet as pq
import os

def ingest_callable(user, password, host, port, db, table_name, file_loc):
    # user = params.user
    # password = params.password
    # host = params.host
    # port = params.port
    # db = params.db
    # table_name = params.table_name
    # url = params.url
    print(user, password, host, port, db, table_name, file_loc)

    print(f'user is {str(user)}')
    df = pq.read_table(source=file_loc).to_pandas()
    csv_file = file_loc.replace('.parquet', '.csv')
    df.to_csv(csv_file, index=False)
    print(f'creating new csv file {csv_file}')
    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100000)

    df = next(df_iter)

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'] )
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'] )

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    print('Creating new connection')
    engine.connect()
    print('Connection successfully')
    df.to_sql(name=table_name, con=engine, if_exists='replace')
    try:
        while True:
            df = next(df_iter)
            t_start = time()
            df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'] )
            df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'] )
            df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
            t_end = time()
            print('Insert another chunk, took %.3f seconds' % (t_end-t_start))
    except StopIteration:
        print('Completed')
