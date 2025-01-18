
from sqlalchemy import create_engine
import pyarrow.parquet as pq
import pandas as pd
from time import time 
import argparse
import os

def main(params):
    
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.database
    table_name = params.table_name
    url = params.url
    file_name = 'output.parquet'
    # Download the parquet file
    
    
    os.system(f'wget {url} -O {file_name}')
    
    
    
    pq.read_metadata(file_name)


    file = pq.ParquetFile(file_name)
    
    # table = file.read()
    # df = table.to_pandas()
    # df.info()

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()


    #print(pd.io.sql.get_schema(df, name=table_name, con=engine))


    batches_iterator = file.iter_batches(batch_size=100000)
    batches_iterator


    t_start = time()
    count = 1

    for batch in batches_iterator:
        
        batch_df = batch.to_pandas()
        print(f"Inserting Batch {count}")
        b_start = time()
        
        batch_df.to_sql(name=table_name, con=engine, if_exists='append')
        b_end = time()
        print(f'inserted! Time taken: {b_end - b_start:10.3f} seconds')
        count += 1
        
    t_end = time()
    print(f'Total Time taken: {t_end - t_start:10.3f} seconds')


if __name__ == '__main__':


    parser = argparse.ArgumentParser(description='Ingest data into Postgres')

    # user
    # password
    # host
    # port 
    # database name
    # table name
    # url of parquet file

    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--database', help='database name for postgres')
    parser.add_argument('--table_name', help='table name for postgres')
    parser.add_argument('--url', help='url of parquet file')   

    args = parser.parse_args()
    
    main(args)





