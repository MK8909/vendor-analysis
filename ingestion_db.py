import pandas as pd
import logging 
import os
from sqlalchemy import create_engine
import time

logging.basicConfig(
    filename="logs/ingestions_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

engine = create_engine('sqlite:///vendors.db')

def ingest_db(df, table_name, engine):
    '''this function will ingest the dataframe into database table'''
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

def load_raw_data():
    '''this function will load the CSVs as dataframe and ingest into db'''
    start = time.time()
    for file in os.listdir('extracted_files/data'):
        if '.csv' in file:
            df = pd.read_csv('extracted_files/data/'+file)
            logging.info(f'Ingesting {file} in db')
            ingest_db(df, file[:-4], engine)

    end = time.time()
    total_time = (end - start)/60
    logging.info('----------Ingestion Complete----------')

    logging.info(f'\nTotal Time Taken: {total_time} minutes')

if __name__ == '__main__':
    load_raw_data()