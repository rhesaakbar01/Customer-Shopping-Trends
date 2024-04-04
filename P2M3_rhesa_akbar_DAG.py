'''
=================================================
Milestone 3

Name  : Rhesa Akbar Elvarettano
Batch : FTDS-SBY-003

This program aims to automate load data from PostgreSQL, data cleaning, and upload data to ElasticSearch. 
The data used in this program is the Customer Shopping Trends Dataset.
=================================================
'''

import datetime as dt
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch

# Connect to Postgres
from sqlalchemy import create_engine

def fetch_data_from_postgres(database, username, password):
    '''
        This function is used to fetch the data from local postgres database

        Params:
            - database (str): the name of the local database
            - username (str): the name of username used to access the local database
            - password (str): the password used to access the local database

        Return: -

        Example of use: fetch_data_from_postgres('database', 'username', 'password')
    '''
    # fetch data from postgres
    database = "airflow" # database name created in PostgreS that connected to PostgreS docker image synced via .env
    username = "airflow" # username/role created through .venv to access the PostgreS database
    password = "airflow" # password for username/role created through .venv to access the PostgreS database
    host = "localhost" # platform used

    # Membuat URL koneksi PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}" # connect to PostgreS query tool using this address

    # establish SQLAlchemy connection to PostgreS image in docker
    engine = create_engine(postgres_url)
    conn = engine.connect()
    
    # run SQL query of SELECT * FROM table_m3 in database=milestone_3, to PostgreS
    df = pd.read_sql_query("select * from table_m3", conn) 
    df.to_csv('/opt/airflow/dags/P2M3_rhesa_akbar_raw.csv', sep=',', index=False) # save the query result to a csv

def data_cleaning():
    '''
        This function is used to pre-processing the fetched data by cleaning it

        Params: -

        Return: -
        
        Example of use: data_cleaning()
    '''
    df_raw = pd.read_csv('/opt/airflow/dags/P2M3_rhesa_akbar_raw.csv')
    df = df_raw.copy() # Membuat salinan dataframe df_raw yang baru dengan nama df. Ini dilakukan untuk menghindari memodifikasi dataframe asli (df_raw) dan untuk memastikan bahwa semua transformasi dilakukan pada salinan

    columns = df.columns # Mengambil daftar nama kolom dari dataframe df
    new_col_names = {} # Membuat kamus kosong yang akan digunakan untuk memetakan nama kolom lama ke nama kolom baru setelah pembersihan

    for col in columns:
        col_remove_char = col.replace('(', '').replace(')', '') # Menghapus tanda kurung dari nama kolom jika ada

        col_split = col_remove_char.split(' ')
        for i in range(len(col_split)):
            word_lower = col_split[i].lower() # Setiap kata diubah menjadi huruf kecil
            col_split[i] = word_lower
        new_col_name = '_'.join(col_split) # Setelah semua kata diubah menjadi huruf kecil, kata-kata tersebut digabungkan kembali menjadi satu string dengan menggunakan _ sebagai pemisah

        new_col_names[col] = new_col_name

    df = df.rename(columns = new_col_names)

    # Dropping Missing Values
    df.dropna(inplace=True) # Menghapus baris yang mengandung nilai yang hilang (NaN) 

    # Dropping Duplicates
    df.drop_duplicates(inplace=True) # Menghapus baris yang merupakan duplikat

    # Saving to new csv
    df.to_csv('/opt/airflow/dags/P2M3_rhesa_akbar_clean.csv', index=False) # Menyimpan dataframe df yang telah dimodifikasi ke dalam file CSV baru 


def upload_to_elasticsearch():
    '''
        This function is used to upload the cleaned data to elasticsearch for visualization

        Params: -

        Return: -
        
        Example of use: upload_to_elasticsearch()
    '''
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/dags/P2M3_rhesa_akbar_clean.csv')
    
    for i, r in df.iterrows():
        doc = r.to_dict()  # Convert the row to a dictionary
        res = es.index(index="table_m3", id=i+1, body=doc)
        print(f"Response from Elasticsearch: {res}")
    
 
default_args = {
    'owner': 'Rhesa', 
    'start_date': datetime(2024, 3, 22, 14,10,0) - timedelta(hours=7), 
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG(
    "P2M3_rhesa_akbar_DAG", # the DAG name
    description='Milestone 3', # the DAG description
    schedule_interval='30 6 * * *', # mengatur waktu interval pembaruan yang secara otomatis akan menjalankan ulang DAG setiap pukul 6.30 (menggunakan format Cronguru)
    default_args=default_args, # insert the defined default_args
    catchup=False # when default_arg's start date is set in the past, it will not immediately run unless is triggered manually
) as dag:
    # Task 1 - Fetch data from postgres
    fetch_data_task = PythonOperator(
        task_id = 'fetch_data_from_postgres',
        python_callable = fetch_data_from_postgres) # run `fetch_data_from_postgres` function using python interpretor function

    # Task 2 - Data cleaning
    data_cleaning_task = PythonOperator(
        task_id = 'data_cleaning',
        python_callable = data_cleaning
    )

    # Task 3 - Upload data to elasticsearch
    upload_data_task = PythonOperator(
        task_id = 'upload_data_to_elasticsearch',
        python_callable = upload_to_elasticsearch
    )

    # Tasks pipeline
    fetch_data_task >> data_cleaning_task >> upload_data_task