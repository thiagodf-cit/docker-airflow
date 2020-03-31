import pandas as pd
import codecs
import logging
import csv
import os
import io

from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator

TRADE_DB_ID = "trade_etanol"
TRADE_TABLE = "etanol"
LOCAL_PATH = "/usr/local/airflow/files/"
FILE_NAME = "trade_etanol"
FILE_EXT = ".csv"
five_days_ago = datetime.combine(datetime.today() - timedelta(5), datetime.min.time())

default_args = {
    'owner': 'thiagodf',
    'depends_on_past': False,
    'start_date': five_days_ago,
    'email': ['thiagodf@ciandt.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'get_data_in_csv',
    default_args=default_args,
    description='A DAG test, import csv file to sql file and add in db',
    schedule_interval='@once',
    tags=['get_data_in_csv'],
)

def load_file(local_path, file_name, ext, **kwargs):
    # print("Load File: \n", file_name + ext)
    file_path = local_path + file_name + ext
    df = pd.read_csv(file_path,
            sep=';',
            decimal='.',
            encoding='utf-8',
            parse_dates=['date_trade'],
            header=None,
            names=['date_trade','value_per_liter_brl','value_per_liter_usd','weekly_variation'])
    # print("Load File With Coluns names: \n", df)
    df.to_csv(file_path, sep=',', header=True)
    file_path = pd.read_csv(local_path + file_name + ext)
    # print("Load File New Format: \n", file_path)
    return file_path
    

def execute_file():
    file_path = LOCAL_PATH + FILE_NAME + FILE_EXT
    print("Execute File: \n", file_path)
    
def show_data_trade():
    file_path = LOCAL_PATH + FILE_NAME + FILE_EXT
    print("Execute File: \n", file_path)
    
    

#[Start_Task]
# Test => docker-compose -f docker-compose.yml run --rm webserver airflow test get_data_in_csv load_file 2020-03-29
load_file = PythonOperator(
    task_id='load_file',
    python_callable=load_file,
    provide_context=True,
    op_kwargs={
                'local_path': LOCAL_PATH,
                'file_name': FILE_NAME,
                'ext': FILE_EXT,
            },
    dag=dag
)

# Test => docker-compose -f docker-compose.yml run --rm webserver airflow test get_data_in_csv execute_file 2020-03-29
execute_file = MsSqlOperator(
    task_id='execute_file',
    mssql_conn_id=TRADE_DB_ID,
    sql="""
    SELECT * FROM [etanol]
    """,
    dag=dag
)

# Test => docker-compose -f docker-compose.yml run --rm webserver airflow test get_data_in_csv show_data_trade 2020-03-29
show_data_trade = MsSqlOperator(
    task_id='show_data_trade',
    mssql_conn_id=TRADE_DB_ID,
    sql="""
    SELECT * FROM [etanol]
    """,
    dag=dag
)
