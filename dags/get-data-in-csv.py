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

TRADE_CONN_ID = "trade_etanol"
TRADE_DATASET = "etanol"
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
    schedule_interval=timedelta(days=1),
    tags=['get_data_in_csv'],
)

def load_file(local_path, file_name, ext):
    print("Load File: ", file_name)
    df = pd.read_csv(local_path + local_path + ext,
            index_col='date_trade',
            parse_dates=['date_trade'],
            header=0,
            names=['value_per_liter_brl','date_trade','value_per_liter_usd','weekly_variation'])
    print(df) 

#[Start_Load]
# Test => docker-compose -f docker-compose.yml run --rm webserver airflow test get_data_in_csv start_load_file 2020-03-29
start_load_file = PythonOperator(
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
