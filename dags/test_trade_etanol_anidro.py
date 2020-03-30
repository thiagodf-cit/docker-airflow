import logging
import os
import csv
import codecs
from datetime import datetime, timedelta
import pandas as pd

# [START import_module]
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.utils.dates import days_ago
# [END import_module]

# [START my_variables]
MY_CONN_ID = "my_mssql_conn"
MY_DATASET = "etanol_anidro"
LOCAL_PATH = "/usr/local/airflow/files/"
FILE_CSV = "trade-etanol.csv"

five_days_ago = datetime.combine(datetime.today() - timedelta(5), datetime.min.time())
schedule_interval = "@daily"
# [END my_variables]

# [START default_args]
default_args = {
    'owner': 'thiagodf',
    'depends_on_past': False,
    'start_date': five_days_ago,
    'email': ['thiagodf@ciandt.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}
# [END default_args]

# [START instantiate_dag]
dag = DAG(
    'test_trade_etanol_anidro',
    default_args=default_args,
    description='A DAG test with import csv + mssql db',
    schedule_interval=schedule_interval,
    tags=['test_trade_etanol_anidro'],
)
# [END instantiate_dag]

# [START definations]
def execute_csv_trade_etanol(local_path, file_csv, header=None, sep=',', decimal='.', **kwargs):
    local_filepath = kwargs['ti'].xcom_pull(local_path + file_csv)
    # logging.info('execute file: {}', format(local_filepath))
    def unpack_col(col_to_unpack, df_to_append = None, header = 'col', sep=';', na_value=''):
        unpacked_cols = col_to_unpack.fillna(na_value).apply(lambda x: pd.Series(x.splint(';'))).fillna(na_value)
        col_names = []
        for i in unpacked_cols.columns:
            col_names.append(header + '_' + str(i))
        unpacked_cols.columns = col_names
        if isinstance(df_to_append, pd.DataFrame):
            return pd.concat([df_to_append, unpacked_cols], axis=1)
        else:
            return unpacked_cols
    local_filepath = kwargs['ti'].xcom_pull(local_path + file_csv)
    df = pd.read_csv(local_filepath, sep = '|', header = header, decimal = '.', parse_dates=True, encoding='utf-8')
    df = unpack_col(df[1], df, header='date')
    df = unpack_col(df[2], df, header='value_per_liter_brl')
    df = unpack_col(df[3], df, header='value_per_liter_usd')
    df = unpack_col(df[4], df, header='weekly_variation')
    df.to_csv(local_filepath, sep='\t', encoding='utf-8')
    
    return local_filepath
    
def insert_trade_etanol_to_db(local_filepath):
    table_name = MY_DATASET
    conn = MySqlHook(conn_name_attr=MY_CONN_ID)
    conn.bulk_load(table_name, local_filepath)
        
def check_trade_etanol_db():
    check_trade_etanol_db = """
        SELECT * FROM master.trade_etanol
        """
        
# [END definations]

# [START Tasks]
# Task I
Task_I = PythonOperator(
    task_id='execute_csv_trade_etanol',
    python_callable=execute_csv_trade_etanol,
    provide_context=True,
    op_kwargs={
                'local_path': LOCAL_PATH,
                'file_csv': FILE_CSV
            },
    dag=dag
    
)

Task_II = PythonOperator(
    task_id="insert_trade_values_in_db", 
    python_callable=insert_trade_etanol_to_db,
    provide_context=True,
    op_kwargs={
                'local_path': LOCAL_PATH,
                'file_csv': FILE_CSV
            },
    dag=dag
)

Task_III = PythonOperator(
    task_id="check_trade_etanol_db", 
    python_callable=check_trade_etanol_db,
    provide_context=True,
    op_kwargs={
                'local_path': LOCAL_PATH,
                'file_csv': FILE_CSV
            },
    dag=dag
)
# [END Tasks]

# [START documentation]
dag.doc_md = __doc__

Task_I.doc_md = """\
#### Task Documentation
It is my tests for connection in db create table and add
itens.
"""
# [END documentation]
Task_I >> Task_II
Task_II >> Task_III