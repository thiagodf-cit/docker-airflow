import pandas as pd
import numpy as np
import csv
import io
import os

from airflow.models import DAG

LOCAL_PATH = "/usr/local/airflow/files/"


def load_file():
    file = open(LOCAL_PATH + '.csv','r',encoding="utf8")
    print("File being processed----", filename)
    path = os.getcwd()
    

# for filename in os.listdir(LOCAL_PATH):
#     print("File being processed----", filename)
#     file_name = filename.split('.')[0]
#     f = open(LOCAL_PATH + file_name + '.csv','r',encoding="utf8")
#     lines = f.read()
#     file_stream = io.StringIO(lines)
#     Split_Rows = [x for x in file_stream if x.count('|') < 297]
#     Split_Rows = ' '.join(map(str, Split_Rows))
#     Split_Rows_Stream = pd.DataFrame(io.StringIO(Split_Rows),columns=['blob'])
#     Split_Rows_Stream.to_csv(LOCAL_PATH + filename + "_error.csv",escapechar=';',encoding='utf-8')