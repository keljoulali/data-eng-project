import airflow
import datetime
import urllib.request as request
import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
import requests
import random
import json
import glob


default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

first_dag = DAG(
    dag_id='first_dag',
    default_args=default_args_dict,
    catchup=False,
)

# Downloading a file from an API/endpoint?
def _extract_csv(output_folder : str, url : str):
    from io import BytesIO
    from urllib.request import urlopen
    from zipfile import ZipFile
    zipurl = url
    with urlopen(zipurl) as zipresp:
        with ZipFile(BytesIO(zipresp.read())) as zfile:
            zfile.extractall(output_folder)


first_node = PythonOperator(
    task_id='extract_csv',
    dag=first_dag,
    trigger_rule='none_failed',
    python_callable=_extract_csv,
    op_kwargs={
        "output_folder": "/usr/local/airflow/data",
        "url": "https://files.grouplens.org/datasets/movielens/ml-1m.zip"
    },
    depends_on_past=False,
)
def _extract_wiki(output_folder : str ):
    # get data links from wiki
    link1 = "https://en.wikipedia.org/wiki/List_of_American_films_of_2018"
    #link2 = "https://en.wikipedia.org/wiki/List_of_American_films_of_2019"
   # link3 = "https://en.wikipedia.org/wiki/List_of_American_films_of_2020"
   # link4 = "https://en.wikipedia.org/wiki/List_of_American_films_of_2021"

    df11=pd.read_html(link1, header=0)[2]
    df12=pd.read_html(link1, header=0)[3]
    df13=pd.read_html(link1, header=0)[4]
    df14=pd.read_html(link1, header=0)[5]

    df1 = df11.append(df12.append(df13.append(df14,ignore_index=true),ignore_index=true),ignore_index=true)

    df1


second_node = PythonOperator(
    task_id='extract_wiki',
    dag=first_dag,
    trigger_rule='none_failed',
    python_callable=_extract_wiki,
    op_kwargs={
        "output_folder": "/usr/local/airflow/data"
    },
    depends_on_past=False,
)
def _ingest_csv() :


def _ingest_wiki() :

def _insert_csv_mongo() :

def _insert_wiki_mongo() :

def _enrich() :
first_node
second_node


