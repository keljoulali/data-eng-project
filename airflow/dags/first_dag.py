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
    template_searchpath=['/opt/airflow/data']
)

# Downloading a file from an API/endpoint?
def _extract_csv(output_folder : str):
    import opendatasets as od
    from io import BytesIO
    from urllib.request import urlopen
    from zipfile import ZipFile
    import os
    os.environ['KAGGLE_USERNAME'] = "khaliloooojo" # username from the json file
    os.environ['KAGGLE_KEY'] = "975fa88d4d3412eee830648e10973255" # key from the json file
    os.system('kaggle datasets download -d rounakbanik/the-movies-dataset -p /opt/airflow/data')
    # api copied from kaggle

    with ZipFile("/opt/airflow/data/the-movies-dataset.zip", "r") as zfile:
        zfile.extractall(output_folder)


extract_csv_node = PythonOperator(
    task_id='extract_csv',
    dag=first_dag,
    trigger_rule='none_failed',
    python_callable=_extract_csv,
    op_kwargs={
        "output_folder": "/opt/airflow/data"
    },
    depends_on_past=False
)
def _extract_wiki():
    # get data links from wiki : boucle for
    link1 = "https://en.wikipedia.org/wiki/List_of_American_films_of_2018"
    link2 = "https://en.wikipedia.org/wiki/List_of_American_films_of_2019"
    link3 = "https://en.wikipedia.org/wiki/List_of_American_films_of_2020"
    link4 = "https://en.wikipedia.org/wiki/List_of_American_films_of_2021"

# data frame 1
    df11=pd.read_html(link1, header=0)[2]
    df12=pd.read_html(link1, header=0)[3]
    df13=pd.read_html(link1, header=0)[4]
    df14=pd.read_html(link1, header=0)[5]

    df1 = df11.append(df12.append(df13.append(df14, ignore_index=True), ignore_index=True), ignore_index=True)

    df1.to_csv(r'/opt/airflow/data/movie2018.csv', index=False)
    # data frame 2
    df21=pd.read_html(link2, header=0)[2]
    df22=pd.read_html(link2, header=0)[3]
    df23=pd.read_html(link2, header=0)[4]
    df24=pd.read_html(link2, header=0)[5]

    df2 = df21.append(df22.append(df23.append(df24, ignore_index=True), ignore_index=True), ignore_index=True)

    df2.to_csv(r'/opt/airflow/data/movie2019.csv', index=False)
    # data frame 3
    df31=pd.read_html(link3, header=0)[2]
    df32=pd.read_html(link3, header=0)[3]
    df33=pd.read_html(link3, header=0)[4]
    df34=pd.read_html(link3, header=0)[5]

    df3 = df31.append(df32.append(df33.append(df34, ignore_index=True), ignore_index=True), ignore_index=True)

    df3.to_csv(r'/opt/airflow/data/movie2020.csv', index=False)
    # data frame 1
    df41=pd.read_html(link4, header=0)[2]
    df42=pd.read_html(link4, header=0)[3]
    df43=pd.read_html(link4, header=0)[4]
    df44=pd.read_html(link4, header=0)[5]

    df4 = df41.append(df42.append(df43.append(df44, ignore_index=True), ignore_index=True), ignore_index=True)

    df4.to_csv(r'/opt/airflow/data/movie2021.csv', index=False)


extract_wiki_node = PythonOperator(
    task_id='extract_wiki',
    dag=first_dag,
    trigger_rule='none_failed',
    python_callable=_extract_wiki,
    op_kwargs={
        "output_folder": "/opt/airflow/data"
    },
    depends_on_past=False,
)

def _ingest_csv() :
    from pymongo import MongoClient
    from random import randint
    import csv
    import warnings
    warnings.filterwarnings('ignore')

    #connect to mongodb
    myclient = MongoClient("mongodb://mongo:27017/") #Mongo URI format
    mydb = myclient["moviedb"]

    # create collections
    links_coll = mydb["links"]
    credits_coll= mydb["credits"]
    movies_metadata_coll = mydb["movies_metadata"]
    ratings_coll = mydb["ratings"]

    #links
    csvfile1 = open('/opt/airflow/data/links.csv','r')
    reader1 = csv.DictReader( csvfile1 )

    data = pd.read_csv(csvfile1)
    row = data.to_dict('reader1')

    mydb.links_coll.insert_many(row)
    #credits
    csvfile2 = open('/opt/airflow/data/credits.csv','r')
    reader2 = csv.DictReader( csvfile2 )

    data = pd.read_csv(csvfile2)
    row = data.to_dict('reader2')

    mydb.credits_coll.insert_many(row)
    #movies metadata
    csvfile3 = open('/opt/airflow/data/movies_metadata.csv','r')
    reader3 = csv.DictReader( csvfile3 )

    data = pd.read_csv(csvfile3)
    row = data.to_dict('reader3')

    mydb.movies_metadata_coll.insert_many(row)
    #ratings
    csvfile4 = open('/opt/airflow/data/ratings.csv','r')
    reader4  = csv.DictReader( csvfile4 )

    data = pd.read_csv(csvfile4)
    row = data.to_dict('reader4')

    mydb.ratings_coll.insert_many(row)

ingest_csv_node = PythonOperator(
    task_id='ingest_csv',
    dag=first_dag,
    trigger_rule='none_failed',
    python_callable=_ingest_csv,
    op_kwargs={

    },
    depends_on_past=False,
)

def _ingest_wiki() :
    from pymongo import MongoClient
    from random import randint
    import csv
    import warnings
    warnings.filterwarnings('ignore')

   #connect to mongodb
    myclient = MongoClient("mongodb://mongo:27017/") #Mongo URI format
    mydb = myclient["moviedb"]

  # create collections
    movie_coll_2018 = mydb["movie_2018"]
    movie_coll_2019 = mydb["movie_2019"]
    movie_coll_2020 = mydb["movie_2020"]
    movie_coll_2021 = mydb["movie_2021"]

    #2018 dataset
    csvfile1 = open('/opt/airflow/data/movie2018.csv','r')
    reader1 = csv.DictReader( csvfile1 )

    data = pd.read_csv(csvfile1)
    row = data.to_dict('reader1')

    mydb.movie_coll_2018.insert_many(row)

    #2019 dataset
    csvfile2 = open('/opt/airflow/data/movie2019.csv','r')
    reader2 = csv.DictReader( csvfile2 )

    data = pd.read_csv(csvfile2)
    row = data.to_dict('reader2')

    mydb.movie_coll_2019.insert_many(row)

    #2020 dataset
    csvfile3 = open('/opt/airflow/data/movie2020.csv','r')
    reader3 = csv.DictReader( csvfile3 )

    data = pd.read_csv(csvfile3)
    row = data.to_dict('reader3')

    mydb.movie_coll_2020.insert_many(row)

    #2021 dataset
    csvfile4 = open('/opt/airflow/data/movie2021.csv','r')
    reader4 = csv.DictReader( csvfile4 )

    data = pd.read_csv(csvfile4)
    row = data.to_dict('reader4')

    mydb.movie_coll_2021.insert_many(row)


ingest_wiki_node = PythonOperator(
    task_id='ingest_wiki',
    dag=first_dag,
    trigger_rule='none_failed',
    python_callable=_ingest_wiki,
    op_kwargs={

    },
    depends_on_past=False,
)


def mongo_df() :
    from pymongo import MongoClient
    from pandas import DataFrame
    myclient = MongoClient("mongodb://mongo:27017/") #Mongo URI format
    mydb = myclient["moviedb"]
    df=DataFrame(list(mydb.movie_coll_2018.find({})))
    df1=DataFrame(list(mydb.movies_metadata_coll.find({})))
    print(df1.head())
    print(df.head())

mongo_nod = PythonOperator(
    task_id='mongo_df',
    dag=first_dag,
    trigger_rule='none_failed',
    python_callable=mongo_df,
    op_kwargs={

    },
    depends_on_past=False,
)



[extract_csv_node >>ingest_csv_node , extract_wiki_node >> ingest_wiki_node ] >> mongo_nod


