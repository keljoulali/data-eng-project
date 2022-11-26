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
    # dataframes append
    df1 = df11.append(df12.append(df13.append(df14, ignore_index=True), ignore_index=True), ignore_index=True)
    # dataframe to csv save
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
    reader2 = csv.DictReader(csvfile2)

    data = pd.read_csv(csvfile2)
    row = data.to_dict('reader2')

    mydb.credits_coll.insert_many(row)
    #movies metadata
    csvfile3 = open('/opt/airflow/data/movies_metadata.csv','r')
    reader3 = csv.DictReader( csvfile3 )

    data = pd.read_csv(csvfile3)
    row = data.to_dict('reader3')

    mydb.movies_metadata_coll.insert_many(row)

    #connect to mongodb

    #ratings
    csvfile4 = open('/opt/airflow/data/ratings_small.csv','r')
    reader4  = csv.DictReader( csvfile4 )

    data = pd.read_csv(csvfile4)
    row = data.to_dict('reader4')

    mydb.ratings_small_coll.insert_many(row)


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
def _wrangle_csv() :
    import pandas as pd
    import numpy as np
    #get data from mongoDB
    movie_metadata_2016 = mongo_df("movies_metadata_coll")
    credit = mongo_df('credits_coll')

    #get the year of release date
    movie_metadata_2016['release_date'] = pd.to_datetime(movie_metadata_2016['release_date'], errors='coerce')
    movie_metadata_2016.rename(columns={'release_date':'date'})
    meta = movie_metadata_2016.loc[movie_metadata_2016.year <= 2017,['id','title','genres','year']]

    meta['id'] = meta['id'].astype(int)

    data = pd.merge(meta, credit, on='id')

    import ast
    data['genres'] = data['genres'].map(lambda x: ast.literal_eval(x))
    data['cast'] = data['cast'].map(lambda x: ast.literal_eval(x))
    data['crew'] = data['crew'].map(lambda x: ast.literal_eval(x))

    def get_genre(x):
        gen = []
        st = " "
        for i in x:
            if i.get('name') == 'Science Fiction':
                scifi = 'Sci-Fi'
                gen.append(scifi)
            else:
                gen.append(i.get('name'))
        if gen == []:
            return np.NaN
        else:
            return (st.join(gen))
    data['genre'] = data['genres'].map(lambda x: get_genre(x))
    data.drop(columns='genres')

    def get_actor1(x):
        casts = []
        for i in x:
            casts.append(i.get('name'))
        if casts == []:
            return np.NaN
        else:
            return (casts[0])

    data['actor1'] = data['cast'].map(lambda x: get_actor1(x))

    def get_actor2(x):
        casts = []
        for i in x:
            casts.append(i.get('name'))
        if casts == [] or len(casts)<=1:
            return np.NaN
        else:
            return (casts[1])

    data['actor2'] = data['cast'].map(lambda x: get_actor2(x))

    def get_actor3(x):
        casts = []
        for i in x:
            casts.append(i.get('name'))
        if casts == [] or len(casts)<=2:
            return np.NaN
        else:
            return (casts[2])

    data['actor3'] = data['cast'].map(lambda x: get_actor3(x))

    def get_directors(x):
        dt = []
        st = " "
        for i in x:
            if i.get('job') == 'Director':
                dt.append(i.get('name'))
        if dt == []:
            return np.NaN
        else:
            return (st.join(dt))

    data['director'] = data['crew'].map(lambda x: get_directors(x))

    new_data = data.loc[:,['title','genre','director','actor_name1','actor2','actor3','year']]

    new_data = new_data.dropna(how='any')
    print(new_data.head)


wrangle_csv_node = PythonOperator(
    task_id='wrangle_csv',
    dag=first_dag,
    trigger_rule='none_failed',
    python_callable=_wrangle_csv,
    op_kwargs={

    },
    depends_on_past=False,
)

def _wrangle_wiki_2018():
    import pandas as pd
    import numpy as np
    data3 = mongo_df('movie_coll_2018')
    data3['Ref.'] = data3['Ref.'].fillna(data3.pop('.mw-parser-output .tooltip-dotted{border-bottom:1px dotted;cursor:help}Ref.'))

    def get_directors(x):
        if " (director)" in x:
            return x.split(" (director)")[0]
        elif " (directors)" in x:
            return x.split(" (directors)")[0]
        else:
            return x.split(" (director/screenplay)")[0]

    data3['director'] = data3['Cast and crew'].map(lambda x: get_directors(x))

    def get_actor1(x):
        return ((x.split("screenplay); ")[-1]).split(", ")[0])

    data3['actor1'] = data3['Cast and crew'].map(lambda x: get_actor1(x))
    def get_actor2(x):
        if len((x.split("screenplay); ")[-1]).split(", ")) < 2:
            return np.NaN
        else:
            return ((x.split("screenplay); ")[-1]).split(", ")[1])
    data3['actor2'] = data3['Cast and crew'].map(lambda x: get_actor2(x))

    def get_actor3(x):
        if len((x.split("screenplay); ")[-1]).split(", ")) < 3:
            return np.NaN
        else:
            return ((x.split("screenplay); ")[-1]).split(", ")[2])
    data3['actor3'] = data3['Cast and crew'].map(lambda x: get_actor2(x))

    data3 = data3.rename(columns={'Title':'title'})
    data3['year']='2018'
    new_data = data3.loc[:,['title','director','actor1','actor2','actor3','year']]
    d={'JANUARY':'01','FEBRUARY':'02','MARCH':'03','APRIL':'04','MAY':'05','JUNE':'06','JULY':'07','AUGUST':'08','SEPTEMBER':'09','OCTOBER':'10','NOVEMBER':'11','DECEMBER':'12'}
    new_data['month']=data3['Opening']
    new_data['day']=data3['Opening.1']
    new_data['month']=new_data['month'].map(d)

    new_data['year'] = new_data['year'].astype(str)
    new_data['month'] = new_data['month'].astype(str)
    new_data['day'] = new_data['day'].astype(str)

    new_data['date']=new_data['day']+'/'+new_data['month']+'/'+new_data['year']
    new_data['date']=pd.to_datetime(new_data['date'])

    new_data.drop(columns={'year','day','month'})
wrangle_wiki_2018_node = PythonOperator(
    task_id='wrangle_wiki_2018',
    dag=first_dag,
    trigger_rule='none_failed',
    python_callable=_wrangle_wiki_2018,
    op_kwargs={

    },
    depends_on_past=False,
)

def _wrangle_wiki_2019():
    import pandas as pd
    import numpy as np
    movie_data_2019 = mongo_df('movie_coll_2019')
    def get_directors(x):
        if " (director)" in x:
            return x.split(" (director)")[0]
        elif " (directors)" in x:
            return x.split(" (directors)")[0]
        else:
            return x.split(" (director/screenplay)")[0]

    movie_data_2019['director'] = movie_data_2019['Cast and crew'].map(lambda x: get_directors(x))

    def get_actor1(x):
        return ((x.split("screenplay); ")[-1]).split(", ")[0])

    movie_data_2019['actor1'] = movie_data_2019['Cast and crew'].map(lambda x: get_actor1(x))
    def get_actor2(x):
        if len((x.split("screenplay); ")[-1]).split(", ")) < 2:
            return np.NaN
        else:
            return ((x.split("screenplay); ")[-1]).split(", ")[1])
    movie_data_2019['actor2'] = movie_data_2019['Cast and crew'].map(lambda x: get_actor2(x))

    def get_actor3(x):
        if len((x.split("screenplay); ")[-1]).split(", ")) < 3:
            return np.NaN
        else:
            return ((x.split("screenplay); ")[-1]).split(", ")[2])
    movie_data_2019['actor3'] = movie_data_2019['Cast and crew'].map(lambda x: get_actor2(x))

    movie_data_2019 = movie_data_2019.rename(columns={'Title':'title'})
    movie_data_2019['year']='2018'
    new_data_2019 = movie_data_2019.loc[:,['title','director','actor1','actor2','actor3','year']]
    d={'JANUARY':'01','FEBRUARY':'02','MARCH':'03','APRIL':'04','MAY':'05','JUNE':'06','JULY':'07','AUGUST':'08','SEPTEMBER':'09','OCTOBER':'10','NOVEMBER':'11','DECEMBER':'12'}
    new_data_2019['month']=movie_data_2019['Opening']
    new_data_2019['day']=movie_data_2019['Opening.1']
    new_data_2019['month']=new_data_2019['month'].map(d)

    new_data_2019['year'] = new_data_2019['year'].astype(str)
    new_data_2019['month'] = new_data_2019['month'].astype(str)
    new_data_2019['day'] = new_data_2019['day'].astype(str)

    new_data_2019['date']=new_data_2019['day']+'/'+new_data_2019['month']+'/'+new_data_2019['year']
    new_data_2019['date']=pd.to_datetime(new_data_2019['date'])

    new_data_2019.drop(columns={'year','day','month'})

wrangle_wiki_2019_node = PythonOperator(
    task_id='wrangle_wiki_2019',
    dag=first_dag,
    trigger_rule='none_failed',
    python_callable=_wrangle_wiki_2019,
    op_kwargs={

    },
    depends_on_past=False,
)

def _wrangle_wiki_2020():
    import pandas as pd
    import numpy as np
    movie_data_2020= mongo_df('movie_coll_2020')
    movie_data_2020['Ref.'] = movie_data_2020['Ref.'].fillna(movie_data_2020.pop('.mw-parser-output .tooltip-dotted{border-bottom:1px dotted;cursor:help}Ref.'))

    def get_directors(x):
        if " (director)" in x:
            return x.split(" (director)")[0]
        elif " (directors)" in x:
            return x.split(" (directors)")[0]
        else:
            return x.split(" (director/screenplay)")[0]

    movie_data_2020['director'] = movie_data_2020['Cast and crew'].map(lambda x: get_directors(x))

    def get_actor1(x):
        return ((x.split("screenplay); ")[-1]).split(", ")[0])

    movie_data_2020['actor1'] = movie_data_2020['Cast and crew'].map(lambda x: get_actor1(x))
    def get_actor2(x):
        if len((x.split("screenplay); ")[-1]).split(", ")) < 2:
            return np.NaN
        else:
            return ((x.split("screenplay); ")[-1]).split(", ")[1])
    movie_data_2020['actor2'] = movie_data_2020['Cast and crew'].map(lambda x: get_actor2(x))

    def get_actor3(x):
        if len((x.split("screenplay); ")[-1]).split(", ")) < 3:
            return np.NaN
        else:
            return ((x.split("screenplay); ")[-1]).split(", ")[2])
    movie_data_2020['actor3'] = movie_data_2020['Cast and crew'].map(lambda x: get_actor2(x))

    movie_data_2020 = movie_data_2020.rename(columns={'Title': 'title'})
    movie_data_2020['year']= '2018'
    new_data_2020 = movie_data_2020.loc[:, ['title', 'director', 'actor1', 'actor2', 'actor3', 'year']]
    d={'JANUARY':'01','FEBRUARY':'02','MARCH':'03','APRIL':'04','MAY':'05','JUNE':'06','JULY':'07','AUGUST':'08','SEPTEMBER':'09','OCTOBER':'10','NOVEMBER':'11','DECEMBER':'12'}
    new_data_2020['month']=movie_data_2020['Opening']
    new_data_2020['day']=movie_data_2020['Opening.1']
    new_data_2020['month']=new_data_2020['month'].map(d)

    new_data_2020['year'] = new_data_2020['year'].astype(str)
    new_data_2020['month'] = new_data_2020['month'].astype(str)
    new_data_2020['day'] = new_data_2020['day'].astype(str)

    new_data_2020['date']=new_data_2020['day']+'/'+new_data_2020['month']+'/'+new_data_2020['year']
    new_data_2020['date']=pd.to_datetime(new_data_2020['date'])

    new_data_2020.drop(columns={'year','day','month'})

wrangle_wiki_2020_node = PythonOperator(
    task_id='wrangle_wiki_2020',
    dag=first_dag,
    trigger_rule='none_failed',
    python_callable=_wrangle_wiki_2020,
    op_kwargs={

    },
    depends_on_past=False,
)

def _wrangle_wiki_2021():
    import pandas as pd
    import numpy as np
    movie_data_2021 = mongo_df('movie_coll_2021')
    movie_data_2021['Ref.'] = movie_data_2021['Ref.'].fillna(movie_data_2021.pop('.mw-parser-output .tooltip-dotted{border-bottom:1px dotted;cursor:help}Ref.'))

    def get_directors(x):
        if " (director)" in x:
            return x.split(" (director)")[0]
        elif " (directors)" in x:
            return x.split(" (directors)")[0]
        else:
            return x.split(" (director/screenplay)")[0]

    movie_data_2021['director'] = movie_data_2021['Cast and crew'].map(lambda x: get_directors(x))

    def get_actor1(x):
        return ((x.split("screenplay); ")[-1]).split(", ")[0])

    movie_data_2021['actor1'] = movie_data_2021['Cast and crew'].map(lambda x: get_actor1(x))
    def get_actor2(x):
        if len((x.split("screenplay); ")[-1]).split(", ")) < 2:
            return np.NaN
        else:
            return ((x.split("screenplay); ")[-1]).split(", ")[1])
    movie_data_2021['actor2'] = movie_data_2021['Cast and crew'].map(lambda x: get_actor2(x))

    def get_actor3(x):
        if len((x.split("screenplay); ")[-1]).split(", ")) < 3:
            return np.NaN
        else:
            return ((x.split("screenplay); ")[-1]).split(", ")[2])
    movie_data_2021['actor3'] = movie_data_2021['Cast and crew'].map(lambda x: get_actor2(x))

    movie_data_2021 = movie_data_2021.rename(columns={'Title':'title'})
    movie_data_2021['year']='2018'
    new_data_2021 = movie_data_2021.loc[:,['title','director','actor1','actor2','actor3','year']]
    d={'JANUARY':'01','FEBRUARY':'02','MARCH':'03','APRIL':'04','MAY':'05','JUNE':'06','JULY':'07','AUGUST':'08','SEPTEMBER':'09','OCTOBER':'10','NOVEMBER':'11','DECEMBER':'12'}
    new_data_2021['month']=movie_data_2021['Opening']
    new_data_2021['day']=movie_data_2021['Opening.1']
    new_data_2021['month']=new_data_2021['month'].map(d)

    new_data_2021['year'] = new_data_2021['year'].astype(str)
    new_data_2021['month'] = new_data_2021['month'].astype(str)
    new_data_2021['day'] = new_data_2021['day'].astype(str)

    new_data_2021['date']=new_data_2021['day']+'/'+new_data_2021['month']+'/'+new_data_2021['year']
    new_data_2021['date']=pd.to_datetime(new_data_2021['date'])

    new_data_2021.drop(columns={'year','day','month'})

wrangle_wiki_2021_node = PythonOperator(
    task_id='wrangle_wiki_2021',
    dag=first_dag,
    trigger_rule='none_failed',
    python_callable=_wrangle_wiki_2021,
    op_kwargs={

    },
    depends_on_past=False,
)


def mongo_df(collection) :
    from pymongo import MongoClient
    from pandas import DataFrame
    myclient = MongoClient("mongodb://mongo:27017/") #Mongo URI format
    mydb = myclient["moviedb"]
    cursor = mydb[collection].find({})
    df = pd.DataFrame(list(cursor))
    return df



extract_csv_node >>ingest_csv_node>>wrangle_csv_node
extract_wiki_node >> ingest_wiki_node >>[wrangle_wiki_2018_node,wrangle_wiki_2019_node,wrangle_wiki_2020_node, wrangle_wiki_2021_node]
