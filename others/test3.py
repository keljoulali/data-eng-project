import pandas as pd
import numpy as np
data3 = pd.read_csv('airflow/data/movie2018.csv')
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