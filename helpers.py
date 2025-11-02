import requests
import os
import streamlit as st
from sqlalchemy import create_engine
import datetime
import pandas as pd
import logging
import random
from functools import cache
import duckdb as dd
import json
from langchain_community.utilities import SQLDatabase
from langchain_community.document_loaders import TextLoader
from langchain_text_splitters import  CharacterTextSplitter
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

# -----------------------------------------------------------------------------------------------------------
# Code below is the ETL process for fetching data from APIs and storing in a Postgres DB
#
# -----------------------------------------------------------------------------------------------------------

endDate = datetime.date.today()
startDate = endDate - datetime.timedelta(days=1)
# dd_conn = dd.connect('db/geovac.db')
# dd_conn = create_engine("duckdb:///db/geovac.db")

# noinspection PyPackageRequirements
cities = ['Mexico','New York','Toronto','Guadalajara','Montreal','Los Angeles','Chicago','Vancouver',
        'San Juan','Tijuana','Brooklyn','Houston','Havana','Panama City','Calgary','Edmonton','Philadelphia',
        'Phoenix','San Jose','Manhattan','Ottawa','San Diego','Juarez','Dallas']


now_time = datetime.datetime.now().strftime("%d%H%M%S")

logging.basicConfig(level=logging.INFO, filename=os.path.dirname(os.path.abspath('logs.txt')) + '/logs.txt',
                    format='%(asctime)s :: %(levelname)s :: %(message)s')
def connect_to_db():
    try:
        # DB_USER = os.getenv("DB_USER")
        # DB_PASSWORD = os.getenv("DB_PASSWORD")
        # DB_HOST = os.getenv("DB_HOST")
        # DB_PORT = os.getenv("DB_PORT")
        # DB_NAME = os.getenv("DB_NAME")
        # pgdb_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        # pgdb_url = f"{os.getenv('SUPABASE_URI')}"
        pgdb_url = f"{st.secrets['SUPABASE_URI']}"
        engine = create_engine(pgdb_url)
        db = SQLDatabase(engine=engine)
        # engine = SQLDatabase.from_uri(neon_conn_string, sample_rows_in_table_info=0)
        return db, engine
    except Exception as e:
        print(f"Error connecting to DB: {e}")
        return None

# eng, conn = connect_to_db()

# db_conn = conn.connect()

def fetch_data(*args):
    # api_objects = os.getenv("API_OBJECTS")
    api_objects = st.secrets["API_OBJECTS"]
    for obj in api_objects.split(","):
        if "boc_fx" in obj:
            try:
                boc_fx_url = f"{os.getenv('BOC_FX_BASE_URL')}start_date={startDate}&end_date={endDate}&order_dir=asc"
                boc_fx_response = requests.get(boc_fx_url)
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data from {boc_fx_url}: {e}")

        if "persons" in obj:
            try:
                # persons_url = os.getenv("ICIJ_BASE_URL")
                persons_url = st.secrets["ICIJ_BASE_URL"]
                persons_response = requests.get(persons_url)
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data from {persons_url}: {e}")

        if "weather" in obj:
            try:
                # weather_url = f"{os.getenv('WEATHER_BASE_URL')}{os.getenv('WEATHER_API_KEY')}&q={random.choice(cities)}&aqi=yes"
                weather_url = f"{st.secrets['WEATHER_BASE_URL']}{st.secrets['WEATHER_API_KEY']}&q={random.choice(cities)}&aqi=yes"
                weather_response = requests.get(weather_url)
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data from {weather_url}: {e}")

    return boc_fx_response.json(), persons_response.json(), weather_response.json()


def transform_api_response():
    boc_fx_response, persons_response, weather_response = fetch_data(startDate, endDate)
    dd_conn = dd.connect('db/geovac.db')
    # boc_rdbms_format = []
    boc_normalized = []
    for i in boc_fx_response["observations"]:
        data_prep = {'date': i['d'],
                     'value': i['FXUSDCAD']['v']
                     }
        for key, value in boc_fx_response['seriesDetail'].items():
            data_prep["label"] = value['label']
        boc_normalized.append(data_prep)
        bocfx_df = pd.DataFrame(boc_normalized)
        dd_conn.execute("CREATE OR REPLACE TABLE boc_fx AS SELECT * FROM bocfx_df")
        print(f"boc_fx data has been written to duckdb: {data_prep}")


    person_df = []
    person_normalized = []
    for items in persons_response['data']:
        # print(items)
        data_prep = {'country_code': str(items['properties']['country_codes']).strip('[').strip(']').replace("'", "").replace('',"ZZZ"),
                     'name': items['properties']['name'],
                     'source_schema': items['schema']
                     }
        person_normalized.append(data_prep)
    person_df = pd.DataFrame(person_normalized)
    # print(person_rdbms_format)
    dd_conn.execute("CREATE OR REPLACE TABLE persons AS SELECT * FROM person_df")

    weather_df = []
    normalized = []
    for k, v in weather_response['location'].items():
        if k == 'name':
            # print(f"{k} : {v}")
            data_prep = {'name': v}
        if k == 'country':
            # print(f"{k} : {v}")
            data_prep['country'] = v
        if k == 'lat':
            # print(f"{k} : {v}")
            data_prep['lat'] = v
        if k == 'lon':
            # print(f"{k} : {v}")
            data_prep['lon'] = v
    # print(data_prep)
    for k, v in weather_response['current'].items():
        if k == 'last_updated':
            # print(f"{k} : {v}")
            data_prep['last_updated'] = v
        if k == 'temp_c':
            # print(f"{k} : {v}")
            data_prep['temp_c'] = v
        if k == 'temp_f':
            # print(f"{k} : v")
            data_prep['temp_f'] = v
    for k, v in weather_response['current']['condition'].items():
        if k == 'text':
            # print(f"{k} : {v}")
            data_prep['text'] = v
            # data_prep['id'] = now_time
    normalized.append(data_prep)
    weather_df = pd.DataFrame(normalized)
    # print(weather_rdbms_format)
    dd_conn.execute("CREATE OR REPLACE TABLE weather AS SELECT * FROM weather_df")

    dd_conn.close()

    return None

@st.dialog(title="Contact Me", width="small")
def show_contact_form():
    st.text_input("Name")
    pass


# def get_news_article():
#     dd_conn = dd.connect('db/geovac.db')
#     topics = ['cyber security']
#     date_today = datetime.datetime.now().strftime("%Y-%m-%d")
#     w_news_url = f"https://api.worldnewsapi.com/search-news?text={random.choice(topics)}&language=en&earliest-publish-date={date_today}"
#
#     headers = {
#         'x-api-key': st.secrets['w_news_api']
#     }
#
#     result = requests.get(w_news_url, headers=headers)
#     news_result = result.json()
#     print(f"THis is news_result: \n {news_result}")
#     all_news = []
#     for i in news_result['news']:
#         ind_news = {
#             "id": i["id"],
#             "news_author": i["author"],
#             "news_title": i["title"],
#             "news_url": i["url"]
#         }
#         all_news.append(ind_news)
#     news_df = pd.DataFrame(all_news)
#     print(f" This is the news_df data: \n {news_df}")
#     dd_conn.execute("CREATE OR REPLACE TABLE news AS SELECT * FROM news_df")
#
#     return news_df



def query_duck_db(sql_):
    dd_conn = dd.connect('db/geovac.db')
    result = dd_conn.execute(sql_).fetchdf()
    dd_conn.close()
    return result

# news_data = get_news_article()

