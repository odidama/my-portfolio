import requests
import os
import streamlit as st
from sqlalchemy import create_engine
import datetime
import pandas as pd
import logging
import random
from functools import cache
from langchain_community.utilities import SQLDatabase
from langchain_community.document_loaders import TextLoader
from langchain.text_splitter import CharacterTextSplitter
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

# -----------------------------------------------------------------------------------------------------------
# Code below is the ETL process for fetching data from APIs and storing in a Postgres DB
#
# -----------------------------------------------------------------------------------------------------------

endDate = datetime.date.today()
startDate = endDate - datetime.timedelta(days=1)
cities = ['Mexico','New York','Toronto','Guadalajara','Montreal','Los Angeles','Chicago','Vancouver',
        'San Juan','Tijuana','Brooklyn','Houston','Havana','Panama City','Calgary','Edmonton','Philadelphia',
        'Phoenix','San Jose','Manhattan','Ottawa','San Diego','Juarez','Dallas']

topics = ['ai','cyber security']
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

eng, conn = connect_to_db()
db_conn = conn.connect()

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
    boc_rdbms_format = []
    boc_normalized = []
    for i in boc_fx_response["observations"]:
        # print(i)
        data_prep = {'date': i['d'],
                     'value': i['FXUSDCAD']['v']
                     }
        for key, value in boc_fx_response['seriesDetail'].items():
            data_prep["label"] = value['label']
        boc_normalized.append(data_prep)
    boc_rdbms_format = pd.DataFrame(boc_normalized)
    # print(boc_rdbms_format)
    boc_rdbms_format.to_sql(name='boc_fx', con=conn, if_exists='append', index=False)

    person_rdbms_format = []
    person_normalized = []
    for items in persons_response['data']:
        # print(items)
        data_prep = {'country_code': str(items['properties']['country_codes']).strip('[').strip(']').replace("'", "").replace('',"ZZZ"),
                     'name': items['properties']['name'],
                     'source_schema': items['schema']
                     }
        person_normalized.append(data_prep)
    person_rdbms_format = pd.DataFrame(person_normalized)
    # print(person_rdbms_format)
    person_rdbms_format.to_sql(name='persons', con=conn, if_exists='append', index=False)

    weather_rdbms_format = []
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
    weather_rdbms_format = pd.DataFrame(normalized)
    # print(weather_rdbms_format)
    weather_rdbms_format.to_sql(name='weather', con=conn, if_exists='append', index=False)

    return weather_rdbms_format

@st.dialog(title="Contact Me", width="small")
def show_contact_form():
    st.text_input("Name")
    pass

# -----------------------------------------------------------------------------------------------------------
#
# Canada population dashboard helpers
#
# -----------------------------------------------------------------------------------------------------------

@cache
def read_population_data():
    population_df = pd.read_sql_table("population", con=conn)
    # print(population_df)
    return population_df

df = read_population_data()

def transform_df():

    can_df = df.loc[df['REGION_NAME'] == 'Canada']
    can_df_agGrp = df.loc[df['REGION_NAME'] == 'Canada'].iloc[8:33]
    can_df_summary = df.loc[df['REGION_NAME'] == 'Canada'].iloc[0:7]

    nfl_df = df.loc[df['REGION_NAME'] == 'Newfoundland and Labrador']
    nfl_df_agGrp = df.loc[df['REGION_NAME'] == 'Newfoundland and Labrador'].iloc[8:33]
    nfl_df_summary = df.loc[df['REGION_NAME'] == 'Newfoundland and Labrador'].iloc[0:7]

    pei_df = df.loc[df['REGION_NAME'] == 'Prince Edward Island']
    pei_df_agGrp = df.loc[df['REGION_NAME'] == 'Prince Edward Island'].iloc[8:33]
    pei_df_summary = df.loc[df['REGION_NAME'] == 'Prince Edward Island'].iloc[0:7]

    ns_df = df.loc[df['REGION_NAME'] == 'Nova Scotia']
    ns_df_agGrp = df.loc[df['REGION_NAME'] == 'Nova Scotia'].iloc[8:33]
    ns_df_summary = df.loc[df['REGION_NAME'] == 'Nova Scotia'].iloc[0:7]

    nb_df = df.loc[df['REGION_NAME'] == 'New Brunswick']
    nb_df_agGrp = df.loc[df['REGION_NAME'] == 'New Brunswick'].iloc[8:33]
    nb_df_summary = df.loc[df['REGION_NAME'] == 'New Brunswick'].iloc[0:7]

    qu_df = df.loc[df['REGION_NAME'] == 'Quebec']
    qu_df_agGrp = df.loc[df['REGION_NAME'] == 'Quebec'].iloc[8:33]
    qu_df_summary = df.loc[df['REGION_NAME'] == 'Quebec'].iloc[0:7]

    on_df = df.loc[df['REGION_NAME'] == 'Ontario']
    on_df_agGrp = df.loc[df['REGION_NAME'] == 'Ontario'].iloc[8:33]
    on_df_summary = df.loc[df['REGION_NAME'] == 'Ontario'].iloc[0:7]

    mb_df = df.loc[df['REGION_NAME'] == 'Manitoba']
    mb_df_agGrp = df.loc[df['REGION_NAME'] == 'Manitoba'].iloc[8:33]
    mb_df_summary = df.loc[df['REGION_NAME'] == 'Manitoba'].iloc[0:7]

    sk_df = df.loc[df['REGION_NAME'] == 'Saskatchewan']
    sk_df_agGrp = df.loc[df['REGION_NAME'] == 'Saskatchewan'].iloc[8:33]
    sk_df_summary = df.loc[df['REGION_NAME'] == 'Saskatchewan'].iloc[0:7]

    ab_df = df.loc[df['REGION_NAME'] == 'Alberta']
    ab_df_AgGrp = df.loc[df['REGION_NAME'] == 'Alberta'].iloc[8:33]
    ab_df_summary = df.loc[df['REGION_NAME'] == 'Alberta'].iloc[0:7]

    bc_df = df.loc[df['REGION_NAME'] == 'British Columbia']
    bc_df_agGrp = df.loc[df['REGION_NAME'] == 'British Columbia'].iloc[8:33]
    bc_df_summary = df.loc[df['REGION_NAME'] == 'British Columbia'].iloc[0:7]

    yk_df = df.loc[df['REGION_NAME'] == 'Yukon']
    yk_df_agGrp = df.loc[df['REGION_NAME'] == 'Yukon'].iloc[8:33]
    yk_df_summary = df.loc[df['REGION_NAME'] == 'Yukon'].iloc[0:7]

    nwt_df = df.loc[df['REGION_NAME'] == 'Northwest Territories']
    nwt_df_agGrp = df.loc[df['REGION_NAME'] == 'Northwest Territories'].iloc[8:33]
    nwt_df_summary = df.loc[df['REGION_NAME'] == 'Northwest Territories'].iloc[0:7]

    nu_df = df.loc[df['REGION_NAME'] == 'Nunavut']
    nu_df_agGrp = df.loc[df['REGION_NAME'] == 'Nunavut'].iloc[8:33]
    nu_df_summary = df.loc[df['REGION_NAME'] == 'Nunavut'].iloc[0:7]

    all_AgeGrp_Data_combined = pd.concat([can_df_agGrp, nfl_df_agGrp, pei_df_agGrp, ns_df_agGrp, nb_df_agGrp,
                                          qu_df_agGrp, on_df_agGrp, mb_df_agGrp, nb_df_agGrp, sk_df_agGrp, ab_df_AgGrp,
                                          bc_df_agGrp, yk_df_agGrp, nwt_df_agGrp, nu_df_agGrp], axis=0)

    all_Df_Summary_Combined = pd.concat([can_df_summary, nfl_df_summary, pei_df_summary, ns_df_summary, nb_df_summary,
                                         qu_df_summary, on_df_summary, mb_df_summary, nb_df_summary, sk_df_summary,
                                         ab_df_summary, bc_df_summary, yk_df_summary, nwt_df_summary,
                                         nu_df_summary], axis=0)

    return all_AgeGrp_Data_combined, all_Df_Summary_Combined


def get_news_article():
    # news_url = f"https://newsapi.org/v2/everything?q={random.choice(topics)}&apiKey={os.getenv('NEWS_API_KEY')}&pageSize=1"
    news_url = f"https://newsapi.org/v2/everything?q={random.choice(topics)}&apiKey={st.secrets['NEWS_API_KEY']}&pageSize=1"
    result = requests.get(news_url)
    news_result = result.json()
    print(type(news_result))
    for i in news_result["articles"]:
        source = i["source"]["name"]
        author = i["author"]
        title = i["title"]
        description = i["description"]
        url = i["url"]
        # print(i)
    return source, author, title, description, url