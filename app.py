import streamlit as st
import plotly.express as px
from decimal import Decimal
import datetime
from plotly.graph_objs.bar.marker.colorbar import Title
import helpers
import pandas as pd

eng, conn = helpers.connect_to_db()
db_conn = conn.connect()
today = datetime.date.today()
# time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

weather = helpers.transform_api_response()
temp = weather[["temp_c","name","text","last_updated"]].sample(n=1).values[0][0]
city = weather[["temp_c","name","text","last_updated"]].sample(n=1).values[0][1]
remark = weather[["temp_c","name","text","last_updated"]].sample(n=1).values[0][2]
time_ = weather[["temp_c","name","text","last_updated"]].sample(n=1).values[0][3]
boc_df = pd.read_sql_table("boc_fx", con=conn)
fx_val = boc_df["value"].head(1).tolist()[0]

st.set_page_config(
    page_title="My Portfolio Page",
    page_icon="🌿",
    layout="wide"
)

st.markdown(
    """
    <style>
    section[data-testid="stSidebar"] {
        width: 310px !important; /* Set a fixed width */
        min-width: 300px; /* Set a minimum width */
        max-width: 500px; /* Set a maximum width */
    }
    </style>
    """,
    unsafe_allow_html=True,
)
st.markdown(
    """
    <style>
    div[data-testid="stMetric"] {
        border: 1px solid green; 
        border-radius: 10px; /* Optional: Adds rounded corners */
        padding: 1px; /* Optional: Adds padding inside the border */
        padding-top: 1px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
        """
        <style>
        div[data-testid="stMetricValue"] > div {
            font-size: 1.5em; /* Adjust this value as needed (e.g., 2em, 48px) */
        }
        </style>
        """,
        unsafe_allow_html=True
    )

st.markdown("""
    <style>
    .block-container {
        padding-top: 1rem; /* Adjust this value as needed */
        padding-bottom: 0rem;
        padding-left: 5rem;
        padding-right: 5rem;
    }
    </style>
    """, unsafe_allow_html=True)

with st.sidebar:
    st.sidebar.page_link("app.py", label="Home", icon="🦧")
    st.sidebar.page_link("pages/1_Maestro_Dashboard_Agulu_(ETL).py", label="Maestro_Dashboard_Agulu_(ETL)", icon="🪼")
    st.sidebar.page_link("pages/2_Ifem_The_Study_Buddy_(AI).py", label="Ifem_The_Study_Buddy_(AI)", icon="🦩")
    st.sidebar.page_link("pages/3_Nnanna_The_Text_to_SQL_Alchemist_(AI).py", label="Nnanna_The_Text_to_SQL_Alchemist_(AI)", icon="🐿")
    st.divider()

st.sidebar.success("Select any of the links above")
st.write("〈〈 Navigate some of my projects using the sidebar.")

a,x,b = st.columns([0.5,0.1,0.3])
with a:
    # a.title(f"The Data Artist 🦧", anchor=False)
    # st.write("\n")
    a.title(":blue[Benjamin Okeke]")
    a.write(
        """
        Full-stack data engineer assisting enterprises in 
        automating workflows and supporting scalable data ecosystems.
        <br>:grey[BSc  |  ITIL  |  Databricks Lakehouse Fundamentals Accreditation    <br>Microsoft Certified Azure Cloud Fundamentals]
        <br>nnaemeka.okeke@gmail.com
        """,unsafe_allow_html=True
    )
    st.write("\n")
    st.subheader(":blue[Experience and Qualifications]", anchor=False)
    st.write(
        """
        Strong hands-on experience in Python and SQL
        \n3 years experience building ETL pipelines and visualizing data.
        \nCollaborative and agile team player with a passion for continuous learning.
        """, unsafe_allow_html=True
    )
    st.write("\n")
    st.subheader(":blue[Hard Skills]")
    st.write(
        """
        **Programming**: Python(Pandas,Plotly,Streamlit), SQL, PySpark, Bash.
        \n**Databases**: RDBMS(MySQL,Postgres, SQLServer), NoSQL(Couchdb, MongoDB)
        \n**AI Tools**: OpenAI API, Hugging Face, Langchain
        \n**Data Visualization**: PowerBI, Streamlit, Plotly
        \n**Cloud**: AWS, GCP, and Azure.
        """, unsafe_allow_html=True
    )

with b:
    st.metric(label=f"{city} - Temperature - {time_}", value=f"{temp}°c", border=True, height=120)
    st.metric(label=f"{city} - {time_}", value=f"{remark}", border=True, height=120)
    st.metric(label=f"{today} - Bank of Canada Rate - USD/CAD", value=f"{fx_val}", border=True, height=160)
    with st.container(border=True, height=250):
        # source, author, title, description, url = helpers.get_news_article()
        source_b = f"Forbes"
        author_b = f"ByDerek Newton"
        title_b = f"National Test Scores Are Down, Is Generative AI Partly To Blame?"
        description_b = (f"American middle and high school students are not performing well on assessments, "
                       f"indicating that fewer of them are learning even as much as their peers were "
                       f"just a handful of years ago.")
        url_b = (f"https://www.forbes.com/sites/dereknewton/2025/09/27/national-test-scores-are-down-is-generative-ai"
               f"-partly-to-blame/")


        st.write(f"""
        **Latest from around the world:**
        <br>:grey[{title_b}]
        <br>:grey[by {author_b}]
        <br>{description_b}
        <br>{url_b}
        """, unsafe_allow_html=True)

st.markdown("---")
boc_df = pd.read_sql_table("boc_fx", con=conn)
fx_val = boc_df["value"].head(1).tolist()[0]
# l_col, r_col = st.columns(2)
# with r_col:
#     # st.markdown("Daily BOC Rate Figures")
#     fig = px.line(boc_df, x=boc_df["date"], y=boc_df["value"], title="Bank Of Canada FX Rate", height=400)
#     st.plotly_chart(fig, use_container_width=True )


weather_tab = pd.read_sql_table("weather", con=conn)


st.dataframe(weather_tab, height=150, width=800, hide_index=True)
st.write("\n")


