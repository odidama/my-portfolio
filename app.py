import streamlit as st
import datetime
import random
from PIL import Image
import helpers


eng, conn = helpers.connect_to_db()
db_conn = conn.connect()
today = datetime.date.today()
# time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# weather = helpers.transform_api_response()
weather = helpers.query_duck_db("select * from weather")
temp = weather['temp_f'][0]
city = weather['name'][0]
remark = weather['text'][0]
time_ = weather['last_updated'][0]
boc_df = helpers.query_duck_db("select * from boc_fx")
fx_val = boc_df['value'][0]

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
        min-width: 320px; /* Set a minimum width */
        max-width: 500px; /* Set a maximum width */
        # background-color: #4d4d4d  /*00091a*/;
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
            font-size: 1.2em; /* Adjust this value as needed (e.g., 2em, 48px) */
        }
        </style>
        """,
        unsafe_allow_html=True
    )

st.markdown("""
    <style>
    .block-container {
        padding-top: 1rem; /* Adjust this value as needed */
        padding-bottom: 1rem;
        padding-left: 5rem;
        padding-right: 5rem;
    }
    </style>
    """, unsafe_allow_html=True)

def local_css(filename):
    with open(filename) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

local_css("style/style.css")

with st.sidebar:
    st.sidebar.page_link(page="app.py", label="Home")
    st.sidebar.page_link(page="https://maestro-dashboard.streamlit.app/", label="Maestro Dashboard (ETL)")
    st.sidebar.page_link(page="https://ifem-the-study-buddy.streamlit.app/", label="Study Buddy (AI)")
    st.sidebar.page_link(page="https://geovactext-to-sql.streamlit.app/", label="Text to SQL(AI)")
    st.sidebar.page_link(page="https://real-time-log-analytics.streamlit.app/", label="Log Analytics & Anomaly Detection (ML)" )
    st.divider()

# st.sidebar.write(" **Click any of the links above**")
st.write("〈〈 Navigate some of my projects using the sidebar.")

a,x,b = st.columns([0.5,0.1,0.3])
with a:
    # a.title(f"The Data Artist 🦧", anchor=False)
    # st.write("\n")
    a.title(":blue[Nnaemeka Benjamin Okeke]")
    a.write(":grey[Full-stack data engineer assisting enterprises in automating workflows and designing scalable data ecosystems.]")
    # st.write("\n")
    aa,ab = st.columns([1,1])
    with aa:
        with st.container(width=400, height=260, border=False):
            st.subheader(":blue[Education and Training]", anchor=False)
            st.write(
                """
                Bachelor of Science <br> ITIL V3 <br>  Databricks Lakehouse Fundamentals Accreditation 
                <br> Microsoft Certified Azure Cloud Fundamentals <br> Mastering Databricks & Apache spark - Build ETL data pipeline
                """, unsafe_allow_html=True
            )
    with ab:
        with st.container(width=400, height=250, border=False ):
            image = Image.open("ice.jpeg")
            # img_rsz = image.resize()
            st.image(image, width=250)
    st.subheader(":blue[Summary]", anchor=False)
    st.write(
        """
        * Innovative Data Developer with a strong background in data engineering, analytics, and software development.
        * Adept at designing and implementing scalable data solutions that enhance system reliability and performance.
        * Skilled in Python, SQL, and cloud-based data ecosystems, with hands-on experience in automation, API integration, and data pipeline development.
        * Proven ability to analyze complex problems, drive cross-functional collaboration, and deliver high-quality technical solutions.
        * Experienced in mentoring junior developers, conducting code reviews, and improving team development practices.
        * Passionate about building reliable, high-performance systems that transform raw data into actionable insights.
        """, unsafe_allow_html=True
    )
    st.write("\n")
    st.subheader(":blue[Work Experience]")
    # st.write("\n")
    st.markdown(":blue[**Service Delivery Analyst – Business Intelligence Platform**] - House of Commons, Canada | Dec 2022 – Sept 2025")
    st.write(
        """
        * Participated in architectural planning and implemented end-to-end data solutions aligned with enterprise standards and scalability goals.
        * Translated business requirements into data models, metrics, and dashboards using Power BI, SQL Server, Databricks and Streamlit.
        * Developed and maintained data pipelines using Python, SQL, and Spark, enabling efficient data ingestion, transformation, and validation.
        * Contributed to CI/CD pipelines using Azure DevOps, enhancing the consistency and scalability of analytics deployments.
        * Implemented automation scripts to streamline data ingestion, testing, and deployment processes—reducing manual workload by 35%.
        """, unsafe_allow_html=True
    )
    st.write("")
    st.markdown(":blue[**Application Engineer - Integration & Automation**] - Identos, Canada | Aug 2022 – Dec 2022")
    st.write(
        """
        * Automated log parsing with regular expressions to extract structured data from unformatted logs and error reports.
        * Supported secure digital identity integrations for public sector clients using Azure App Insights, SQL, and Python.
        * Collaborated with DevOps engineers to containerize services and improve scalability in production.
        * Worked closely with the analytics team to design and develop models and dashboards, supporting data-driven decision-making.
        """, unsafe_allow_html=True
    )
    st.write("")
    st.markdown(":blue[**Data and Systems Integration Lead**] - Sherpa, Canada | Nov 2021 – Jul 2022")
    st.write(
        """
        * I helped clients integrate their platforms with Sherpa products and global travel APIs using SDKs and Webhooks.
        * Implemented data validation and pipeline reliability checks, reducing downtime and enhancing delivery accuracy for global travel data systems.
        * Worked with product and engineering teams to map and optimize data ingestion logic for cross-platform analytics.
        * Managed data pipelines interacting with relational databases using SQL and SQLAlchemy ORM.
        """, unsafe_allow_html=True
    )
    st.write("")
    st.markdown(":blue[**Senior Associate - Data Integration**] - Appnovation Technologies, Canada | Feb 2020 – Nov 2021")
    st.write(
        """
        * Led data ingestion and transformation for healthcare analytics systems, enabling targeted insights for digital marketing compliance.
        * Implemented data validation and anomaly detection frameworks using Python, Pytest, Pandas and Redshift to ensure PII-compliant, accurate data delivery.
        * Worked with product and engineering teams to map and optimize data ingestion logic for cross-platform analytics.
        """, unsafe_allow_html=True
    )
    st.write("\n")
    st.subheader(":blue[Skills]")
    st.write(
        """
        Programming : `Python`,`Shell`,`JS`  
        Data Processing : `SQL`,`Pandas`,`Numpy`,`PySpark`
        <br>Data Visualization : `Streamlit`,`Plotly`,`PowerBI`
        <br>Machine Learning : `scikit-learn`
        <br>Web Development : `FastAPI`,`REST`,`HTML`,`CSS`
        """, unsafe_allow_html=True
    )
    st.write("\n")
    st.subheader(":blue[Social Media]")
    st.write(
        """
        `LinkedIn` : https://www.linkedin.com/in/benjaminokeke/  
        `github` : https://github.com/odidama | https://github.com/eluemuno
        <br>`portfolio` : https://benjamin-okeke-portfolio.streamlit.app/
        """, unsafe_allow_html=True
    )
    st.write("\n")
    st.write("nnaemeka.okeke@gmail.com")

with b:
    # st.metric(label=f"{city} - {time_} | BoC Rate - USD/CAD ", value=f"{temp}°c | {fx_val}", border=True, height=120)
    # # st.metric(label=f"{city} - {time_}", value=f"{remark}", border=True, height=120)
    # # st.metric(label=f"{today} - Bank of Canada Rate - USD/CAD", value=f"{fx_val}", border=True, height=160)
    with st.container(border=True, height=430):
        st.subheader(":blue[Please leave a message]")
        st.write("")
        contact_form = """
        <form action="https://formsubmit.co/e984f1204845468c68e1500858661655" method="POST">
        <input type="hidden" name="_captcha"  value="false">
        <input type="text" name="Your Name" placeholder="Your name or contact" required>
        <input type="email" name="Your email" placeholder="Your email address" required>
        <textarea name="message" placeholder="Leave a message"></textarea>
        <button type="submit">Send</button>
        </form>
        """
        st.markdown(contact_form, unsafe_allow_html=True)

    with st.container(border=False, height=350,vertical_alignment='center'):
        st.subheader(":blue[Core Skills]")
        st.write(
            """
            * Data Engineering & ETL Development
            * Data Modeling & Analytics
            * Solution Architecture & System Integration
            * Automation & Workflow Optimization
            * Cloud Platforms
            * CI/CD, Git, Docker
            * Agile & DevOps Practices
            * Testing & Quality Assurance
            * Technical Documentation & Mentorship
            """, unsafe_allow_html=True
        )








    #     # news_dict_list = helpers.get_items_from_redis("news_articles")
    #     # single_article = news_dict_list[random.randint(1, len(news_dict_list))]
    #
    #     single_article = helpers.query_duck_db("select * from news USING SAMPLE 1 ROWS")
    #
    #     author_b = single_article['news_author'][0]
    #     title_b = single_article['news_title'][0]
    #     url_b = single_article['news_url'][0]
    #
    #
    #     st.write(f"""
    #     **Latest from around the world:**
    #     <br>:grey[{title_b}]
    #     <br>:grey[by {author_b}]
    #     <br>{url_b}
    #     """, unsafe_allow_html=True)
# st.markdown("---")
# boc_df = pd.read_sql_table("boc_fx", con=conn)
# fx_val = boc_df["value"].head(1).tolist()[0]
# # l_col, r_col = st.columns(2)
# # with r_col:
# #     # st.markdown("Daily BOC Rate Figures")
# #     fig = px.line(boc_df, x=boc_df["date"], y=boc_df["value"], title="Bank Of Canada FX Rate", height=400)
# #     st.plotly_chart(fig, use_container_width=True )
#
#
# weather_tab = pd.read_sql_table("weather", con=conn)
#
#
# st.dataframe(weather_tab, height=150, width=800, hide_index=True)
st.write("\n")
st.write("\n")
st.write("\n")


