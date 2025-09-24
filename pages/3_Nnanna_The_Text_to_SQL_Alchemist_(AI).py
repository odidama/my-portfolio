import os
import streamlit as st
import helpers
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.messages import AIMessage, HumanMessage
from langchain_community.utilities import SQLDatabase
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_huggingface import ChatHuggingFace, HuggingFaceEndpoint, HuggingFacePipeline
from dotenv import load_dotenv

load_dotenv()

st.title("Nnanna The Text-to-SQL Alchemist 🦫")
st.write("")


# st.title("Hi! I'm nnanna, happy to help...")
st.markdown("Only sample Customers and Persons data are currently available. Sample questions include: Are "
            "there any customers from Canada? Do we have any customers named Emeka from Malaysia? etc.")
with st.sidebar:
    st.sidebar.page_link("app.py", label="Home", icon="🦧")
    st.sidebar.page_link("pages/1_Maestro_Dashboard_Agulu_(ETL).py", label="Maestro_Dashboard_Agulu_(ETL)", icon="🪼")
    st.sidebar.page_link("pages/2_Ifem_The_Study_Buddy_(AI).py", label="Ifem_The_Study_Buddy_(AI)", icon="🦩")
    st.sidebar.page_link("pages/3_Nnanna_The_Text_to_SQL_Alchemist_(AI).py", label="Nnanna_The_Text_to_SQL_Alchemist_(AI)", icon="🐿")
    st.divider()

engine, db_conn = helpers.connect_to_db()
db = db_conn.connect()


def get_db_schema(_):
    return engine.get_table_info()


def run_sql_query(query):
    print(f"Running SQL Query: {query} \n\n")
    return engine.run(query)


def initiate_llm(load_from_hugging_face=False):
    if load_from_hugging_face:
        llm = HuggingFaceEndpoint(
            repo_id="meta-llama/Llama-3.1-8B-Instruct",
            huggingfacehub_api_token=os.getenv("HUGGINGFACE_API_KEY"),
            task="text-generation",
            temperature=0.7,
            max_new_tokens=200
        )
        return ChatHuggingFace(llm=llm)
    # return ChatGoogleGenerativeAI(model="gemini-2.5-flash", api_key=os.getenv("GOOGLE_API_KEY"), temperature=0)
    return ChatGoogleGenerativeAI(model="gemini-2.5-flash", api_key=st.secrets["GOOGLE_API_KEY"], temperature=0)


def write_sql_query(llm):
    template = """You are an expert in SQL query generation. Based on the table schema below, write an SQL query that 
    would answer the user's question: {schema}

    Question: {question}
    SQL Query:
    """

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", "Given an input question, convert it to SQL query. No preamble"
                       "Do not return anything else apart from the SQL query. No prefix or suffix, no SQL keywords"
             ),
            ("user", template),
        ]

    )
    return (
            RunnablePassthrough.assign(schema=get_db_schema)
            | prompt
            | llm
            | StrOutputParser()
    )


def process_user_query(query, llm):
    template = """
    Based on the table schema below, question, sql query and sql response, write a natural language response:
    {schema}

    Question: {question}
    SQL Query: {query}
    SQL Response: {response}
    """

    prompt_response = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                "Given an input question and SQL response, convert it to a natural language response.",
            ),
            ("user", template)

        ]
    )
    full_chain = (
            RunnablePassthrough.assign(query=write_sql_query(llm))
            | RunnablePassthrough.assign(schema=get_db_schema, response=lambda x: run_sql_query(x["query"]), )
            | prompt_response
            | llm
    )
    return full_chain.invoke({"question": query})


if "chart_history" not in st.session_state:
    st.session_state.chart_history = [
        AIMessage(content="Hello! I'm Nnanna, your SQL assistant. Ask me questions about your data"),
    ]

with st.sidebar:
    # default_db = st.secrets["NEON_DATABASE_URL"]
    st.subheader("Settings")
    st.write("This is an ai chat application that interacts with a cloud DB and analyzes data through Natural "
             "Language. Login and experiment! ")
    st.text_input("Host:", value="odidama.agulu:anambra", disabled=True, key="Host")
    # st.text_input("Port:", value="1234", disabled=True, key="Port")
    # st.text_input("User:", value="You", disabled=True, key="User")
    st.text_input("Password:", value="********", disabled=True, key="Password")
    st.text_input("Database:", value="geovac", disabled=True, key="Database")

    if st.button("Login"):
        with st.spinner("Connecting to DB..."):
            # db = connect_to_db()
            st.session_state.db = db
            st.success("Connected to NeonDB!")

for message in st.session_state.chart_history:
    if isinstance(message, AIMessage):
        with st.chat_message("AI"):
            st.markdown(message.content)
    elif isinstance(message, HumanMessage):
        with st.chat_message("Human"):
            st.markdown(message.content)

query = st.chat_input("Type your message here...")
if query is not None and query != "":
    st.session_state.chart_history.append(HumanMessage(content=query))

    with st.chat_message("Human"):
        st.markdown(query)

    with st.chat_message("AI"):
        response = process_user_query(query, llm=initiate_llm(load_from_hugging_face=False))
        # response = process_user_query(query, llm=initiate_llm(load_from_hugging_face=True))
        st.markdown(response.content)

    st.session_state.chart_history.append(AIMessage(content=response.content))