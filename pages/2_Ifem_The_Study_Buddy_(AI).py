import glob
import streamlit as st
from extractor import DocumentProcessor
from transformer import EmbeddingIndexer
from rag_to_riches import RagChain
from chatbot import Chatbot
from pathlib import Path
import os


st.title("Ifem The Study Buddy 🦩")
st.write("")

st.set_page_config(
    layout="centered",
)

with st.sidebar:
    st.sidebar.page_link("app.py", label="Home", icon="🦧")
    st.sidebar.page_link("pages/1_Maestro_Dashboard_Agulu_(ETL).py", label="Maestro_Dashboard_Agulu_(ETL)", icon="🪼")
    st.sidebar.page_link("pages/2_Ifem_The_Study_Buddy_(AI).py", label="Ifem_The_Study_Buddy_(AI)", icon="🦩")
    st.sidebar.page_link("pages/3_Nnanna_The_Text_to_SQL_Alchemist_(AI).py", label="Nnanna_The_Text_to_SQL_Alchemist_(AI)", icon="🐿")
    st.divider()

@st.cache_resource
def initialize_chatbot(file_path):
    processor = DocumentProcessor(file_path)
    contents = processor.load_and_split()
    indexer = EmbeddingIndexer()
    vectorstore = indexer.create_vectorstore(contents)
    rag_chain = RagChain(vectorstore)
    return Chatbot(rag_chain.create_chain())


uploaded_file = st.file_uploader("Please upload a text file to interact with Ifem", type=["txt"])

if uploaded_file:
    with open("temp_file.txt", "wb") as f:
        f.write(uploaded_file.getbuffer())

    chatbot = initialize_chatbot("temp_file.txt")

    if "messages" not in st.session_state:
        st.session_state.messages = []

    for message in st.session_state.messages:
        st.chat_message(message["role"]).markdown(message["content"])

    if prompt := st.chat_input("Ask a question"):
        st.chat_message("user").markdown(prompt)
        st.session_state.messages.append({"role": "user", "content": prompt})

        response = chatbot.get_response(prompt)
        st.chat_message("assistant").markdown(response)
        st.session_state.messages.append({"role": "assistant", "content": response})
else:
    st.write("Please upload a text file...")
