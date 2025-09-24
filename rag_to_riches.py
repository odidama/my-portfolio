from langchain.chains.retrieval_qa.base import RetrievalQA
from langchain_google_genai import ChatGoogleGenerativeAI
from dotenv import load_dotenv
import os
import streamlit as st
from pathlib import Path

fpath = Path(__file__)
curr_dir = Path(__file__).parent
f_path = curr_dir / 'file_repo'

load_dotenv()


class RagChain:
    def __init__(self, vectorestore):
        self.vectorstore = vectorestore
        self.llm = self.get_llm()

    def get_llm(self):
        if os.getenv("GOOGLE_API_KEY"):
            return ChatGoogleGenerativeAI(model="gemini-1.5-flash", max_retries=2,
                                          api_key=st.secrets["GOOGLE_API_KEY"], temperature=0)
                                          # api_key=os.getenv("GOOGLE_API_KEY"), temperature=0)
        else:
            raise ValueError("No valid API key found! Please set on in .env file.")

    def create_chain(self):
        retriever = self.vectorstore.as_retriever(search_kwargs={"k": 3})
        qa_chain = RetrievalQA.from_chain_type(
            llm=self.llm,
            chain_type="stuff",
            retriever=retriever,
            return_source_documents=True
        )
        return qa_chain


if __name__ == "__main__":
    from extractor import DocumentProcessor
    from transformer import EmbeddingIndexer

    processor = DocumentProcessor(f_path)
    contents = processor.load_and_split()

    indexer = EmbeddingIndexer()
    vectorstore = indexer.create_vectorstore(contents)

    rag_chain = RagChain(vectorstore)
    qa_chain = rag_chain.create_chain()

    query = input("What is REVOCATION CLAUSE ?")
    result = qa_chain({"query": query})
    print(f"Answer: {result['result']}")