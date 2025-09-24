from langchain.embeddings import HuggingFaceEmbeddings
from langchain.vectorstores import FAISS
from dotenv import load_dotenv
from pathlib import Path
import os

load_dotenv()
curr_dir = Path(__file__).parent
f_path = curr_dir / 'file_repo'


class EmbeddingIndexer:
    def __init__(self):
        self.embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")

    def create_vectorstore(self, doc_contents):
        vectorstore = FAISS.from_documents(doc_contents, self.embeddings)
        return vectorstore


if __name__ == "__main__":
    from extractor import DocumentProcessor

    processor = DocumentProcessor(f_path)
    contents = processor.load_and_split()

    indexer = EmbeddingIndexer()
    vectorstore = indexer.create_vectorstore(contents)
    print("Vector store has been created successfully")