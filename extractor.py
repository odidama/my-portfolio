from langchain.document_loaders import TextLoader
from langchain.text_splitter import CharacterTextSplitter
from dotenv import load_dotenv
import os
from pathlib import Path

load_dotenv()
curr_dir = Path(__file__).parent
f_path = curr_dir / 'file_repo'


class DocumentProcessor:
    def __init__(self, file_path):
        self.file_path = file_path

    def load_and_split(self):
        loader = TextLoader(self.file_path)
        docs = loader.load()
        splitter = CharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
        content = splitter.split_documents(docs)
        return content


if __name__ == "__main__":
    processor = DocumentProcessor(f_path)
    content = processor.load_and_split()
    print("The total number of chunks is: ", len(content))