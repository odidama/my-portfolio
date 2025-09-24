from dotenv import load_dotenv
from pathlib import Path
import os

load_dotenv()
curr_dir = Path(__file__).parent
f_path = curr_dir / 'file_repo'

class Chatbot:
    def __init__(self, qa_chain):
        self.qa_chain = qa_chain

    def get_response(self, user_input):
        try:
            response = self.qa_chain({"query": user_input})
            return response['result']
        except Exception as e:
            return f"An error occured: {str(e)}"


if __name__ == "__main__":
    from rag_to_riches import RagChain
    from extractor import DocumentProcessor
    from transformer import EmbeddingIndexer

    processor = DocumentProcessor(f_path)
    contents = processor.load_and_split()

    indexer = EmbeddingIndexer()
    vectorstore = indexer.create_vectorstore(contents)

    rag_chain = RagChain(vectorstore)
    qa_chain = rag_chain.create_chain()

    chatbot = Chatbot(qa_chain)

    while True:
        user_input = input("You: ")
        if user_input.lower() in ['exit', 'quit', 'bye']:
            print("Geovac: Goodbye!")
            break
        response = chatbot.get_response(user_input)
        print(f"Geovac: {response}")