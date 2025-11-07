from langchain_community.document_loaders import JSONLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.documents import Document

import argparse

CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200
ADD_START_INDEX = True



def load_processed_data(file_path) -> list[Document]:
    loader = JSONLoader(
        file_path=file_path,
        jq_schema='.', # Process each JSONL object (line)
        json_lines=True, # Important: specifies this is a JSONL file
        text_content=False, # We will construct content manually if needed, or let it use the whole JSON
    )
    docs = loader.load()
    return docs

def split_excel_data(docs) -> list[Document]:
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP, add_start_index=ADD_START_INDEX
    )
    splits = splitter.split_documents(docs)
    return splits



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("jsonl_file_path", type=str, help="Path to the input JSONL file.")

    args = parser.parse_args()
    docs = load_processed_data(args.jsonl_file_path)
    print(docs)
    #splits = split_excel_data(format_docs(docs))
    #print(format_docs(splits[0]))
