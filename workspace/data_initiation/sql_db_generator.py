from langchain_community.document_loaders import JSONLoader
from langchain_core.documents import Document

import argparse
import json
import sqlite3
import os
import glob

def load_processed_data(file_path: str) -> list[Document]:
    loader = JSONLoader(
        file_path=file_path,
        jq_schema='.', # Process each JSONL object (line)
        json_lines=True, # load & split by each json-line
        text_content=False,
    )
    docs = loader.load()
    readable_docs = []
    for doc in docs:
        try:
            ori_json = json.loads(doc.page_content)
            # Decode JSON string to Python dict, then dump back as string with readable Chinese
            readable_content = json.dumps(ori_json, ensure_ascii=False)
        except Exception:
            # Fallback: keep original if not valid JSON
            readable_content = doc.page_content
        readable_docs.append(
            Document(page_content=readable_content, metadata=doc.metadata)
        )
    return readable_docs

def create_and_populate_db(input_dir: str, db_name: str):
    """
    Creates a SQLite database and populates it with data from a list of Document objects.

    Args:
        input_dir (str): The directory containing the .jsonl files.
        db_name (str): The name of the SQLite database file to create.
    """
    jsonl_files = glob.glob(os.path.join(input_dir, '*.jsonl'))
    if not jsonl_files:
        print(f"No .jsonl files found in '{input_dir}'.")
        return
    
    # Create a hash of all file contents to check for updates
    if os.path.exists(db_name):
        print(f"---Existing database '{db_name}' found, removing it before recreation.---")
        os.remove(db_name)

    # Establish a connection to the SQLite database
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    total_records = 0
    for file_path in jsonl_files:
        print(f"\nProcessing file: {file_path}")
        docs = load_processed_data(file_path)
        if not docs:
            print(f"No documents to process in '{file_path}'.")
            continue

        # Use the first document to create the table schema
        first_doc_content = json.loads(docs[0].page_content)
        columns = first_doc_content.keys()
        sql_columns = [f'"{col}" TEXT' for col in columns]

        # Derive table name from the JSONL filename
        table_name = os.path.splitext(os.path.basename(file_path))[0]
        create_table_query = f"CREATE TABLE IF NOT EXISTS \"{table_name}\" ({', '.join(sql_columns)})"
        cursor.execute(create_table_query)
        print(f"Table '{table_name}' is ready in database '{db_name}'.")

        # Insert data from all documents in the current file
        for doc in docs:
            try:
                record = json.loads(doc.page_content)
                values = [str(record.get(col, "")) for col in columns]
                placeholders = ", ".join(["?"] * len(columns))
                insert_query = f"INSERT INTO \"{table_name}\" ({', '.join(f'\"{c}\"' for c in columns)}) VALUES ({placeholders})"
                cursor.execute(insert_query, values)
            except json.JSONDecodeError:
                print(f"Warning: Skipping a document due to JSON decoding error: {doc.page_content}")
        total_records += len(docs)
        print(f"Successfully inserted {len(docs)} records into '{table_name}'.")

    conn.commit()
    conn.close()
    print(f"\nDatabase population complete. Total records inserted: {total_records}.")


#if __name__ == "__main__":
#    parser = argparse.ArgumentParser(description="Create and populate a SQLite database from a JSONL file.")
#    parser.add_argument("jsonl_file_path", type=str, help="Path to the input JSONL file.")
#    parser.add_argument("db_name", type=str, help="Name of the SQLite database file to create.")
#    args = parser.parse_args()
#
#    create_and_populate_db(args.jsonl_file_path, args.db_name)