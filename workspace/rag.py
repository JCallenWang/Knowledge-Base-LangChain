from langchain_community.document_loaders import JSONLoader
from langchain_core.documents import Document

import argparse
import json
import sqlite3
import hashlib
import os

CHUNK_SIZE = 10
DATA_NAME = './preprocess/result/clean_random_p1_事務機.jsonl'
DATABASE_NAME = 'xlsx_jsonl_data_p1.db'
HASH_FILE_PATH = 'xlsx_jsonl_data_p1_hash.txt'
    

def needs_update(data, hash_file_path):
    hasher = hashlib.md5()
    hasher.update(str(data).encode('utf-8'))
    new_hash = hasher.hexdigest()

    if not os.path.exists(hash_file_path):
        return True, new_hash
    
    with open(hash_file_path, "r") as f:
        existing_hash = f.read().strip()
    return new_hash != existing_hash, new_hash

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

def create_and_populate_db(file_path: str, db_name: str):
    """
    Creates a SQLite database and populates it with data from a list of Document objects.

    Args:
        docs (list[Document]): A list of documents, where each document's page_content is a JSON string.
        db_name (str): The name of the SQLite database file to create.
    """
    docs = load_processed_data(file_path)
    if not docs:
        print("No documents to process.")
        return
    
    update_required, new_hash = needs_update(docs, HASH_FILE_PATH)
    if not update_required:
        print("---loading same database, no need to update---")
        return
    else:
        print("---data update required, remove existing database---")
        with open(HASH_FILE_PATH, "w") as f:
            f.write(new_hash)
        if os.path.exists(db_name):
            os.remove(db_name)

    # Establish a connection to the SQLite database
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Use the first document to create the table schema
    first_doc_content = json.loads(docs[0].page_content)
    columns = first_doc_content.keys()

    # Sanitize column names for SQL (e.g., replace spaces with underscores)
    sql_columns = [f'"{col}" TEXT' for col in columns]

    # Create the table
    table_name = "data"
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(sql_columns)})"
    cursor.execute(create_table_query)
    print(f"Database '{db_name}' and table '{table_name}' are ready.")

    # Insert data from all documents
    for doc in docs:
        try:
            record = json.loads(doc.page_content)
            values = [str(record.get(col, "")) for col in columns]
            placeholders = ", ".join(["?"] * len(columns))
            insert_query = f"INSERT INTO {table_name} ({', '.join(f'\"{c}\"' for c in columns)}) VALUES ({placeholders})"
            cursor.execute(insert_query, values)
        except json.JSONDecodeError:
            print(f"Warning: Skipping a document due to JSON decoding error: {doc.page_content}")


    # Commit the changes and close the connection
    conn.commit()
    conn.close()
    print(f"Successfully inserted {len(docs)} records into '{table_name}'.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("jsonl_file_path", type=str, help="Path to the input JSONL file.")

    args = parser.parse_args()
    create_and_populate_db(args.jsonl_file_path, DATABASE_NAME)

    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    sql_commands = "SELECT COUNT(*) FROM data;"
    cursor.execute(sql_commands)
    count = cursor.fetchone()[0]
    print(f"Total records in 'data' table: {count}")

