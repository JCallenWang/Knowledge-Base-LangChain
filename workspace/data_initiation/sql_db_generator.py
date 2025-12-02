"""
Module for generating SQLite databases from JSONL files.

This module processes JSON Lines (.jsonl) files, infers the schema from the data,
and populates SQLite databases with the content.
"""

from langchain_community.document_loaders import JSONLoader
from langchain_core.documents import Document

import argparse
import json
import sqlite3
import os
import glob
from typing import List, Any

def _load_processed_data(file_path: str) -> List[Document]:
    """
    Loads and processes JSONL data from a file.

    Args:
        file_path (str): The path to the .jsonl file.

    Returns:
        List[Document]: A list of LangChain Document objects containing the loaded data.
    """
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

def _populate_db_from_docs(cursor: sqlite3.Cursor, docs: List[Document], table_name: str) -> int:
    """
    Creates a table and inserts documents into the SQLite database.

    Args:
        cursor (sqlite3.Cursor): The SQLite database cursor.
        docs (List[Document]): The list of documents to insert.
        table_name (str): The name of the table to create and populate.

    Returns:
        int: The number of records successfully inserted.
    """
    if not docs:
        print(f"No documents to process for table '{table_name}'.")
        return 0

    try:
        # Use the first document to create the table schema
        first_doc_content = json.loads(docs[0].page_content)
        columns = first_doc_content.keys()
        
        # Infer column types
        sql_columns = []
        for col in columns:
            col_type = "TEXT" # Default
            is_numeric = True
            is_integer = True
            
            # Check first few non-null values to infer type
            check_count = 0
            for doc in docs:
                if check_count > 50: break # Check max 50 rows
                try:
                    val = json.loads(doc.page_content).get(col)
                    if val is not None and val != "":
                        check_count += 1
                        try:
                            float_val = float(val)
                            if not float_val.is_integer():
                                is_integer = False
                        except ValueError:
                            is_numeric = False
                            break
                except:
                    pass
            
            if is_numeric and check_count > 0:
                col_type = "INTEGER" if is_integer else "REAL"
            
            sql_columns.append(f'"{col}" {col_type}')

        create_table_query = f"CREATE TABLE IF NOT EXISTS \"{table_name}\" ({', '.join(sql_columns)})"
        cursor.execute(create_table_query)
        print(f"Table '{table_name}' is ready. Schema: {', '.join(sql_columns)}")

        # Insert data from all documents
        for doc in docs:
            try:
                record = json.loads(doc.page_content)
                values = []
                for col in columns:
                    val = record.get(col)
                    if val is None:
                        values.append(None)
                    else:
                        values.append(str(val))
                placeholders = ", ".join(["?"] * len(columns))
                insert_query = f"INSERT INTO \"{table_name}\" ({', '.join(f'\"{c}\"' for c in columns)}) VALUES ({placeholders})"
                cursor.execute(insert_query, values)
            except json.JSONDecodeError:
                print(f"Warning: Skipping a document due to JSON decoding error: {doc.page_content}")
        
        print(f"Successfully inserted {len(docs)} records into '{table_name}'.")
        return len(docs)

    except (json.JSONDecodeError, IndexError) as e:
        print(f"Error processing documents for table '{table_name}': {e}")
        return 0
    except sqlite3.Error as e:
        print(f"An SQLite error occurred while processing table '{table_name}': {e}")
        return 0

def _create_dbs_from_jsonl_files(input_dir: str, output_dir: str) -> None:
    """
    Creates a separate SQLite database for each .jsonl file in a directory.

    Args:
        input_dir (str): The directory containing the .jsonl files.
        output_dir (str): The directory where the .db files will be saved.

    Returns:
        None
    """
    jsonl_files = glob.glob(os.path.join(input_dir, '*.jsonl'))
    if not jsonl_files:
        print(f"No .jsonl files found in '{input_dir}'.")
        return

    os.makedirs(output_dir, exist_ok=True)
    total_records = 0

    for file_path in jsonl_files:
        print(f"\nProcessing file: {file_path}")
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        db_name = os.path.join(output_dir, f"{base_name}.db")

        if os.path.exists(db_name):
            print(f"---Existing database '{db_name}' found, removing it before recreation.---")
            os.remove(db_name)

        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        docs = _load_processed_data(file_path)
        inserted_count = _populate_db_from_docs(cursor, docs, base_name)
        total_records += inserted_count

        conn.commit()
        conn.close()
        print(f"Database '{db_name}' created successfully.")

    print(f"\nAll databases created. Total records inserted across all databases: {total_records}.")

def initialize_database(input_dir: str, output_dir: str) -> None:
    """
    Initializes the database(s) from .jsonl files in the input directory.
    This function creates a separate .db file for each .jsonl file found.

    Args:
        input_dir (str): The directory containing the .jsonl files.
        output_dir (str): The directory where the .db files will be saved.

    Returns:
        None
    """
    if not glob.glob(os.path.join(input_dir, '*.jsonl')):
        print("No processed data files found. Skipping database creation.")
        return

    _create_dbs_from_jsonl_files(input_dir, output_dir)

#if __name__ == "__main__":
#    parser = argparse.ArgumentParser(description="Create and populate a SQLite database from a JSONL file.")
#    parser.add_argument("jsonl_file_path", type=str, help="Path to the input JSONL file.")
#    parser.add_argument("db_name", type=str, help="Name of the SQLite database file to create.")
#    args = parser.parse_args()
#
#    create_and_populate_db(args.jsonl_file_path, args.db_name)
