import os
import sys
import shutil
import hashlib
import argparse

from data_preprocessing.config_generator import generate_config
from data_preprocessing.data_processor import process_data_from_config
from data_initiation.sql_db_generator import create_and_populate_db

from agents.sql_agent import start_sql_agent

SCHEMA_DESCRIPTION = """
    請注意 :
    - '姓氏' 欄位實際代表「單位／處室名稱」，並非人名，請以單位為統計依據，若為空值，請在統計時獨立歸類為「未知單位」。
    - '彩色頁面' 欄位代表列印的彩色頁面數量。
    - '黑白頁面' 欄位代表列印的黑白頁面數量。
    """  

def get_file_hash(file_path):
    """Computes the SHA256 hash of a file."""
    hasher = hashlib.sha256()
    try:
        with open(file_path, 'rb') as f:
            while chunk := f.read(8192):
                hasher.update(chunk)
        return hasher.hexdigest()
    except FileNotFoundError:
        print(f"Error: Source file not found at '{file_path}'")
        sys.exit(1)
    except Exception as e:
        print(f"An error occurred while hashing the file: {e}")
        sys.exit(1)

def xlsx_to_sql_init(source_file):
    if not os.path.exists(source_file) or not source_file.endswith('.xlsx'):
        print(f"Error: File '{source_file}' is not a valid .xlsx file.")
        sys.exit(1)

    file_hash = get_file_hash(source_file)
    base_name = os.path.splitext(os.path.basename(source_file))[0]
    root_output_dir = f"./{base_name}_{file_hash[:8]}"
    os.makedirs(root_output_dir, exist_ok=True)

    # Define subdirectories for better organization
    source_files_dir = os.path.join(root_output_dir, "source")
    config_files_dir = os.path.join(root_output_dir, "config")
    processed_data_dir = os.path.join(root_output_dir, "processed_data")
    database_dir = os.path.join(root_output_dir, "database")

    # Create subdirectories
    os.makedirs(source_files_dir, exist_ok=True)
    os.makedirs(config_files_dir, exist_ok=True)
    os.makedirs(processed_data_dir, exist_ok=True)
    os.makedirs(database_dir, exist_ok=True)

    config_file = os.path.join(config_files_dir, f"{base_name}_config.json")
    db_file = os.path.join(database_dir, f"{base_name}.db")

    if os.path.exists(db_file):
        print(f"database file already exists: '{db_file}'.")
        return db_file
    else:
        print(f"Artifacts will be saved in: '{root_output_dir}'")


    # Update file paths to use the new subdirectories
    staged_source_file = os.path.join(source_files_dir, os.path.basename(source_file))
    shutil.copy(source_file, staged_source_file)

    print("\nStep 1: Generating configuration file...")
    generate_config(staged_source_file, config_file)

    print("\nStep 2: Processing data and converting to JSONL...")
    process_data_from_config(config_file, processed_data_dir)

    print("\nStep 3: Initializing SQL database from JSONL files...")
    create_and_populate_db(processed_data_dir, db_file)

    print(f"\nWorkflow complete! All artifacts and description file are located in '{root_output_dir}'.")
    return db_file

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert .xlsx into sql database.")
    parser = argparse.ArgumentParser(description="Process an .xlsx file to create a SQL database and then start an interactive SQL agent.")
    parser.add_argument("input_file", help="The name of the input source file (.xlsx).")
    args = parser.parse_args()
    db_path = xlsx_to_sql_init(args.input_file)

    start_sql_agent(db_path, SCHEMA_DESCRIPTION)