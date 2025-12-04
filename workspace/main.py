"""
Main entry point for the Excel to SQL Agent workflow.

This module coordinates the entire process of converting an Excel file to a SQL database
and starting an interactive SQL agent to query the data.
"""

import os
import sys
import shutil
import hashlib
import glob
import argparse

from data_preprocessing.config_generator import generate_config
from data_preprocessing.data_processor import load_dataframes_from_config
from data_initiation.sql_db_generator import create_dbs_from_dataframes

from agents.sql_agent import start_sql_agent

#ADDITIONAL_DESCRIPTION = """
#    請注意 :
#    - '姓氏' 欄位實際代表「單位／處室名稱」，並非人名，請以單位為統計依據，若為空值，請在統計時獨立歸類為「未知單位」。
#    - '彩色頁面' 欄位代表列印的彩色頁面數量。
#    - '黑白頁面' 欄位代表列印的黑白頁面數量。
#    """  
ADDITIONAL_DESCRIPTION = ""

def get_file_hash(file_path: str) -> str:
    """
    Computes the SHA256 hash of a file.

    Args:
        file_path (str): The path to the file to hash.

    Returns:
        str: The hexadecimal SHA256 hash of the file content.
    """
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

def xlsx_to_sql_init(source_file: str, header_mode: str = "row") -> str:
    """
    Converts an Excel file to a SQL database through a series of processing steps.

    This function handles:
    1. Hashing the file to create a unique workspace.
    2. Generating a configuration file for the Excel data (automatically).
    3. Processing the Excel data directly into DataFrames.
    4. Creating SQLite databases from the DataFrames.

    Args:
        source_file (str): The path to the source .xlsx file.
        header_mode (str): "row" or "column". Defaults to "row".

    Returns:
        str: The path to the directory containing the generated database(s).
    """
    if not os.path.exists(source_file) or not source_file.endswith('.xlsx'):
        print(f"Error: File '{source_file}' is not a valid .xlsx file.")
        sys.exit(1)

    file_hash = get_file_hash(source_file)
    base_name = os.path.splitext(os.path.basename(source_file))[0]

    root_output_dir = f"./{base_name}_{file_hash[:8]}"
    source_files_dir = os.path.join(root_output_dir, "source")
    config_files_dir = os.path.join(root_output_dir, "config")
    # processed_data_dir is no longer needed in the simplified flow
    database_dir = os.path.join(root_output_dir, "database")

    if os.path.isdir(database_dir) and glob.glob(os.path.join(database_dir, '*.db')):
        print(f"Database files already exist in '{database_dir}'. Skipping generation.")
        return database_dir
    else:
        print(f"Artifacts will be saved in: '{root_output_dir}'")

    os.makedirs(root_output_dir, exist_ok=True)
    os.makedirs(source_files_dir, exist_ok=True)
    os.makedirs(config_files_dir, exist_ok=True)
    os.makedirs(database_dir, exist_ok=True)

    config_file = os.path.join(config_files_dir, f"{base_name}_config.json")

    staged_source_file = os.path.join(source_files_dir, os.path.basename(source_file))
    if os.path.abspath(source_file) != os.path.abspath(staged_source_file):
        shutil.copy(source_file, staged_source_file)
    else:
        print(f"Source file is already in the target directory: {staged_source_file}")

    print("\nStep 1: Generating configuration file (Auto-Detection)...")
    generate_config(staged_source_file, config_file, header_mode)

    print("\nStep 2: Loading data into DataFrames...")
    dfs = load_dataframes_from_config(config_file)

    print("\nStep 3: Creating SQL databases from DataFrames...")
    create_dbs_from_dataframes(dfs, database_dir)

    print(f"\nWorkflow complete! All artifacts are located in '{root_output_dir}'.")
    return database_dir

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process an .xlsx file to create a SQL database and then start an interactive SQL agent.")
    parser.add_argument("input_file", help="The name of the input source file (.xlsx).")
    args = parser.parse_args()

    db_output_path = xlsx_to_sql_init(args.input_file)
    start_sql_agent(db_output_path, ADDITIONAL_DESCRIPTION)
