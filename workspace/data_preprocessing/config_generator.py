"""
Module for generating configuration files for data processing.

This module analyzes Excel files to extract sheet information and prompts the user
to define header structures and excluded rows, saving the configuration to a JSON file.
"""

import pandas as pd
import sys
import os
import json
import argparse
from typing import List, Union

def get_sheet_info(input_file: str) -> List[str]:
    """
    Retrieves the list of sheet names from an Excel file.

    Args:
        input_file (str): The path to the Excel file.

    Returns:
        List[str]: A list of sheet names found in the Excel file.

    Raises:
        ValueError: If the file format is not supported (not .xlsx or .xls).
    """
    file_extension = os.path.splitext(input_file)[1].lower()

    if file_extension in ['.xlsx', '.xls']:
        try:
            xls = pd.ExcelFile(input_file, engine='openpyxl')
            sheet_names = xls.sheet_names
            return sheet_names
        except Exception as e:
            print(f"failed to load Excel or sheet information: {e}")
            sys.exit(1)

    else:
        raise ValueError(f"unsupported data format: {file_extension}. only support .xlsx or .xls.")

def detect_header_row(input_file: str, sheet_name: str, max_scan_rows: int = 20) -> int:
    """
    Automatically detects the header row by finding the row with the most non-empty columns.

    Args:
        input_file (str): The path to the Excel file.
        sheet_name (str): The name of the sheet to analyze.
        max_scan_rows (int): The maximum number of rows to scan.

    Returns:
        int: The 1-based row number of the detected header.
    """
    try:
        # Read first few rows without header to analyze structure
        df = pd.read_excel(input_file, sheet_name=sheet_name, header=None, nrows=max_scan_rows, engine='openpyxl')
        
        max_non_null = -1
        header_idx = 0
        
        for idx, row in df.iterrows():
            # Count non-null values
            non_null_count = row.count()
            if non_null_count > max_non_null:
                max_non_null = non_null_count
                header_idx = idx
        
        # Return 1-based index
        return header_idx + 1
        
    except Exception as e:
        print(f"Warning: Could not auto-detect header for sheet '{sheet_name}': {e}. Defaulting to row 1.")
        return 1

def generate_config(input_file: str, output_config_file: str) -> None:
    """
    Generates a configuration file for the specified input Excel file using automatic detection.

    Args:
        input_file (str): The path to the input Excel file.
        output_config_file (str): The path where the generated JSON configuration will be saved.

    Returns:
        None
    """
    print(f"--- start generating config file of '{input_file}' ---")

    try:
        sheet_names = get_sheet_info(input_file)
        print(f"detect {len(sheet_names)} sheet(s).")
    except FileNotFoundError:
        print(f"Error: cannot find file'{input_file}'.")
        return
    except ValueError as e:
        print(f"Error: {e}")
        return

    config_data = {
        "input_file": input_file,
        "sheets": {}
    }

    for sheet_name in sheet_names:
        print("-" * 40)
        print(f"analyzing sheet: '{sheet_name}'")

        # Auto-detect header
        header_num = detect_header_row(input_file, sheet_name)
        
        # Default values for simplified flow
        merge_rows_count = 1
        excluded_rows = []

        config_data["sheets"][sheet_name] = {
            "header_row": header_num,
            "merge_rows": merge_rows_count,
            "excluded_rows": excluded_rows
        }
        print(f"auto-detection complete: Header at row {header_num}.")

    try:
        with open(output_config_file, 'w', encoding='utf-8') as f:
            json.dump(config_data, f, ensure_ascii=False, indent=4)

        print("\n" + "=" * 50)
        print(f"config generate successfully! please check '{output_config_file}'.")
        print("=" * 50)

    except Exception as e:
        print(f"Error while writing config file:{e}")

#if __name__ == '__main__':
#    parser = argparse.ArgumentParser(description="Generate a configuration file for data processing.")
#    parser.add_argument("input_file", help="The name of the input source file (.xlsx).")
#    parser.add_argument("config_file", help="The name of the output config file.")
#    args = parser.parse_args()
#
#    generate_config(args.input_file, args.config_file)
