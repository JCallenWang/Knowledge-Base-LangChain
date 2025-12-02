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

def get_integer_input(prompt_text: str, min_value: int = 1) -> int:
    """
    Prompts the user for an integer input with validation.

    Args:
        prompt_text (str): The text to display in the prompt.
        min_value (int, optional): The minimum acceptable value. Defaults to 1.

    Returns:
        int: The validated integer input from the user.
    """
    while True:
        try:
            prompt = f"please enter {prompt_text} (must >= {min_value}): "
            value = int(input(prompt))

            if value < min_value:
                print(f"invalid input: value must be {min_value} or greater. please enter again.")
                continue

            return value

        except ValueError:
            print("invalid input: value must be valid integer. please enter again.")

def get_excluded_rows_input(sheet_name: str, header_row: int) -> List[Union[int, str]]:
    """
    Prompts the user to specify rows to exclude from the data processing.

    Args:
        sheet_name (str): The name of the sheet being configured.
        header_row (int): The row number where the header ends.

    Returns:
        List[Union[int, str]]: A list of row indices or range strings (e.g., '20-30') to exclude.
    """
    prompt = (
        f"please enter *original rows* that will be excluded in sheet '{sheet_name}' (>{header_row}). "
        f"example: 15,16,20-99 (separate with comma, leave empty if none): "
    )

    while True:
        try:
            input_str = input(prompt).strip()
            if not input_str:
                return []

            rows_to_exclude = []
            raw_parts = [part.strip() for part in input_str.split(',') if part.strip()]

            all_valid_rows = []
            invalid_parts = []

            for part in raw_parts:
                if '-' in part:
                    start, end = map(int, part.split('-'))
                    if start > header_row and end > header_row and start <= end:
                        all_valid_rows.extend(range(start, end + 1))
                        rows_to_exclude.append(part)
                    else:
                        invalid_parts.append(part)
                elif part.isdigit() and int(part) > header_row:
                    all_valid_rows.append(int(part))
                    rows_to_exclude.append(int(part))
                else:
                    invalid_parts.append(part)

            if invalid_parts:
                print(f"invalid input: parts must be integers or ranges (e.g., '20-30') greater than the header row ({header_row}). Invalid parts: {invalid_parts}. please enter again.")
                continue

            return rows_to_exclude

        except ValueError:
            print("invalid input format. please make sure to use integers or valid ranges (e.g., '20-30') separated by commas.")
        except Exception:
            print("invalid input format. please make sure to use ',' to separate integers or ranges only.")


def generate_config(input_file: str, output_config_file: str) -> None:
    """
    Generates a configuration file for the specified input Excel file.

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
        print(f"setting sheet: '{sheet_name}'")

        header_num = get_integer_input(
            f"the *ends row* of Header in '{sheet_name}' (start at 1)", 1
        )

        merge_rows_count = get_integer_input(
            f"the number of Header rows in '{sheet_name} (1 = single row)", 1
        )

        if merge_rows_count > header_num:
            print(f"Warning: the number of Header rows({merge_rows_count}) should <= the ends row of Header. the number of Header rows has set to {header_num}")
            merge_rows_count = header_num

        excluded_rows = get_excluded_rows_input(sheet_name, header_num)

        config_data["sheets"][sheet_name] = {
            "header_row": header_num,
            "merge_rows": merge_rows_count,
            "excluded_rows": excluded_rows
        }
        print(f"settings complete: Header ends in {header_num}, merged {merge_rows_count} rows, excluded rows: {excluded_rows if excluded_rows else 'none'}.")

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
