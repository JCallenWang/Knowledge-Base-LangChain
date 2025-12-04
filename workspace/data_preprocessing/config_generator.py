import pandas as pd
import sys
import os
import json
import argparse
import datetime
from typing import List, Union, Tuple

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

def detect_header_and_merge_count(input_file: str, sheet_name: str, max_scan_rows: int = 20) -> Tuple[int, int]:
    """
    Automatically detects the header row and the number of rows to merge.
    
    Strategy:
    1. Identify the first row that looks like "Data" (mostly numeric or dates).
    2. The row immediately preceding the first data row is the "Header End Row".
    3. All rows from the top down to the "Header End Row" are considered part of the header (merged).
    4. Fallback: If no data row is found (e.g., all text), use the "Max Non-Nulls" strategy.

    Args:
        input_file (str): The path to the Excel file.
        sheet_name (str): The name of the sheet to analyze.
        max_scan_rows (int): The maximum number of rows to scan.

    Returns:
        Tuple[int, int]: (header_row_number (1-based), merge_rows_count)
    """
    try:
        # Read first few rows without header to analyze structure
        df = pd.read_excel(input_file, sheet_name=sheet_name, header=None, nrows=max_scan_rows, engine='openpyxl')
        
        if df.empty:
            return 1, 1

        first_data_idx = -1
        
        for idx, row in df.iterrows():
            numeric_count = 0
            total_count = 0
            for val in row:
                if pd.isna(val): continue
                total_count += 1
                
                # Check for numeric or date types
                # Note: bool is subclass of int, but usually we don't count it as "data" in this context? 
                # Actually bool data is rare in these reports, but let's count it as data if present.
                if isinstance(val, (int, float, complex)):
                    numeric_count += 1
                elif isinstance(val, (pd.Timestamp, datetime.datetime, datetime.date, datetime.time)):
                    numeric_count += 1
            
            # If > 40% of non-null values are numeric/date, consider it a Data row
            ratio = numeric_count / total_count if total_count > 0 else 0
            # print(f"Row {idx}: numeric={numeric_count}, total={total_count}, ratio={ratio:.2f}")
            if total_count > 0 and ratio > 0.4:
                first_data_idx = idx
                break
        
        if first_data_idx > 0:
            # Found a data row. The header ends at the previous row.
            header_end_idx = first_data_idx - 1
            # Merge everything from top to header_end_idx
            # But we should check if top rows are empty?
            # Let's count non-empty rows above header_end_idx
            merge_count = 0
            for i in range(header_end_idx, -1, -1):
                if df.iloc[i].count() > 0:
                    merge_count += 1
                else:
                    # If we hit an empty row, do we stop? 
                    # Usually yes, but if there is a Title at Row 0 and Header at Row 2, Row 1 empty.
                    # We probably don't want to merge Title if there is a gap.
                    break
            
            return header_end_idx + 1, merge_count
        elif first_data_idx == 0:
             # The very first row looks like data.
             # This is tricky. It could be a file without header, or the first row IS the header but contains numbers (e.g. years).
             # However, standard Excel files usually have a string header.
             # If Row 0 is data-like, we might assume:
             # A) No header (rare for this use case)
             # B) Header is actually Row 0, but our heuristic failed (e.g. header has dates?)
             # C) The heuristic is too aggressive.
             
             # Let's check if Row 0 is purely string.
             # If Row 0 is mixed/numeric, and we think it's data, maybe we default to Header=Row 1, Merge=1?
             # But wait, if Row 0 is data, then where is the header?
             # If the file has NO header, we can't really support it easily without column names.
             # So let's assume Row 0 is the Header, even if it looks like data (unlikely) OR our heuristic is just finding data immediately at Row 1 (0-index).
             
             # Actually, if first_data_idx == 0, it means Row 0 is data.
             # But we need a header.
             # Let's assume Row 0 is Header (1-based Row 1) and Row 1 is Data.
             # This is the standard case: Header at 1, Data at 2.
             return 1, 1

        # Fallback: Max Non-Nulls Strategy
        max_non_null = -1
        header_end_idx = 0
        
        for idx, row in df.iterrows():
            non_null_count = row.count()
            if non_null_count > max_non_null:
                max_non_null = non_null_count
                header_end_idx = idx
        
        # Scan upwards for merge count
        merge_count = 1
        current_idx = header_end_idx - 1
        while current_idx >= 0:
            if df.iloc[current_idx].count() > 0:
                merge_count += 1
                current_idx -= 1
            else:
                break
        
        return header_end_idx + 1, merge_count
        
    except Exception as e:
        print(f"Warning: Could not auto-detect header for sheet '{sheet_name}': {e}. Defaulting to row 1.")
        return 1, 1

def generate_config(input_file: str, output_config_file: str, header_mode: str = "row") -> None:
    """
    Generates a configuration file for the specified input Excel file using automatic detection.

    Args:
        input_file (str): The path to the input Excel file.
        output_config_file (str): The path where the generated JSON configuration will be saved.
        header_mode (str): "row" or "column".

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
        "header_mode": header_mode,
        "sheets": {}
    }

    for sheet_name in sheet_names:
        print("-" * 40)
        print(f"analyzing sheet: '{sheet_name}'")

        if header_mode == "column":
             # Skip row-based detection for column mode, assume first column (now row 1 after transpose) is header
             header_num = 1
             merge_rows_count = 1
             print(f"Header Mode is 'column'. Skipping row detection. Defaulting to Header at row 1 (after transpose).")
        else:
            # Auto-detect header and merge count
            header_num, merge_rows_count = detect_header_and_merge_count(input_file, sheet_name)
        
        # Default values for simplified flow
        excluded_rows = []

        config_data["sheets"][sheet_name] = {
            "header_row": header_num,
            "merge_rows": merge_rows_count,
            "excluded_rows": excluded_rows
        }
        print(f"auto-detection complete: Header at row {header_num}, Merged {merge_rows_count} rows.")

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
