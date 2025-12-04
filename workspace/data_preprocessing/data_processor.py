"""
Module for processing Excel data based on configuration files.

This module reads Excel sheets according to a provided configuration, cleans the data
by handling headers, metadata, and excluded rows, and outputs the result as JSONL files.
"""

import argparse
import pandas as pd
import os
import json
import re
import datetime
from typing import List, Set, Union, Tuple, Optional, Dict, Any

def sanitize_filename(name: str) -> str:
    """
    Sanitizes a string to be safe for use as a filename.

    Args:
        name (str): The original filename string.

    Returns:
        str: The sanitized filename string with special characters removed or replaced.
    """
    name = name.strip().replace(' ', '_')
    name = re.sub(r'[^\w\-]', '', name)
    return name

def load_data_from_sheet(input_file: str, sheet_name: str, header_index: int, merge_rows_count: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Loads data and metadata (pre-header rows) from a specific Excel sheet.

    Args:
        input_file (str): The path to the Excel file.
        sheet_name (str): The name of the sheet to load.
        header_index (int): The 0-based index of the last header row.
        merge_rows_count (int): The number of rows that make up the header.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the main data DataFrame and the pre-header (metadata) DataFrame.
    """
    header_indices = list(range(header_index - merge_rows_count + 1, header_index + 1))
    before_header_rows_count = header_indices[0]

    kwargs_excel = {'engine': 'openpyxl', 'sheet_name': sheet_name}

    # Main data
    kwargs_main = {'header': header_indices, **kwargs_excel}
    df_main = pd.read_excel(input_file, **kwargs_main)

    # merge header
    if merge_rows_count > 1:
        df_main.columns = [
            ' - '.join(
                str(c).strip() for c in col
                if not pd.isna(c) and str(c).strip() and not str(c).strip().startswith('Unnamed:'))
            for col in df_main.columns.values
        ]
    df_main.columns = [str(col) for col in df_main.columns]

    # Metadata (before header)
    kwargs_pre = {'header': None, 'nrows': before_header_rows_count, **kwargs_excel}
    df_pre_header = pd.read_excel(input_file, **kwargs_pre)

    return df_main, df_pre_header

def _parse_excluded_rows(excluded_rows_config: List[Union[int, str]]) -> Set[int]:
    """
    Parses the excluded_rows config into a set of integer row indices.

    Args:
        excluded_rows_config (List[Union[int, str]]): A list of integers or string ranges (e.g., '18-134').

    Returns:
        Set[int]: A set of unique row numbers to exclude.
    """
    excluded = set()
    if not excluded_rows_config:
        return excluded

    for item in excluded_rows_config:
        if isinstance(item, int):
            excluded.add(item)
        elif isinstance(item, str) and '-' in item:
            try:
                start, end = map(int, item.split('-'))
                if start <= end:
                    excluded.update(range(start, end + 1))
            except ValueError:
                print(f"Warning: Could not parse range '{item}' in excluded_rows. Skipping.")
    return excluded

def _load_and_clean_sheet(input_file: str, sheet_name: str, header_config: Dict[str, Any], header_mode: str = "row") -> Tuple[Optional[pd.DataFrame], str]:
    """
    Loads an Excel sheet and applies cleaning operations such as removing empty columns,
    processing headers, and excluding specified rows.

    Args:
        input_file (str): The path to the Excel file.
        sheet_name (str): The name of the sheet to process.
        header_config (Dict[str, Any]): Configuration dictionary containing 'header_row', 'merge_rows', and 'excluded_rows'.
        header_mode (str): "row" or "column".

    Returns:
        Tuple[Optional[pd.DataFrame], str]: A tuple containing the cleaned DataFrame and a metadata string.
        Returns (None, None) if loading fails.
    """
    header_num = header_config['header_row']
    merge_rows_count = header_config['merge_rows']
    excluded_rows = header_config.get('excluded_rows', [])
    print(f"\n--- process sheet: {sheet_name} (Header ends:{header_num}, merged count: {merge_rows_count}, Mode: {header_mode}) ---")
    header_index = header_num - 1

    try:
        if header_mode == "column":
             # Read entire sheet without header
             df_raw = pd.read_excel(input_file, sheet_name=sheet_name, header=None, engine='openpyxl')
             # Transpose
             df_transposed = df_raw.T.reset_index(drop=True)
             
             # Apply header logic on transposed data
             if header_num > merge_rows_count:
                 # Metadata exists
                 header_indices = list(range(header_index - merge_rows_count + 1, header_index + 1))
                 before_header_rows_count = header_indices[0]
                 
                 df_pre_header = df_transposed.iloc[:before_header_rows_count].copy()
                 df_main = df_transposed.iloc[header_index + 1:].copy()
                 
                 # Set header
                 header_rows_df = df_transposed.iloc[header_indices]
                 if merge_rows_count > 1:
                     new_columns = []
                     for col_idx in range(header_rows_df.shape[1]):
                         col_vals = header_rows_df.iloc[:, col_idx]
                         col_name = ' - '.join([str(v).strip() for v in col_vals if pd.notna(v) and str(v).strip()])
                         new_columns.append(col_name)
                     df_main.columns = new_columns
                 else:
                     df_main.columns = header_rows_df.iloc[0].astype(str)
                 
                 is_metadata_present = True
                 print("datas before header have override default Metadata (Column Mode).")
                 
             else:
                 # No metadata
                 header_indices = list(range(header_index - merge_rows_count + 1, header_index + 1))
                 
                 df_main = df_transposed.iloc[header_index + 1:].copy()
                 
                 header_rows_df = df_transposed.iloc[header_indices]
                 if merge_rows_count > 1:
                     new_columns = []
                     for col_idx in range(header_rows_df.shape[1]):
                         col_vals = header_rows_df.iloc[:, col_idx]
                         col_name = ' - '.join([str(v).strip() for v in col_vals if pd.notna(v) and str(v).strip()])
                         new_columns.append(col_name)
                     df_main.columns = new_columns
                 else:
                     df_main.columns = header_rows_df.iloc[0].astype(str)
                     
                 df_pre_header = pd.DataFrame()
                 is_metadata_present = False
                 print("using default Metadata (Column Mode).")
                 
             df = df_main
             
        else:
            if header_num > merge_rows_count:
                df, df_pre_header = load_data_from_sheet(
                    input_file, sheet_name, header_index, merge_rows_count
                )
                is_metadata_present = True
                print("datas before header have override default Metadata.")
            else:
                df, _ = load_data_from_sheet(
                    input_file, sheet_name, header_index, merge_rows_count
                )
                df_pre_header = pd.DataFrame()
                is_metadata_present = False
                print("using default Metadata.")

    except Exception as e:
        print(f"loading data failed: {e}")
        return None, None

    if header_mode == 'row':
        temp_kwargs = {'header': None, 'nrows': header_num, 'engine': 'openpyxl', 'sheet_name': sheet_name}
        df_header_block = pd.read_excel(input_file, **temp_kwargs)
        cols_to_drop_indices = df_header_block.columns[df_header_block.isna().all()].tolist()
        if cols_to_drop_indices:
            print(f"cleaning Metadata/Header block: detect {len(cols_to_drop_indices)} empty columns (index: {cols_to_drop_indices}).")

            if is_metadata_present and not df_pre_header.empty:
                all_cols_indices = df_pre_header.columns.tolist()
                cols_to_keep_indices = [col for col in all_cols_indices if col not in cols_to_drop_indices]

                df_pre_header = df_pre_header.iloc[:, cols_to_keep_indices]
                print(f"cleaning Metadata: remove {len(cols_to_drop_indices)} columns successfully.")
        else:
            print("Metadata/Header block donnot have empty field.")

    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed:')]
    df.columns = df.columns.str.strip()

    float_cols = df.select_dtypes(include=['float64']).columns
    for col in float_cols:
        try:
            df[col] = df[col].astype('Int64')
        except:
            pass

    if excluded_rows:
        parsed_excluded_rows = _parse_excluded_rows(excluded_rows)
        indices_to_drop = set()
        for original_row in parsed_excluded_rows:
            df_index = original_row - (header_num + 1)
            if 0 <= df_index < len(df):
                indices_to_drop.add(df_index)

        initial_rows = len(df)
        if indices_to_drop:
            df.drop(list(indices_to_drop), inplace=True)
            removed_rows = initial_rows - len(df)
            print(f"exclude {removed_rows} rows successfully (from config: {excluded_rows}).")
        else:
            print(f"cannot find valid rows to exclude in config file.")
    else:
        print("there is no exclude rows specified in config file.")

    df.dropna(axis=0, how='all', inplace=True)
    print("data cleaning completed (remove null/ Unnamed field, convert float in integer, exlcude specified rows).")

    metadata_string = ""

    if is_metadata_present and not df_pre_header.empty:
        metadata_list = []
        for _, row in df_pre_header.iterrows():
            clean_row = ' '.join(row.dropna().astype(str))
            if clean_row:
                clean_row = clean_row.replace('\n', ' ').strip()
                metadata_list.append(clean_row)

        metadata_string = ' | '.join(metadata_list)
        print("Metadata have been extracted from sheet and structured successfully.")

    return df, metadata_string

def _format_records(df: pd.DataFrame, metadata_string: str) -> List[Dict[str, Any]]:
    """
    Formats the DataFrame records into a list of dictionaries, handling timestamps
    and appending metadata.

    Args:
        df (pd.DataFrame): The DataFrame containing the data.
        metadata_string (str): The metadata string to append to each record.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries representing the records.
    """
    if df.empty:
        return []

    records = df.to_dict(orient='records')

    for record in records:
        for k, v in record.items():
            if pd.notna(v):
                if isinstance(v, (pd.Timestamp, datetime.datetime)):
                    record[k] = v.date().isoformat() if v.time() == datetime.time(0) else v.isoformat()
                elif isinstance(v, datetime.time):
                    record[k] = v.isoformat()
                # pandas >= 2.0 uses pd.NA, older versions use np.nan
                elif pd.isna(v):
                     record[k] = None
            else:
                record[k] = None

        if metadata_string:
            record['ExtraInfo'] = metadata_string

    return records

def process_single_sheet(input_file: str, output_file_path: str, sheet_name: str, header_config: Dict[str, Any]) -> int:
    """
    Processes a single sheet from an Excel file and saves it as a JSONL file.

    Args:
        input_file (str): The path to the input Excel file.
        output_file_path (str): The path where the JSONL output will be saved.
        sheet_name (str): The name of the sheet to process.
        header_config (Dict[str, Any]): Configuration for the sheet's header and structure.

    Returns:
        int: The number of records successfully written to the output file.
    """
    df, metadata_string = _load_and_clean_sheet(input_file, sheet_name, header_config)

    if df is None:
        return 0

    if df.empty:
        print(f"sheet '{sheet_name}' have empty data after cleaning.")
        return 0

    records = _format_records(df, metadata_string)

    try:
        with open(output_file_path, 'w', encoding='utf-8') as output_stream:
            for record in records:
                json_string = json.dumps(record, ensure_ascii=False)
                output_stream.write(f"{json_string}\n")

        print(f"sheet '{sheet_name}' process complete, output {len(records)} datas to '{output_file_path}'.")
        return len(records)

    except Exception as e:
        print(f"error occurred when writing file '{output_file_path}': {e}")
        return 0


def load_dataframes_from_config(config_file: str) -> Dict[str, pd.DataFrame]:
    """
    Loads and processes Excel sheets into Pandas DataFrames based on a configuration file.

    Args:
        config_file (str): The path to the JSON configuration file.

    Returns:
        Dict[str, pd.DataFrame]: A dictionary where keys are sheet names and values are processed DataFrames.
    """
    print(f"--- start processing: loading config '{config_file}' ---")

    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: cannot find file '{config_file}'. please run 'config_generator.py' first")
        return {}
    except json.JSONDecodeError:
        print(f"Error: config file '{config_file}' has wrong format.")
        return {}

    input_file = config_data.get("input_file")
    header_mode = config_data.get("header_mode", "row")
    sheets_config = config_data.get("sheets", {})

    if not input_file or not sheets_config:
        print("Error: information in config file is not completed. please check the fields of 'input_file', 'sheets'.")
        return {}

    sheet_names = list(sheets_config.keys())
    print(f"source file: {input_file}")
    print(f"header mode: {header_mode}")
    print(f"total sheet count: {len(sheet_names)}, output as DataFrames")

    processed_dfs = {}

    try:
        for sheet_name in sheet_names:
            header_config = sheets_config[sheet_name]
            
            # Load and clean the sheet
            df, metadata_string = _load_and_clean_sheet(input_file, sheet_name, header_config, header_mode)

            if df is None or df.empty:
                print(f"sheet '{sheet_name}' is empty or failed to load.")
                continue

            # Add metadata as a column if it exists
            if metadata_string:
                df['ExtraInfo'] = metadata_string
            
            # Ensure all columns are string type for consistency before SQL export, 
            # or let pandas handle types. 
            # The previous logic in _format_records handled dates. 
            # Let's do a quick pass to ensure dates are strings if we want strict compatibility, 
            # but SQLite handles dates as strings usually.
            # However, to match previous behavior:
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                     df[col] = df[col].apply(lambda x: x.date().isoformat() if pd.notnull(x) and x.time() == datetime.time(0) else (x.isoformat() if pd.notnull(x) else None))

            processed_dfs[sheet_name] = df
            print(f"sheet '{sheet_name}' processed into DataFrame with {len(df)} rows.")

        print("\n" + "=" * 50)
        print(f"process complete! {len(processed_dfs)} sheets loaded.")
        print("=" * 50)
        return processed_dfs

    except FileNotFoundError:
        print(f"Error: cannot find source file '{input_file}'.")
        return {}
    except Exception as e:
        print(f"Error: unexpected error occurred: {e}")
        return {}

def process_data_from_config(config_file: str, output_file_dir: str) -> None:
    """
    Orchestrates the data processing workflow based on a configuration file.

    Args:
        config_file (str): The path to the JSON configuration file.
        output_file_dir (str): The directory where the output JSONL files will be saved.

    Returns:
        None
    """
    print(f"--- start processing: loading config '{config_file}' ---")

    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: cannot find file '{config_file}'. please run 'config_generator.py' first")
        return
    except json.JSONDecodeError:
        print(f"Error: config file '{config_file}' has wrong format.")
        return

    input_file = config_data.get("input_file")
    sheets_config = config_data.get("sheets", {})

    if not input_file or not sheets_config:
        print("Error: information in config file is not completed. please check the fields of 'input_file', 'sheets'.")
        return

    total_records = 0
    sheet_names = list(sheets_config.keys())
    input_file_base = os.path.splitext(os.path.basename(input_file))[0]

    print(f"source file: {input_file}")
    print(f"total sheet count: {len(sheet_names)}, output as .jsonl file")

    try:
        for sheet_name in sheet_names:
            header_config = sheets_config[sheet_name]

            safe_sheet_name = sanitize_filename(sheet_name)
            output_file_path = f"{output_file_dir}/{input_file_base}_{safe_sheet_name}.jsonl"

            records_count = process_single_sheet(
                input_file, output_file_path, sheet_name, header_config
            )
            total_records += records_count
        print("\n" + "=" * 50)
        print(f"process complete! all sheets have been converted into .jsonl file.")
        print(f"Total process count: {total_records}")
        print("=" * 50)

    except FileNotFoundError:
        print(f"Error: cannot find source file '{input_file}'.")
    except Exception as e:
        print(f"Error: unexpected error occurred: {e}")


#if __name__ == '__main__':
#    parser = argparse.ArgumentParser(description="Process Excel data based on a configuration file.")
#    parser.add_argument("config_file", help="The name of the configuration file (e.g., config.json).")
#    parser.add_argument("output_file_dir", help="The directory where the output files will be saved.")
#    args = parser.parse_args()
#
#    process_data_from_config(args.config_file, args.output_file_dir)
