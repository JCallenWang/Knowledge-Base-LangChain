import argparse
import pandas as pd
import os
import json
import re
import datetime

def sanitize_filename(name):
    name = name.strip().replace(' ', '_')
    name = re.sub(r'[^\w\-]', '', name)
    return name

def load_data_from_sheet(input_file, sheet_name, header_index, merge_rows_count):
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

def _load_and_clean_sheet(input_file, sheet_name, header_config):
    header_num = header_config['header_row']
    merge_rows_count = header_config['merge_rows']
    excluded_rows = header_config.get('excluded_rows', [])
    print(f"\n--- process sheet: {sheet_name} (Header Row ends:{header_num}, merged rows count: {merge_rows_count} ) ---")
    header_index = header_num - 1

    try:
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
        indices_to_drop = set()
        for original_row in excluded_rows:
            df_index = original_row - (header_num + 1)
            if 0 <= df_index < len(df):
                indices_to_drop.add(df_index)

        initial_rows = len(df)
        if indices_to_drop:
            df.drop(list(indices_to_drop), inplace=True)
            removed_rows = initial_rows - len(df)
            print(f"exclude {removed_rows} rows successfully (original row number: {excluded_rows}.)")
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

def _format_records(df, metadata_string, source_file_id):
    if df.empty:
        return []

    records = df.to_dict(orient='records')
    
    for record in records:
        for k, v in record.items():
            if pd.notna(v):
                if isinstance(v, (pd.Timestamp, datetime.datetime)):
                    record[k] = v.date().isoformat() if v.time() == datetime.time(0) else v.isoformat()
                # pandas >= 2.0 uses pd.NA, older versions use np.nan
                elif pd.isna(v):
                     record[k] = ""
            else:
                record[k] = ""
        
        if metadata_string:
            record['Metadata'] = metadata_string
        record['SourceFile'] = source_file_id

    return records

def process_single_sheet(input_file, output_file_path, sheet_name, header_config):
    df, metadata_string = _load_and_clean_sheet(input_file, sheet_name, header_config)

    if df is None:
        return 0

    if df.empty:
        print(f"sheet '{sheet_name}' have empty data after cleaning.")
        return 0

    source_filename_only = os.path.basename(input_file)
    source_file_id = f"{source_filename_only}-{sheet_name}"
    
    records = _format_records(df, metadata_string, source_file_id)

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

def process_single_sheet_with_separated_output(input_file, input_file_base, sheet_name, header_config):
    df, metadata_string = _load_and_clean_sheet(input_file, sheet_name, header_config)

    if df is None:
        return 0

    if df.empty:
        print(f"sheet '{sheet_name}' have empty data after cleaning.")
        return 0

    source_filename_only = os.path.basename(input_file)
    source_file_id = f"{source_filename_only}-{sheet_name}"

    records = _format_records(df, metadata_string, source_file_id)
    
    print(f"Saving each row to a separate .jsonl file.")
    for i, record in enumerate(records):
        output_file_path_row = f"{input_file_base}_{sanitize_filename(sheet_name)}_{i}.jsonl"
        try:
            with open(output_file_path_row, 'w', encoding='utf-8') as output_stream:
                json_string = json.dumps(record, ensure_ascii=False)
                output_stream.write(f"{json_string}\n")
        except Exception as e:
            print(f"Error occurred when writing file '{output_file_path_row}': {e}")

    print(f"sheet '{sheet_name}' process complete, output {len(records)} separated datas.")
    return len(records)


def main(config_file, output_file_dir, separated_output=False, ):
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
    print(f"total sheet count: {len(sheet_names)}, output as separated .txt file")
    
    try:
        for sheet_name in sheet_names:
            header_config = sheets_config[sheet_name]
            
            if not separated_output:
                safe_sheet_name = sanitize_filename(sheet_name)
                output_file_path = f"{output_file_dir}/{input_file_base}_{safe_sheet_name}.jsonl"

                records_count = process_single_sheet(
                    input_file, output_file_path, sheet_name, header_config
                )
                total_records += records_count
            else:
                records_count = process_single_sheet_with_separated_output(
                    input_file, input_file_base, sheet_name, header_config
                )
                total_records += records_count
            break
        print("\n" + "=" * 50)
        print(f"process complete! all sheets have been converted into separated .txt file.")
        print(f"Total process count: {total_records}")
        print("=" * 50)
        
    except FileNotFoundError:
        print(f"Error: cannot find source file '{input_file}'.")
    except Exception as e:
        print(f"Error: unexpected error occurred: {e}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Process Excel data based on a configuration file.")
    parser.add_argument("config_file", help="The name of the configuration file (e.g., config.json).")
    parser.add_argument("output_file_dir", help="The directory where the output files will be saved.")
    parser.add_argument("--separated_output", type=lambda x: x.lower() == 'true', default=False,
                        help="Set to 'True' to output each row as a separate .txt file, 'False' for combined output per sheet.")
    args = parser.parse_args()

    main(args.config_file, args.output_file_dir, args.separated_output)
