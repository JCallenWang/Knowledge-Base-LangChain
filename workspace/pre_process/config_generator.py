import pandas as pd
import sys
import os
import json
import argparse

def get_sheet_info(input_file):
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

def get_integer_input(prompt_text, min_value=1):
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
            
def get_excluded_rows_input(sheet_name, header_row):
    prompt = (
        f"please enter *original rows* that will be excluded in sheet '{sheet_name}' (>{header_row}). "
        f"example: 15,16,20,99 (separate with comma, leave empty if none): "
    )
    
    while True:
        try:
            input_str = input(prompt).strip()
            if not input_str:
                return []
            
            rows = [int(r.strip()) for r in input_str.split(',') if r.strip().isdigit()]
            
            invalid_rows = [r for r in rows if r <= header_row]
            if invalid_rows:
                print(f"invalid input: excluded row ({invalid_rows}) must greater than the ends row of Header ({header_row}). please enter again.")
                continue
                
            return sorted(list(set(rows)))
            
        except Exception:
            print("invalid input format. please make sure to use *,* to separate integer only.")

def generate_config(input_file, output_config_file):
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
        print(f"continue next step with 'data_processor.py'.")
        print("=" * 50)

    except Exception as e:
        print(f"Error while writing config file:{e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Generate a configuration file for data processing.")
    parser.add_argument("input_file", help="The name of the input source file (.xlsx).")
    parser.add_argument("config_file", help="The name of the output config file.")
    args = parser.parse_args()

    generate_config(args.config_file, args.input_file)
