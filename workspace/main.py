import os
import sys
import hashlib
import shutil
import subprocess


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


def run_script(command):
    """Runs a script as a subprocess and checks for errors."""
    try:
        print(f"\n--- Running command: {' '.join(command)} ---\n")
        process = subprocess.run(command, check=True, text=True, capture_output=True)
        print(process.stdout)
        if process.stderr:
            print("--- Stderr ---")
            print(process.stderr)
        print(f"\n--- Command finished successfully ---")
    except subprocess.CalledProcessError as e:
        print(f"Error executing {' '.join(e.cmd)}.")
        print(f"Return code: {e.returncode}")
        print("--- Stdout ---")
        print(e.stdout)
        print("--- Stderr ---")
        print(e.stderr)
        sys.exit(1)


def main():
    """Main function to orchestrate the data processing workflow."""
    # 1. Get source file from user
    source_file = input("Please enter the path to the source XLSX file: ").strip()
    if not os.path.exists(source_file) or not source_file.endswith('.xlsx'):
        print(f"Error: File '{source_file}' is not a valid .xlsx file.")
        sys.exit(1)

    # 2. Create a hash-based directory to store artifacts
    file_hash = get_file_hash(source_file)
    base_name = os.path.splitext(os.path.basename(source_file))[0]
    output_dir = f"./{base_name}_{file_hash[:8]}"
    os.makedirs(output_dir, exist_ok=True)
    print(f"Artifacts will be saved in: '{output_dir}'")

    # Copy source file to the new directory
    staged_source_file = os.path.join(output_dir, os.path.basename(source_file))
    shutil.copy(source_file, staged_source_file)

    # Define file paths
    config_file = os.path.join(output_dir, f"{base_name}_config.json")
    jsonl_file = os.path.join(output_dir, f"{base_name}_事務機.jsonl")
    db_file = os.path.join(output_dir, f"{base_name}.db")
    db_hash_file = os.path.join(output_dir, f"{base_name}_hash.txt")

    # 3. Run config_generator.py
    print("\nStep 1: Generating configuration file...")
    config_command = ["python", "pre_process/config_generator.py", staged_source_file, config_file]
    # This script requires user input, so we run it differently to allow interaction.
    subprocess.run(config_command)

    # 4. Run data_processor.py
    print("\nStep 2: Processing data and converting to JSONL...")
    process_command = ["python", "pre_process/data_processor.py", config_file, output_dir]
    run_script(process_command)

    # 5. Run db_sql_init.py
    print("\nStep 3: Initializing SQL database from JSONL file...")
    db_init_command = ["python", "db_sql_init.py", jsonl_file, db_file, db_hash_file]
    run_script(db_init_command)

    print(f"\nWorkflow complete! All artifacts are located in '{output_dir}'.")


if __name__ == "__main__":
    main()