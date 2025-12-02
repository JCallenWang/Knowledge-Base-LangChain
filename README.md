# Knowledge Base LangChain SQL Agent

This repository provides a comprehensive toolset for converting Excel files into SQLite databases and interacting with them using a Large Language Model (LLM) powered SQL agent.

## Purpose

The main goal of this project is to streamline the process of querying data stored in Excel spreadsheets. Instead of manually filtering and analyzing Excel files, this tool automates the ingestion process:
1.  **Ingestion**: Converts Excel sheets into structured SQLite databases.
2.  **Interaction**: Provides a natural language interface (Chat) to query the data using an LLM.

## Setup and Installation

### Prerequisites

*   Docker and Docker Compose
*   Git

### Installation

1.  **Clone the repository** (if you haven't already):
    ```bash
    git clone <repository_url>
    cd Knowledge-Base-LangChain
    ```

2.  **Start the Application**:
    Use the provided `app_start.sh` script to set up the environment. This script handles:
    *   Building and starting the Docker container.
    *   Pulling the necessary LLM model (e.g., `gemma3:27b`) via Ollama.
    *   Entering the container environment.

    ```bash
    ./app_start.sh
    ```

    Follow the interactive menu:
    *   Select **First Run** for the initial setup.
    *   Select **Resume** if you have already set it up and want to restart the container.

## Usage

Once you are inside the Docker container, you can run the main application to process an Excel file and start the agent.

### Running the Agent

Navigate to the `workspace` directory and run `main.py` with your Excel file as an argument:

```bash
cd workspace
python main.py source/Financial\ Sample.xlsx
```

### Workflow Description

The `main.py` script orchestrates the following steps:

1.  **Configuration Generation**:
    *   It scans the Excel file and detects sheets.
    *   It prompts you to define the header rows and any rows to exclude.
    *   A configuration JSON file is generated.

2.  **Data Processing**:
    *   Based on the configuration, it cleans the data (removes empty columns, handles merged headers).
    *   It converts the data into JSON Lines (`.jsonl`) format.

3.  **Database Initialization**:
    *   It creates a SQLite database (`.db`) for each processed sheet.
    *   It infers data types and populates the tables.

4.  **SQL Agent Interaction**:
    *   The SQL Agent starts and connects to the generated database(s).
    *   You can ask questions in natural language (e.g., "What is the total profit for Government segment?").
    *   The agent translates your question into SQL, executes it, and provides a natural language answer.

### Interactive Query System

When the agent starts, you will see a prompt:
```text
(Enter 'bye'/'exit' to exit the query system) Enter a question:
```
Simply type your question and press Enter.

## Project Structure

*   `workspace/`: Contains the source code and working directories.
    *   `main.py`: The entry point script.
    *   `agents/`: Contains the Logic for the SQL Agent and LangChain integration.
    *   `data_initiation/`: Handles SQLite database creation.
    *   `data_preprocessing/`: Handles Excel parsing and cleaning.
*   `app_start.sh`: Helper script for Docker management.
*   `dockerfile/`: Docker configuration files.

## Documentation

All source code files are fully documented with docstrings adhering to Google Style Python Docstrings.
