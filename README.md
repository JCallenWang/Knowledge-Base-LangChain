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
    *   Start web service.

    ```bash
    ./app_start.sh
    ```

    Follow the interactive menu:
    *   Select **Setup & Start (Build, Start, Pull Model, Web Service)** for the initial setup.
    *   Select **Resume (Start if needed, Web Service)** if you have already set it up and want to restart the container.
    *   Select **Stop Container** to stop the running container.
    *   Select **Exit** to leave the script.

## Usage

You can interact with the agent via a Web UI.

**Access the UI**:
Open your browser and navigate to `http://localhost:8000/static/index.html`
