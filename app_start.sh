#!/bin/bash

CONTAINER_NAME="fc-rag-dev-v3"
DOCKER_DIR="dockerfile/stage"

# Helper to navigate to docker directory
check_and_cd_docker_dir() {
    if [ -d "$DOCKER_DIR" ]; then
        cd "$DOCKER_DIR" || return 1
    else
        return 1
    fi
}

first_run() {
    # Navigate to docker directory
    if ! check_and_cd_docker_dir; then
        echo "Error: Cannot find docker directory '$DOCKER_DIR'."
        echo "Please ensure you are running this script from the project root."
        read -n 1 -s -r -p "Press any key to return to menu..."
        return
    fi

    # 2. Build and Start
    echo "Building docker image..."
    docker compose build --no-cache
    
    echo "Starting docker container..."
    docker compose up -d

    # 3. Wait for Ollama
    echo "Waiting for Ollama service to be ready..."
    until docker exec "$CONTAINER_NAME" ollama list > /dev/null 2>&1; do
        sleep 2
    done

    # 4. Pull Model
    echo "Pulling model gemma3:27b..."
    docker exec -it "$CONTAINER_NAME" ollama pull gemma3:27b

    # 5. Start Web Service
    echo "Starting Web Service..."
    echo "Access the UI at: http://localhost:8000/static/index.html"
    echo "Press Ctrl+C to stop the server and return to menu."
    docker exec -it "$CONTAINER_NAME" python3 -u server.py
}

resume_run() {
    # 1. Check if project directory exists
    if ! check_and_cd_docker_dir; then
        echo "Error: Docker directory not found."
        echo "Please ensure you are running this script from the project root."
        read -n 1 -s -r -p "Press any key to return to menu..."
        return
    fi

    # 2. Check if container exists (running or stopped)
    if [ -z "$(docker ps -a -q -f name=$CONTAINER_NAME)" ]; then
        echo "Container $CONTAINER_NAME does not exist. Creating and starting..."
        docker compose up -d
    elif [ -z "$(docker ps -q -f name=$CONTAINER_NAME)" ]; then
        echo "Container $CONTAINER_NAME is stopped. Starting..."
        docker compose start
    else
        echo "Container $CONTAINER_NAME is already running."
    fi

    # 3. Wait for Ollama
    echo "Waiting for Ollama service to be ready..."
    until docker exec "$CONTAINER_NAME" ollama list > /dev/null 2>&1; do
        sleep 2
    done

    # 4. Start Web Service
    echo "Starting Web Service..."
    echo "Access the UI at: http://localhost:8000/static/index.html"
    echo "Press Ctrl+C to stop the server and return to menu."
    docker exec -it "$CONTAINER_NAME" python3 -u server.py
}

stop_container() {
    # Check if container exists at all
    if [ -z "$(docker ps -a -q -f name=$CONTAINER_NAME)" ]; then
        echo "Container $CONTAINER_NAME does not exist."
        read -n 1 -s -r -p "Press any key to return to menu..."
        return
    fi

    echo "Stopping container $CONTAINER_NAME..."
    docker stop "$CONTAINER_NAME"
    echo "Container stopped."
    read -n 1 -s -r -p "Press any key to return to menu..."
}

# Interactive Menu
echo -ne "\033[?25l" # Hide cursor
trap 'echo -ne "\033[?25h"; exit' INT TERM EXIT # Restore cursor on exit

while true; do
    clear
    echo "Use UP/DOWN arrows to navigate, ENTER to select."
    echo

    OPTIONS=(
        "Setup & Start (Build, Start, Pull Model, Web Service)"
        "Resume (Start if needed, Web Service)"
        "Stop Container"
        "Exit"
    )

    # Default selection if not set
    if [ -z "$SELECTED" ]; then SELECTED=0; fi

    # Print menu
    for i in "${!OPTIONS[@]}"; do
        if [ $i -eq $SELECTED ]; then
            echo -e "\033[7m> ${OPTIONS[$i]}\033[0m" # Inverse video
        else
            echo "  ${OPTIONS[$i]}"
        fi
    done

    # Read input
    read -rsn1 key
    if [[ $key == $'\x1b' ]]; then
        read -rsn2 key
        if [[ $key == "[A" ]]; then # Up
            ((SELECTED--))
            if [ $SELECTED -lt 0 ]; then SELECTED=$((${#OPTIONS[@]} - 1)); fi
        elif [[ $key == "[B" ]]; then # Down
            ((SELECTED++))
            if [ $SELECTED -ge ${#OPTIONS[@]} ]; then SELECTED=0; fi
        fi
    elif [[ $key == "" ]]; then # Enter
        echo # Newline after selection
        case $SELECTED in
            0) first_run ;;
            1) resume_run ;;
            2) stop_container ;;
            3) echo "Exiting..."; exit 0 ;;
        esac
    fi
done