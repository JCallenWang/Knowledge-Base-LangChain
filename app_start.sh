#!/bin/bash

REPO_URL="https://github.com/JCallenWang/Knowledge-Base-LangChain.git"
DIR_NAME="Knowledge-Base-LangChain"

# 1. Clone repo and cd into it
if [ -d "$DIR_NAME" ]; then
    echo "Directory '$DIR_NAME' already exists. Skipping clone."
else
    echo "Cloning $REPO_URL..."
    git clone "$REPO_URL"
fi

cd "$DIR_NAME" || { echo "Failed to enter directory $DIR_NAME"; exit 1; }

# 2. cd dockerfile/stage
cd dockerfile/stage || { echo "Failed to enter directory dockerfile/stage"; exit 1; }

# 3. run "docker compose build --no-cache"
echo "Building docker image..."
docker compose build --no-cache

# 4. run "docker compose up -d"
echo "Starting docker container..."
docker compose up -d


# Wait for Ollama to be ready
CONTAINER_NAME="fc-rag-dev-v3"
echo "Waiting for Ollama service to be ready..."
until docker exec "$CONTAINER_NAME" ollama list > /dev/null 2>&1; do
    sleep 2
done

# Pull the model interactively (shows progress bar)
echo "Pulling model gemma3:27b..."
docker exec -it "$CONTAINER_NAME" ollama pull gemma3:27b

# 5. run "docker exec -it <container-name> bash"
echo "Entering container $CONTAINER_NAME..."
docker exec -it "$CONTAINER_NAME" bash