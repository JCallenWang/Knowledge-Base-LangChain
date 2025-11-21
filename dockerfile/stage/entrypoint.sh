#!/bin/bash

# Start ollama serve in the background
ollama serve &
pid=$!

# Wait for ollama to be ready
echo "Waiting for Ollama service to start..."
while ! ollama list > /dev/null 2>&1; do
  sleep 1
done

# Pull the model
echo "Pulling model gemma3:27b..."
ollama pull gemma3:27b

# Wait for the background process to ensure container stays running
wait $pid
