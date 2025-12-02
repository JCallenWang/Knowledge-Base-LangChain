import os
import shutil
import uuid
import json
import glob
from typing import List, Optional
from fastapi import FastAPI, UploadFile, File, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from main import xlsx_to_sql_init
from agents.sql_agent import get_agent_chain
from agents.utils_sql import SQLAgentContext

app = FastAPI()

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Store active agents: session_id -> {chain, is_multi_db, db_names}
active_agents = {}
CONFIG_FILE = "agent_config.json"

class ChatRequest(BaseModel):
    session_id: str
    message: str

class ConfigRequest(BaseModel):
    instructions: str

def get_agent_instructions() -> str:
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get("instructions", "")
        except Exception:
            return ""
    return ""

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload .xlsx or .xls")

    # Create a unique session ID
    session_id = str(uuid.uuid4())
    upload_dir = f"uploads/{session_id}"
    os.makedirs(upload_dir, exist_ok=True)
    
    file_path = os.path.join(upload_dir, file.filename)
    
    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
            
        # Process the file
        db_path = xlsx_to_sql_init(file_path)
        
        # Get instructions from config
        instructions = get_agent_instructions()
        
        # Initialize agent chain
        chain, is_multi_db, db_names = get_agent_chain(db_path, instructions)
        
        # Cleanup upload directory
        if os.path.exists(upload_dir):
            shutil.rmtree(upload_dir)
        
        if chain:
            active_agents[session_id] = {
                "chain": chain,
                "is_multi_db": is_multi_db,
                "db_names": db_names
            }
            return {"session_id": session_id, "message": "File processed successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to initialize SQL Agent")
            
    except Exception as e:
        # Cleanup on error
        if os.path.exists(upload_dir):
            shutil.rmtree(upload_dir)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/chat")
async def chat(request: ChatRequest):
    session_id = request.session_id
    if session_id not in active_agents:
        raise HTTPException(status_code=404, detail="Session not found. Please upload a file first.")
    
    agent_data = active_agents[session_id]
    chain = agent_data["chain"]
    is_multi_db = agent_data["is_multi_db"]
    db_names = agent_data["db_names"]
    
    try:
        # Get current instructions
        instructions = get_agent_instructions()
        
        context = SQLAgentContext(
            user_input=request.message,
            schema_description=instructions
        )
        if is_multi_db:
            context.db_names = db_names
            
        # Invoke the chain
        result_context = chain.invoke(context)
        
        return {
            "response": result_context.final_response,
            "sql_query": result_context.query,
            "sql_result": str(result_context.result)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- Management Endpoints ---

@app.get("/config/instructions")
async def read_instructions():
    return {"instructions": get_agent_instructions()}

@app.post("/config/instructions")
async def update_instructions(config: ConfigRequest):
    try:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump({"instructions": config.instructions}, f, ensure_ascii=False, indent=4)
        return {"message": "Instructions updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/files")
async def list_files():
    # List directories that look like datasets (e.g., *_hash)
    # We assume they are in the current directory and match the pattern from main.py
    # But main.py creates folders like "./{base_name}_{file_hash[:8]}"
    # Let's list all directories that contain 'config' and 'database' subdirs to be safe, 
    # or just list directories that are not 'workspace', 'dockerfile', etc.
    
    # Better approach: Look for directories containing a .db file in a 'database' subdir
    datasets = []
    for d in os.listdir('.'):
        if os.path.isdir(d) and not d.startswith('.') and d not in ['agents', 'data_initiation', 'data_preprocessing', 'dockerfile', 'static', 'uploads', '__pycache__']:
            db_dir = os.path.join(d, 'database')
            if os.path.exists(db_dir) and glob.glob(os.path.join(db_dir, '*.db')):
                datasets.append(d)
    return {"files": datasets}

@app.delete("/files/{folder_name}")
async def delete_file(folder_name: str):
    # Security check: prevent deleting system folders
    if folder_name in ['agents', 'data_initiation', 'data_preprocessing', 'dockerfile', 'static', 'uploads', '__pycache__'] or folder_name.startswith('.'):
        raise HTTPException(status_code=403, detail="Cannot delete system folders")
    
    if not os.path.exists(folder_name):
        raise HTTPException(status_code=404, detail="Folder not found")
        
    try:
        shutil.rmtree(folder_name)
        return {"message": f"Dataset '{folder_name}' deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
async def read_index():
    return JSONResponse(content={"message": "Welcome to Knowledge Base LangChain API. Please use /static/index.html to access the UI."})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
