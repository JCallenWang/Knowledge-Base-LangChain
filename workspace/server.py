import os
import shutil
import uuid
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

class ChatRequest(BaseModel):
    session_id: str
    message: str

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
        
        # Initialize agent chain
        chain, is_multi_db, db_names = get_agent_chain(db_path, "")
        
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
        context = SQLAgentContext(
            user_input=request.message,
            schema_description=""
        )
        if is_multi_db:
            context.db_names = db_names
            
        # Invoke the chain
        # Note: The chain prints to stdout, but also updates the context object.
        # We need to capture the final response from the context.
        # The chain returns the context object.
        result_context = chain.invoke(context)
        
        return {
            "response": result_context.final_response,
            "sql_query": result_context.query,
            "sql_result": str(result_context.result)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
async def read_index():
    return JSONResponse(content={"message": "Welcome to Knowledge Base LangChain API. Please use /static/index.html to access the UI."})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
