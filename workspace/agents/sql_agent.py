from langchain_ollama import OllamaLLM
from langchain_core.callbacks.streaming_stdout import StreamingStdOutCallbackHandler
from sqlalchemy.exc import OperationalError  
from langchain_core.runnables import RunnablePassthrough, RunnableLambda
from langchain_community.utilities import SQLDatabase
import time

import os
import glob
import re
from typing import Dict, Any
from .utils_sql import PromptFactory

SQL_QUERY_TRYING_LIMIT = 2
#LLM_MODEL = "gemma3:27b"
LLM_MODEL = "weitsung50110/llama-3-taiwan:8b-instruct-dpo-q4_K_M"
#LLM_MODEL = "cwchang/llama3-taide-lx-8b-chat-alpha1:q4_k_m"

SQL_LLM = "gemma3:27b"
#SQL_LLM = "weitsung50110/llama-3-taiwan:8b-instruct-dpo-q4_K_M"
#SQL_LLM = "cwchang/llama3-taide-lx-8b-chat-alpha1:q4_k_m"
#SQL_LLM = "duckdb-nsql:latest"

def init_model():
    llm = OllamaLLM(
        model = LLM_MODEL,
        #callbacks = [StreamingStdOutCallbackHandler()]
    )
    sqlm = OllamaLLM(
        model = SQL_LLM,
        #callbacks = [StreamingStdOutCallbackHandler()]
    )

    return llm, sqlm

def check_sql_query(query: str) -> bool:
    if any(keyword in query.upper() for keyword in ["DELETE FROM", "UPDATE", "INSERT INTO", "DROP TABLE", "ALTER TABLE", "CREATE TABLE"]):
        return True
    return False

def _parse_sql_query(raw_query: str) -> str:
    """
    Parses a raw string to extract a clean SQL query,
    removing markdown code blocks if present.
    """
    # Match content inside ```sql ... ``` or ``` ... ```
    match = re.search(r"```(?:sql)?\n(.*?)\n```", raw_query, re.DOTALL)
    if match:
        return match.group(1).strip()
    return raw_query.strip().strip('`').strip()
            
def start_sql_agent(db_path: str, additional_description: str):
    """
    Initializes and starts the SQL agent, which can handle either a single database file
    or a directory of databases.

    Args:
        db_path (str): The path to the database file or directory.
        schema_description (str): A description of the database schema.
    """
    llm, sqlm = init_model()

    if not os.path.exists(db_path):
        print(f"Cannot find database path: {db_path}")
        exit(0)

    db_files = []
    if os.path.isdir(db_path):
        db_files = glob.glob(os.path.join(db_path, '*.db'))
        if not db_files:
            print(f"No .db files found in directory: {db_path}")
            exit(0)
    elif os.path.isfile(db_path):
        db_files = [db_path]
    
    # A dictionary to cache SQLDatabase objects
    db_connections: Dict[str, SQLDatabase] = {}

    def get_db_connection(db_file_path: str) -> SQLDatabase:
        if db_file_path not in db_connections:
            #print(f"\nConnecting to database: '{db_file_path}'")
            db_connections[db_file_path] = SQLDatabase.from_uri(
                f"sqlite:///{db_file_path}",
                sample_rows_in_table_info=1
            )
        return db_connections[db_file_path]

    def run_sql_query(context_dict: Dict[str, Any]) -> str:
        query = context_dict["query"]
        db = context_dict["db"]
        
        if check_sql_query(query):
            return "Error: The generated SQL query contains disallowed operations (e.g., DELETE, UPDATE, INSERT, DROP, ALTER, CREATE). Please refine your request to only retrieve data."

        query = _parse_sql_query(query)

        for _ in range(SQL_QUERY_TRYING_LIMIT):
            try:
                return db.run(query)
            except (OperationalError, Exception) as e:
                print("\nEncountered a SQL error. Attempting to correct the query...")
                
                correction_chain = (
                    PromptFactory.create_sql_correction_prompt()
                    | sqlm
                )
                
                # The correction chain can now access 'llm' from the outer scope
                query = correction_chain.invoke({
                    "input": context_dict["input"],
                    "db_schema": context_dict["db_schema"],
                    "failing_query": query,
                    "error_message": str(e)
                })
                print(f"\n[Correcting] Generated SQL Query: \n{query}")

        return f"Error occurred when executing query: '{query}' with error: {str(e) if 'e' in locals() else 'Unknown error'}"

    def start_query_system(chain, is_multi_db: bool):
        while True:
            user_input = input("(Enter 'bye'/'exit' to exit the query system) Enter a question: ") 
            if user_input.lower() in ('bye', 'exit', 'clear'):
                print("Thanks for using, bye!")
                break

            if user_input.strip():
                start_time = time.time()
                invoke_params = {'input': user_input, 
                                 'schema_description': additional_description,
                                 'start_time': start_time}
                if is_multi_db:
                    # Provide the list of db names for the router to choose from
                    invoke_params['db_names'] = [os.path.basename(f) for f in db_files]
                chain.invoke(invoke_params)
                print("\n")
            else:
                print("Please enter a valid question!")

    # This chain selects the correct DB file path from a list of names
    db_router_chain = (
        PromptFactory.create_db_selection_prompt()
        | llm
        | (lambda selected_db_name: os.path.join(db_path, selected_db_name.strip()))
    ) if len(db_files) > 1 else RunnableLambda(lambda _: db_files[0])

    # This is the main chain that processes the user's request.
    db_chain = (
        RunnablePassthrough.assign(db_path=db_router_chain)
        | RunnableLambda(lambda x: print(f"\n[Debug][{time.time() - x['start_time']:.2f}s] Selected DB Path: {x['db_path']}") or x)
        | RunnablePassthrough.assign(db=lambda x: get_db_connection(x["db_path"]))
        | RunnablePassthrough.assign(db_schema=lambda x: x["db"].get_table_info())
        | RunnablePassthrough.assign(
            query=(
                PromptFactory.create_sql_generation_prompt()
                | sqlm
            )
        )
        | RunnableLambda(lambda x: print(f"\n[Debug][{time.time() - x['start_time']:.2f}s] Generated SQL Query: \n{x['query']}") or x)
        | RunnablePassthrough.assign(result=run_sql_query)
        | RunnableLambda(lambda x: print(f"\n[Debug][{time.time() - x['start_time']:.2f}s] SQL Query Result: {x['result']}") or x)
        | RunnablePassthrough.assign(
            final_response=(
                PromptFactory.create_answer_generation_prompt() | llm
            )
        )
        | RunnableLambda(lambda x: print(f"\n[Debug][{time.time() - x['start_time']:.2f}s] Final LLM Response: {x['final_response']}") or x)
    )

    start_query_system(db_chain, is_multi_db=len(db_files) > 1)