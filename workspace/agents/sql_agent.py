"""
Main module for the SQL Agent.

This module initializes and runs the SQL agent, which processes user questions,
selects appropriate databases, generates and executes SQL queries, and constructs
natural language responses.
"""

from langchain_ollama import OllamaLLM
from langchain_core.runnables import RunnableLambda, Runnable
from langchain_community.utilities import SQLDatabase

import os
import re
import time
import glob
from typing import Dict, Tuple, Optional
from sqlalchemy.exc import OperationalError  

from .utils_sql import PromptFactory, SQLAgentContext

SQL_QUERY_TRYING_LIMIT = 2
LLM_MODEL = "gemma3:27b"
#LLM_MODEL = "weitsung50110/llama-3-taiwan:8b-instruct-dpo-q4_K_M"
#LLM_MODEL = "cwchang/llama3-taide-lx-8b-chat-alpha1:q4_k_m"

SQL_LLM = "gemma3:27b"
#SQL_LLM = "weitsung50110/llama-3-taiwan:8b-instruct-dpo-q4_K_M"
#SQL_LLM = "cwchang/llama3-taide-lx-8b-chat-alpha1:q4_k_m"
#SQL_LLM = "duckdb-nsql:latest"


def init_model() -> Tuple[OllamaLLM, OllamaLLM]:
    """
    Initializes the LLM models for general tasks and SQL generation.

    Returns:
        Tuple[OllamaLLM, OllamaLLM]: A tuple containing the main LLM and the SQL-specific LLM.
    """
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
    """
    Checks if the SQL query contains potentially dangerous operations.

    Args:
        query (str): The SQL query string to check.

    Returns:
        bool: True if the query contains disallowed keywords (e.g., DELETE, DROP), False otherwise.
    """
    if any(keyword in query.upper() for keyword in ["DELETE FROM", "UPDATE", "INSERT INTO", "DROP TABLE", "ALTER TABLE", "CREATE TABLE"]):
        return True
    return False

def _parse_sql_query(raw_query: str) -> str:
    """
    Parses a raw string to extract a clean SQL query,
    removing markdown code blocks if present.

    Args:
        raw_query (str): The raw string output from the LLM, which may contain markdown formatting.

    Returns:
        str: The extracted and cleaned SQL query string.
    """
    # Match content inside ```sql ... ``` or ``` ... ```
    match = re.search(r"```(?:sql)?\n(.*?)\n```", raw_query, re.DOTALL)
    if match:
        return match.group(1).strip()
    return raw_query.strip().strip('`').strip()


def start_sql_agent(db_dir_path: str, additional_description: str) -> None:
    """
    Initializes and starts the SQL agent, which can handle either a single database file
    or a directory of databases.

    Args:
        db_dir_path (str): The path to the database file or directory containing database files.
        additional_description (str): A description of the database schema or additional context for the agent.

    Returns:
        None
    """
    llm, sqlm = init_model()

    if not os.path.exists(db_dir_path):
        print(f"Cannot find database path: {db_dir_path}")
        exit(0)

    db_files = []
    if os.path.isdir(db_dir_path):
        db_files = glob.glob(os.path.join(db_dir_path, '*.db'))
        if not db_files:
            print(f"No .db files found in directory: {db_dir_path}")
            exit(0)
    elif os.path.isfile(db_dir_path):
        db_files = [db_dir_path]
    
    # A dictionary to cache SQLDatabase objects
    db_connections: Dict[str, SQLDatabase] = {}

    def get_db_connection(db_file_path: str) -> SQLDatabase:
        """
        Retrieves or creates a connection to the specified SQLite database file.

        Args:
            db_file_path (str): The path to the database file.

        Returns:
            SQLDatabase: The LangChain SQLDatabase object for the given file.
        """
        if db_file_path not in db_connections:
            #print(f"\nConnecting to database: '{db_file_path}'")
            db_connections[db_file_path] = SQLDatabase.from_uri(
                f"sqlite:///{db_file_path}",
                sample_rows_in_table_info=1
            )
        return db_connections[db_file_path]

    def run_sql_query(context: SQLAgentContext) -> SQLAgentContext:
        """
        Executes the generated SQL query and handles errors with automatic retry/correction.

        Args:
            context (SQLAgentContext): The current execution context containing the query and database connection.

        Returns:
            SQLAgentContext: The updated context with the query execution result.
        """
        query = context.query
        db = context.db
        
        if check_sql_query(query):
            return "Error: The generated SQL query contains disallowed operations (e.g., DELETE, UPDATE, INSERT, DROP, ALTER, CREATE). Please refine your request to only retrieve data."

        query = _parse_sql_query(query)

        for _ in range(SQL_QUERY_TRYING_LIMIT):
            try:
                context.result = db.run(query)
                return context
            except (OperationalError, Exception) as e:
                print("\nEncountered a SQL error. Attempting to correct the query...")
                
                correction_chain = (
                    PromptFactory.create_sql_correction_prompt()
                    | sqlm
                )
                
                # The correction chain can now access 'llm' from the outer scope
                query = correction_chain.invoke({
                    "input": context.user_input,
                    "db_schema": context.db_schema,
                    "failing_query": query,
                    "error_message": str(e)
                })
                print(f"\n[Correcting] Generated SQL Query: \n{query}")

        context.result = f"Error occurred when executing query: '{query}' with error: {str(e) if 'e' in locals() else 'Unknown error'}"
        return context

    def start_query_system(chain: Runnable, is_multi_db: bool) -> None:
        """
        Starts the interactive command-line query loop.

        Args:
            chain (Runnable): The LangChain runnable chain to process user queries.
            is_multi_db (bool): Indicates if multiple databases are available (enables database selection).

        Returns:
            None
        """
        while True:
            user_input = input("(Enter 'bye'/'exit' to exit the query system) Enter a question: ") 
            if user_input.lower() in ('bye', 'exit', 'clear'):
                print("Thanks for using, bye!")
                break

            if user_input.strip():                
                context = SQLAgentContext(
                    user_input=user_input,
                    schema_description=additional_description
                )
                if is_multi_db:
                    context.db_names = [os.path.basename(f) for f in db_files]
                
                chain.invoke(context)
                print("\n")
            else:
                print("Please enter a valid question!")

    # This chain selects the correct DB file path from a list of names
    db_router_chain = (
        RunnableLambda(lambda ctx: {"db_names": ctx.db_names, "input": ctx.user_input})
        | PromptFactory.create_db_selection_prompt()
        | llm
        | (lambda selected_db_name: os.path.join(db_dir_path, selected_db_name.strip()))
    )

    # This is the main chain that processes the user's request.
    db_chain = (
        RunnableLambda(lambda ctx: setattr(ctx, 'db_path', db_router_chain.invoke(ctx) if len(db_files) > 1 else db_files[0]) or ctx)
        | RunnableLambda(lambda ctx: print(f"\n[Debug][{time.time() - ctx.start_time:.2f}s] Selected DB Path: {ctx.db_path}") or ctx)
        | RunnableLambda(lambda ctx: setattr(ctx, 'db', get_db_connection(ctx.db_path)) or ctx)
        | RunnableLambda(lambda ctx: setattr(ctx, 'db_schema', ctx.db.get_table_info()) or ctx)
        | RunnableLambda(
            lambda ctx: setattr(ctx, 'query', (
                    RunnableLambda(lambda c: {"input": c.user_input, "db_schema": c.db_schema, "schema_description": c.schema_description})
                    | PromptFactory.create_sql_generation_prompt()
                    | sqlm
                ).invoke(ctx)
            ) or ctx
        )
        | RunnableLambda(lambda ctx: print(f"\n[Debug][{time.time() - ctx.start_time:.2f}s] Generated SQL Query: \n{ctx.query}") or ctx)
        | RunnableLambda(run_sql_query)
        | RunnableLambda(lambda ctx: print(f"\n[Debug][{time.time() - ctx.start_time:.2f}s] SQL Query Result: {ctx.result}") or ctx)
        | RunnableLambda(
            lambda ctx: setattr(ctx, 'final_response', (
                    RunnableLambda(lambda c: {"input": c.user_input, "query": c.query, "result": c.result})
                    | PromptFactory.create_answer_generation_prompt() 
                    | llm
                ).invoke(ctx)
            ) or ctx
        )
        | RunnableLambda(lambda ctx: print(f"\n[Debug][{time.time() - ctx.start_time:.2f}s] Final LLM Response: {ctx.final_response}") or ctx)
    )

    start_query_system(db_chain, is_multi_db=len(db_files) > 1)
