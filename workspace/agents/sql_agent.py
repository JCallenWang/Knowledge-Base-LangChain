from langchain_ollama import OllamaLLM
from langchain_core.callbacks.streaming_stdout import StreamingStdOutCallbackHandler
from sqlalchemy.exc import OperationalError  
from langchain_core.runnables import RunnablePassthrough
from langchain_community.utilities import SQLDatabase

from .utils_sql import PromptFactory
import os

SQL_QUERY_TRYING_LIMIT = 2
LLM_MODEL = "gemma3:27b"

def init_model():
    llm = OllamaLLM(
        model = LLM_MODEL,
        callbacks = [StreamingStdOutCallbackHandler()]
    )
    return llm

def check_sql_query(query: str) -> bool:
    if any(keyword in query.upper() for keyword in ["DELETE FROM", "UPDATE", "INSERT INTO", "DROP TABLE", "ALTER TABLE", "CREATE TABLE"]):
        return True
    return False
            
def start_sql_agent(db_path: str, schema_description: str):
    llm = init_model()

    if not os.path.exists(db_path):
        print(f"cannot find database file: {db_path}")
        exit(0)

    db = SQLDatabase.from_uri(
        f"sqlite:///{db_path}",
        sample_rows_in_table_info = 1
    )

    def get_db_schema(_):
        return db.get_table_info()

    def run_sql_query(context_dict):
        query = context_dict["query"]
        
        if check_sql_query(query):
            return "Error: The generated SQL query contains disallowed operations (e.g., DELETE, UPDATE, INSERT, DROP, ALTER, CREATE). Please refine your request to only retrieve data."

        for _ in range(SQL_QUERY_TRYING_LIMIT):
            try:
                return db.run(query)
            except (OperationalError, Exception) as e:
                print("\nEncountered a SQL error. Attempting to correct the query...")
                
                correction_chain = (
                    PromptFactory.create_sql_correction_prompt()
                    | llm
                )
                
                # The correction chain can now access 'llm' from the outer scope
                query = correction_chain.invoke({
                    "input": context_dict["input"],
                    "db_schema": context_dict["db_schema"],
                    "failing_query": query,
                    "error_message": str(e)
                })
        return f"Error occurred when executing query: '{query}' with error: {str(e) if 'e' in locals() else 'Unknown error'}"

    def test_query_system(chain):
        while True:
            user_input = input("(Enter 'bye'/'exit' to exit the query system) Enter a question: ") 
            if user_input.lower() in ('bye', 'exit'):
                print("Thanks for using, bye!")
                break

            if user_input.strip():
                chain.invoke({'input': user_input, 'schema_description': schema_description}) 
                print("\n")
            else:
                print("Please enter a valid question!")

    gen_query_chain = (
        PromptFactory.create_sql_generation_prompt()
        | llm
    )

    db_chain = (
        RunnablePassthrough.assign(db_schema=get_db_schema)
        | RunnablePassthrough.assign(query=gen_query_chain)
        | RunnablePassthrough.assign(result=lambda x: run_sql_query(x))
        | PromptFactory.create_answer_generation_prompt()
        | llm
    )   

    test_query_system(db_chain)