from langchain_ollama import OllamaLLM
from langchain_core.callbacks.streaming_stdout import StreamingStdOutCallbackHandler
from sqlalchemy.exc import OperationalError  
from langchain_core.runnables import RunnablePassthrough
from langchain_community.utilities import SQLDatabase  

from utils_sql import PromptFactory
import os

DATABASE_PATH = './xlsx_jsonl_data_p1.db'
SQL_QUERY_TRYING_LIMIT = 3
LLM_MODEL = "gemma3:27b"
SCHEMA_DESCRIPTION = """
    請注意 :
    - '姓氏' 欄位實際代表「單位／處室名稱」，並非人名，請以單位為統計依據，若為空值，請在統計時獨立歸類為「未知單位」。
    - '彩色頁面' 欄位代表列印的彩色頁面數量。
    - '黑白頁面' 欄位代表列印的黑白頁面數量。
    """  


def init_model():
    llm = OllamaLLM(
        model = LLM_MODEL,
        callbacks = [StreamingStdOutCallbackHandler()]
    )
    return llm

def get_db_schema(_):
    table_info = db.get_table_info()
    return table_info + SCHEMA_DESCRIPTION

def run_sql_query(context_dict):
    query = context_dict["query"]
    original_input = context_dict["input"]
    db_schema = context_dict["db_schema"]
    
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
            
            query = correction_chain.invoke({
                "input": original_input,
                "db_schema": db_schema,
                "failing_query": query,
                "error_message": str(e)
            })
    return f"Error occurred when executing query: '{query}' with error: {str(e) if 'e' in locals() else 'Unknown error'}"

def check_sql_query(query: str) -> bool:
    if any(keyword in query.upper() for keyword in ["DELETE FROM", "UPDATE", "INSERT INTO", "DROP TABLE", "ALTER TABLE", "CREATE TABLE"]):
        return True
    return False

def test_query_system():
    while True:
        user_input = input("(Enter 'bye'/'exit' to exit the query system) Enter a question: ") 
        if user_input.lower() in ('bye', 'exit'):
            print("Thanks for using, bye!")
            break

        if user_input.strip():
            db_chain.invoke({'input': user_input}) 
            print("\n")
        else:
            print("Please enter a valid question!")
            

if __name__ == "__main__":
    llm = init_model()

    if not os.path.exists(DATABASE_PATH):
        print(f"cannot find database file: {DATABASE_PATH}")
        exit(0)

    db = SQLDatabase.from_uri(
        f"sqlite:///{DATABASE_PATH}",
        sample_rows_in_table_info = 1
    )

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

    test_query_system()