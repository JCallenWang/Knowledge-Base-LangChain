from langchain_ollama import OllamaLLM, OllamaEmbeddings
from langchain_core.callbacks.streaming_stdout import StreamingStdOutCallbackHandler
from sqlalchemy.exc import OperationalError  
from langchain_core.runnables import RunnablePassthrough
from langchain_core.prompts import ChatPromptTemplate  
from langchain_community.utilities import SQLDatabase  

DATABASE_NAME = './xlsx_jsonl_data_p1.db'

def init_model():
    llm = OllamaLLM(
        model = "gemma3:27b",
        #model = "weitsung50110/llama-3-taiwan:8b-instruct-dpo-q4_K_M",
        #callbacks = [StreamingStdOutCallbackHandler()]
    )
    ebm = OllamaEmbeddings(model = "weitsung50110/multilingual-e5-large-instruct:f16")
    return llm, ebm

def get_db_schema(_):
    table_info = db.get_table_info()
    custom_descriptions = """
    請注意 :
    - '姓氏' 欄位實際代表「單位／處室名稱」，並非人名，請以單位為統計依據，若為空值，請在統計時獨立歸類為「未知單位」。
    - '彩色頁面' 欄位代表列印的彩色頁面數量。
    - '黑白頁面' 欄位代表列印的黑白頁面數量。
    """  
    return table_info + custom_descriptions


def run_query(query):
    try:
        return db.run(query)
    except (OperationalError, Exception) as e:
        return f"Error occured when executing: {e}"

def start_query_system():
    while True:
        user_input = input("(輸入 'bye'/'exit' 離開詢問系統) 請輸入問題: ") 
        if user_input.lower() in ('bye', 'exit'):
            print("謝謝使用，再見!")
            break

        if user_input.strip():
            #_sql = gen_query_chain.invoke({'input': user_input})
            #print(run_query(_sql))
            print(db_chain.invoke({'input': user_input})) 
            print("\n")
        else:
            print("請輸入有效的問題!")
            


if __name__ == "__main__":
    llm, ebm = init_model()

    db = SQLDatabase.from_uri(
        f"sqlite:///{DATABASE_NAME}",
        sample_rows_in_table_info = 1
    )

    gen_sql_prompt = ChatPromptTemplate.from_messages([
        ('system', '請根據提供的SQL資料庫結構與範例，生成SQL的查詢語法: {db_schema}'),
        ('user', '使用者提問: "{input}"。\n請根據以下規則生成SQL查詢語法: '
                 '指生成SQL語法，不要添加額外解釋，不要用Markdown語法包裝SQL語法')
    ])

    gen_query_chain = (
        RunnablePassthrough.assign(db_schema=get_db_schema) 
        | gen_sql_prompt
        | llm
    )

    gen_answer_prompt = ChatPromptTemplate.from_template("""
        請根據以下資訊生成自然語言回應：
        - 問題: {input}
        - SQL 查詢: {query}
        - 查詢結果: {result}
        
        請提供簡潔的回應。""")


    db_chain = (
        RunnablePassthrough.assign(query=gen_query_chain).assign(result=lambda x: run_query(x["query"]))
        | gen_answer_prompt
        | llm
    )   


    start_query_system()