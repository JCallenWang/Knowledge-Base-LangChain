from langchain_core.prompts import ChatPromptTemplate  

class PromptFactory:
    """
    A factory class for creating reusable and parameterized ChatPromptTemplates.
    """

    @staticmethod
    def _create_prompt(system_message: str, user_message: str) -> ChatPromptTemplate:
        return ChatPromptTemplate.from_messages([
            ('system', system_message),
            ('user', user_message)
        ])

    @classmethod
    def create_sql_generation_prompt(cls) -> ChatPromptTemplate:
        system_message = '請根據提供的SQL資料庫結構與範例，生成SQL的查詢語法: {db_schema}{schema_description}'
        user_message = ('使用者提問: "{input}"。\n請根據以下規則生成SQL查詢語法: '
                        '只生成SQL語法，不要添加額外解釋，不要用Markdown語法包裝SQL語法')
        return cls._create_prompt(system_message, user_message)

    @classmethod
    def create_sql_correction_prompt(cls) -> ChatPromptTemplate:
        system_message = '你之前生成了一個SQL查詢，但執行失敗了。請根據提供的SQL資料庫結構、原始使用者提問、失敗的SQL查詢以及錯誤訊息，生成一個修正後的SQL查詢語法。'
        user_message = ('原始使用者提問: "{input}"\n'
                        'SQL資料庫結構: {db_schema}\n'
                        '失敗的SQL查詢: "{failing_query}"\n'
                        '錯誤訊息: "{error_message}"\n'
                        '請根據以上資訊，生成修正後的SQL查詢語法。只生成SQL語法，不要添加額外解釋，不要用Markdown語法包裝SQL語法。')
        return cls._create_prompt(system_message, user_message)

    @classmethod
    def create_answer_generation_prompt(cls) -> ChatPromptTemplate:
        template = ("請根據以下資訊生成自然語言回應：\n"
                    "- 問題: {input}\n"
                    "- SQL 查詢: {query}\n"
                    "- 查詢結果: {result}\n\n"
                    "請提供簡潔有、具有說明性的回應。")
        return ChatPromptTemplate.from_template(template)