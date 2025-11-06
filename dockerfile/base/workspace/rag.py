from langchain_community.document_loaders import UnstructuredExcelLoader

FILE_PATH = "AI_DB_data_clean_random_p1.xlsx"

loader = UnstructuredExcelLoader("your_excel_file.xlsx", mode="elements")
docs = loader.load()