import os
from dotenv import load_dotenv
from openai import OpenAI
import json
from tools import get_table_schema_tool
from prompts import sql_template, answer_template
import sys
sys.path.append('./')
from src.postgres_db import DBConnection

load_dotenv()
openai_api_key = os.getenv("OPENAI_API_KEY")
connection_string = os.getenv("CONNECTION_STRING")

client = OpenAI(api_key=openai_api_key)

def chat_response(question: str,
    system_prompt: str = "You are a helpful assistant.",
    tools: list | None = None):
    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system",
                "content": system_prompt},
            {
                "role": "user",
                "content": question
            }
        ], tools=tools
    )
    if completion.choices[0].message.tool_calls is not None:
        return completion.choices[0].message.tool_calls[0]
    else:
        return completion.choices[0].message.content

def parse_tool_call(tool_call: str):
    return json.loads(tool_call.function.arguments)

db = DBConnection(connection_string)

def answerSQLQuestion(question: str, db: DBConnection):
    schema = db.get_table_schema("bdc_info")
    sql_prompt = sql_template.format(schema=schema, question=question)
    sql_response = chat_response(question=question, system_prompt=sql_prompt)
    results = db.execute_query(sql_response)
    answer = answer_template.format(query=sql_response, results=results, question=question)
    rag_answer = chat_response(question=answer)
    return rag_answer

question = input("Ask a question: ")
answer = answerSQLQuestion(question, db)
print("---"*20)
print(answer)
