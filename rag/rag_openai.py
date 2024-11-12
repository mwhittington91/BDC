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
    # return completion.choices[0].message

question = "what is the schema of the bdc_info table?"
response = chat_response(question, tools=[get_table_schema_tool])
# print(response)
tool_call = response
#print(tool_call)
arguments = json.loads(tool_call.function.arguments)
print(arguments)
table_name = arguments["table_name"]
print(table_name)

db = DBConnection(connection_string)
schema = db.get_table_schema(table_name)
print(schema)

sql_prompt = sql_template.format(schema=schema, question=question)
question2 = "How do download speeds differ between business_residential_code values?"
sql_response = chat_response(question=question2, system_prompt=sql_prompt)
query = sql_response
results = db.execute_query(query)
print(results)

answer = answer_template.format(query=query, results=results, question=question2)
print(answer)
print("---")

rag_answer = chat_response(question=answer)
print(rag_answer)
