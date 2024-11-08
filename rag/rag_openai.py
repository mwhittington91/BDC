import os
from dotenv import load_dotenv
from openai import OpenAI
import json
from tools import get_table_schema_tool

load_dotenv()
openai_api_key = os.getenv("OPENAI_API_KEY")
connection_string = os.getenv("CONNECTION_STRING")

client = OpenAI(api_key=openai_api_key)

def chat_response(question: str, tools: list = []):
    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {
                "role": "user",
                "content": question
            }
        ], tools=tools
    )

    return completion.choices[0].message

question = "what is the schema of the bdc_info table?"
response = chat_response(question, tools=[get_table_schema_tool])
# print(response)
tool_call = response.tool_calls[0]
print(tool_call)
arguments = json.loads(tool_call.function.arguments)
print(arguments)
table_name = arguments["table_name"]
print(table_name)
