#https://python.langchain.com/docs/tutorials/sql_qa/

import os
from dotenv import load_dotenv
from langchain_community.utilities import SQLDatabase
from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool
from langchain.chains import create_sql_query_chain
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langgraph.prebuilt import create_react_agent
from postgres_db import DBConnection
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
import pandas as pd

load_dotenv()
openai_api_key = os.getenv("OPENAI_API_KEY")
connection_string = os.getenv("CONNECTION_STRING")

llm = ChatOpenAI(model="gpt-4o-mini")

db = DBConnection(connection_string)

table_name = 'bdc_info'

def get_schema(_):
    return db.get_table_schema(table_name)

template = """You are an agent designed to interact with a SQL database.
Given an input question, create a syntactically correct PostgreSQL query to run.
Unless the user specifies a specific number of examples they wish to obtain, always limit your query to at most 5 results.
You can order the results by a relevant column to return the most interesting examples in the database.
Never query for all the columns from a specific table, only ask for the relevant columns given the question.
DO NOT make any DML statements (INSERT, UPDATE, DELETE, DROP etc.) to the database.
DO NOT start with ```sql, just start with the query.

Here is an example of what the response should look like: SELECT column FROM table ORDER BY column DESC LIMIT 5

Schema: {schema}

Question: {question}"""

prompt = ChatPromptTemplate.from_template(template)

sql_response = RunnablePassthrough.assign(schema=get_schema) | prompt | llm | StrOutputParser()

question = "How do download speeds differ between business_residential_code values?"

response = sql_response.invoke({"question": question})

print(response)

def execute_query(query: str):
    try:
        db.cur.execute(query)
        results = db.cur.fetchall()
        columns = [desc[0] for desc in db.cur.description]
        df = pd.DataFrame(results, columns=columns)
        return df
    except Exception as e:
        print(f"Error executing query: {e}")
        return None
    finally:
        db.cleanup()

print(execute_query(response))

answer_template = """Here are the results for the query: {query}"""

prompt2 = ChatPromptTemplate.from_template(template)
