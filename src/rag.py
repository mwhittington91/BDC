#https://python.langchain.com/docs/tutorials/sql_qa/

from langchain_community.utilities import SQLDatabase
from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool
from langchain.chains import create_sql_query_chain
import os
from dotenv import load_dotenv
import psycopg2
from langchain_openai import ChatOpenAI

load_dotenv()
openai_api_key = os.getenv("OPENAI_API_KEY")
connection_string = os.getenv("CONNECTION_STRING")

llm = ChatOpenAI(model="gpt-4o-mini")

db = SQLDatabase.from_uri(connection_string)

question = "What states (state_usps) have the most entries in the table?"
# execute_query = QuerySQLDataBaseTool(db=db)
# write_query = create_sql_query_chain(llm, db)
# chain = write_query | execute_query
# response = chain.invoke({"question": question})
# print(response)

from langchain_community.agent_toolkits import SQLDatabaseToolkit

toolkit = SQLDatabaseToolkit(db=db, llm=llm)

tools = toolkit.get_tools()

from langchain_core.messages import SystemMessage

SQL_PREFIX = """You are an agent designed to interact with a SQL database.
Given an input question, create a syntactically correct SQLite query to run, then look at the results of the query and return the answer.
Unless the user specifies a specific number of examples they wish to obtain, always limit your query to at most 5 results.
You can order the results by a relevant column to return the most interesting examples in the database.
Never query for all the columns from a specific table, only ask for the relevant columns given the question.
You have access to tools for interacting with the database.
Only use the below tools. Only use the information returned by the below tools to construct your final answer.
You MUST double check your query before executing it. If you get an error while executing a query, rewrite the query and try again.

DO NOT make any DML statements (INSERT, UPDATE, DELETE, DROP etc.) to the database.

To start you should ALWAYS look at the tables in the database to see what you can query.
Do NOT skip this step.
Then you should query the schema of the most relevant tables."""

system_message = SystemMessage(content=SQL_PREFIX)

from langchain_core.messages import HumanMessage
from langgraph.prebuilt import create_react_agent

agent_executor = create_react_agent(llm, tools, messages_modifier=system_message)

for s in agent_executor.stream(
    {"messages": [HumanMessage(content=question)]}
):
    print(s)
    print("----")
