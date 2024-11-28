import os
from supabase import create_client, Client
from dotenv import load_dotenv
import asyncio
import pandas as pd
from tqdm import tqdm
import psycopg2

load_dotenv()
# Environment variables
url: str|None = os.getenv("SUPABASE_URL")
key: str|None = os.getenv("SUPABASE_KEY")
connection_string: str|None = os.getenv("CONNECTION_STRING")

# Create a Supabase client
supabase: Client = create_client(url, key)


def get_all_data(table_name: str):
    data = supabase.table(table_name).select("*").execute()
    return data

def create_table_from_CSV(csv_path: str, table_name: str):
    df = pd.read_csv(csv_path)

    schema: str = pd.io.sql.get_schema(df, table_name)

    print(schema)

    try:
        # Verify data was inserted
        supabase.rpc("execute_query", {"query": schema}).execute()
        print(f"Successfully created table {table_name}")

        return True
    except Exception as e:
        print("Error connecting to Supabase:", e)
        return False

def dataFrametoSupabase(
    df: pd.DataFrame, table_name: str):

    try:
        results = []
        # rows = [row for _, row in df.iterrows()]
        for _, row in tqdm(df.iterrows(), total=df.shape[0]):
            data = row.to_dict()
            result = supabase.table(table_name).insert(data).execute()
            results.append(result)
        print(f"{len(results)} inserted successfully")
        return results
    except Exception as e:
        print(f"Error inserting data: {str(e)}")
        raise e

#insert data from CSV to Supabase
def insert_data_from_CSV(csv_path: str, table_name: str):
    df = pd.read_csv(csv_path)
    data =df.to_dict(orient='records')

    try:
        supabase.table(table_name).insert(data).execute()
        print("Data inserted successfully")
    except Exception as e:
        print("Error inserting data:", e)
        raise e


def get_table_schema(table_name: str):
    schema = supabase.rpc("get_table_schema", {"p_table_name": table_name}).execute()
    # Turn into a DataFrame
    schema_df = pd.DataFrame(schema.data)
    return schema_df, table_name
