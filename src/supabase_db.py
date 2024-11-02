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

    conn = psycopg2.connect(connection_string)

    #print(conn.get_dsn_parameters(), "\n")

    # Create a cursor object
    cur = conn.cursor()

    try:
        # Verify data was inserted
        cur.execute(schema)
        conn.commit()
        print(f"Successfully created table {table_name}")

        return True

    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL:", e)
        conn.rollback()
        return False

    finally:
        # Close cursor and connection
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()


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
def insert_data_from_CSV(csv_path: str):
    df = pd.read_csv(csv_path)
    data =df.to_dict(orient='records')

    try:
        supabase.table("BDC_Info").insert(data).execute()
        print("Data inserted successfully")
    except Exception as e:
        print("Error inserting data:", e)
        raise e


def copy_data_to_postgres(csv_path: str, table_name: str):
    # Establish connection
    conn = psycopg2.connect(connection_string)

    #print(conn.get_dsn_parameters(), "\n")

    # Create a cursor object
    cur = conn.cursor()

    try:
        # Execute a query
        with open(csv_path, 'r') as file:
            sql = f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER AS ','"
            cur.copy_expert(sql, file)

        # Commit the transaction
        conn.commit()

        # Verify data was inserted
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]
        print(f"Successfully inserted {count} rows")

        return True

    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL:", e)
        conn.rollback()
        return False

    finally:
        # Close cursor and connection
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
