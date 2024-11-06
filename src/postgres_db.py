import os
from dotenv import load_dotenv
import pandas as pd
from tqdm import tqdm
import psycopg2

load_dotenv()
# Environment variables
connection_string: str|None = os.getenv("CONNECTION_STRING")

class DBConnection:
    def __init__(self, connection_string: str = connection_string):
            self.connection_string = connection_string
            self.conn = psycopg2.connect(connection_string)
            self.cur = self.conn.cursor()

    def __str__(self) -> str:
        return  f'{self.conn.get_dsn_parameters()}, "\n"'

    def cleanup(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()

    def create_table_from_CSV(self, csv_path: str, table_name: str):
        df = pd.read_csv(csv_path)

        schema: str = pd.io.sql.get_schema(df, table_name)

        try:
            # Verify data was inserted
            self.cur.execute(schema)
            self.conn.commit()
            print(f"Successfully created table {table_name}")

            return True

        except psycopg2.Error as e:
            print("Error connecting to PostgreSQL:", e)
            self.conn.rollback()
            return False

        finally:
            self.cleanup()

    def copy_data_to_postgres(self, csv_path: str, table_name: str):
        try:
            # Execute a query
            with open(csv_path, 'r') as file:
                sql = f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER AS ','"
                self.cur.copy_expert(sql, file)

            # Commit the transaction
            self.conn.commit()

            # Verify data was inserted
            self.cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = self.cur.fetchone()[0]
            print(f"Successfully inserted {count} rows")

            return True

        except psycopg2.Error as e:
            print("Error connecting to PostgreSQL:", e)
            self.conn.rollback()
            return False

        finally:
            self.cleanup()

    def get_table_schema(self, table_name: str):
        # Replace with your table name
        query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table_name}';
        """

        try:
            self.cur.execute(query)
            records = self.cur.fetchall()
            schema = {record[0]: record[1] for record in records}
            return f"Schema for table {table_name}:\n{schema}"
        except psycopg2.Error as e:
            print("Error connecting to PostgreSQL:", e)
            return False

        # finally:
        #     self.cleanup()
