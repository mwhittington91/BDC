import pandas as pd
import psycopg2
from pandas.io.sql import get_schema


class DBConnection:
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.conn = psycopg2.connect(connection_string)
        self.cur = self.conn.cursor()

    def __str__(self) -> str:
        return f'{self.conn.get_dsn_parameters()}, "\n"'

    def create_table_from_CSV(self, csv_path: str, table_name: str):
        df = pd.read_csv(csv_path)

        schema: str = get_schema(df, table_name)

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

    def execute_query(self, query: str) -> pd.DataFrame | None:
        try:
            self.cur.execute(query)
            results = self.cur.fetchall()
            if self.cur.description:
                columns = pd.Index([desc[0] for desc in self.cur.description])
                df = pd.DataFrame(results, columns=columns)
                return df
            else:
                return None
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    def get_rowcount(self, table_name: str) -> int:
        try:
            self.cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            result = self.cur.fetchone()
            if result is not None:
                count: int = result[0]
                return count
            else:
                raise psycopg2.Error("No results returned from count query")
        except psycopg2.Error as e:
            print("Error connecting to PostgreSQL:", e)
            raise e
