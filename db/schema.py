from sqlalchemy import Integer, Text, BigInteger, create_engine, Float
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped, sessionmaker
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

db = str(os.getenv("CONNECTION_STRING"))

engine = create_engine(db)

Session = sessionmaker(bind=engine)

def copy_data_to_postgres(csv_path: str, table_name: str, start_id: int):
    """Copy data from a CSV file to a PostgreSQL table
    Add an id column if it doesn't exist, ensure IDs are unique and not null"""

    # Load CSV data into a pandas DataFrame
    df = pd.read_csv(csv_path)
    row_count = len(df)
    print(f"{csv_path} loaded into pandas DataFrame")

    # Always generate new sequential IDs starting from the provided start_id
    df['id'] = range(start_id, start_id + row_count)
    df.to_csv(csv_path, index=False)
    print(f"Generated IDs from {start_id} to {start_id + row_count - 1}")

    # Copy data to PostgreSQL
    with engine.begin() as connection:
        cursor = connection.connection.cursor()
        with open(csv_path, 'r') as file:
            sql_copy_statement: str = f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER AS ','"
            cursor.copy_expert(sql_copy_statement, file)
            print("Data copied to PostgreSQL")

    return df

def main():
    Base.metadata.drop_all(engine)
    print("Tables dropped")
    Base.metadata.create_all(engine)
    print("Tables created")
    table_name = "bdc_info_sqlalchemy"

    #Loop through all the CSV files in the exports folder
    directory = os.listdir('exports')
    print(f"Found {len(directory)} files in exports directory")

    # Keep track of the next available ID
    next_id = 1

    for file in directory:
        if file.endswith('.csv'):
            try:
                csv_path = f'exports/{file}'
                print(f"Processing {csv_path}")
                # Process file and get number of rows processed
                rows_processed = copy_data_to_postgres(csv_path, table_name, next_id)
                # Update next_id for the next file
                next_id += len(rows_processed)
                print(f"Next ID will be {next_id}")
            except Exception as e:
                print(f"Error processing {file}: {str(e)}")

class Base(DeclarativeBase):
    pass

class BDCInfo(Base):
    __tablename__ = 'bdc_info_sqlalchemy'

    frn: Mapped[int | None] = mapped_column(Integer, nullable=True)
    provider_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    brand_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    location_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    technology: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_advertised_download_speed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_advertised_upload_speed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    low_latency: Mapped[int | None] = mapped_column(Integer, nullable=True)
    business_residential_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    state_usps: Mapped[str | None] = mapped_column(Text, nullable=True)
    block_geoid: Mapped[int | None] = mapped_column(Float, nullable=True)
    h3_res8_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    id : Mapped[int | None] = mapped_column(Integer, primary_key=True, autoincrement=True)


if __name__ == "__main__":
    main()
