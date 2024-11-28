from sqlalchemy import Integer, Text, BigInteger, create_engine
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped, sessionmaker
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

db = str(os.getenv("CONNECTION_STRING"))

engine = create_engine(db)

Session = sessionmaker(bind=engine)

def copy_data_to_postgres(csv_path: str, table_name: str):
    """Copy data from a CSV file to a PostgreSQL table
    Add an id column if it doesn't exist"""

    # Load CSV data into a pandas DataFrame
    df = pd.read_csv(csv_path)
    print("Data loaded into pandas DataFrame")
    # Create id column if it doesn't exist
    if 'id' not in df.columns:
        df['id'] = range(1, len(df) + 1)
        df.to_csv(csv_path, index=False)
        print("id column added to CSV")

    with engine.begin() as connection:
        cursor = connection.connection.cursor()  # Access the raw psycopg2 connection
        with open(csv_path, 'r') as file:
            sql_copy_statement: str = f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER AS ','"
            cursor.copy_expert(sql_copy_statement, file)
            print("Data copied to PostgreSQL")


def main():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    print("Tables created")
    csv_path = "exports/bdc_01_Cable_fixed_broadband_D23_14may2024.csv"
    table_name = "bdc_info_sqlalchemy"
    copy_data_to_postgres(csv_path, table_name)


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
    block_geoid: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    h3_res8_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    id : Mapped[int | None] = mapped_column(Integer, primary_key=True, autoincrement=True)


if __name__ == "__main__":
    main()
