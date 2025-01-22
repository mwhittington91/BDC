import pandas as pd
from sqlalchemy import Engine, Float, Integer, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def copy_data_to_postgres(
    engine: Engine, csv_path: str, table_name: str, start_id: int
) -> pd.DataFrame:
    """Add 'id' column to CSV and copy data from a CSV file to a PostgreSQL table
    args:
        engine: Engine - SQLAlchemy engine
        csv_path: str - path to the CSV file
        table_name: str - name of the PostgreSQL table
        start_id: int - starting ID for the 'id' column
    returns:
        pandas DataFrame - the data from the CSV file
    """

    # Load CSV data into a pandas DataFrame
    df = pd.read_csv(csv_path)
    row_count = len(df)
    print(f"{csv_path} loaded into pandas DataFrame")

    # Always generate new sequential IDs starting from the provided start_id
    df["id"] = range(start_id, start_id + row_count)
    df.to_csv(csv_path, index=False)
    print(f"Generated IDs from {start_id} to {start_id + row_count - 1}")

    # Copy data to PostgreSQL
    with engine.begin() as connection:
        cursor = connection.connection.cursor()
        with open(csv_path, "r") as file:
            sql_copy_statement: str = (
                f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER AS ','"
            )
            cursor.copy_expert(sql_copy_statement, file)
            print("Data copied to PostgreSQL")

    return df


class Base(DeclarativeBase):
    pass


class BDCInfo(Base):
    __tablename__ = "bdc_info_sqlalchemy"

    frn: Mapped[int | None] = mapped_column(Integer, nullable=True)
    provider_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    brand_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    location_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    technology: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_advertised_download_speed: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    max_advertised_upload_speed: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    low_latency: Mapped[int | None] = mapped_column(Integer, nullable=True)
    business_residential_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    state_usps: Mapped[str | None] = mapped_column(Text, nullable=True)
    block_geoid: Mapped[int | None] = mapped_column(Float, nullable=True)
    h3_res8_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    id: Mapped[int | None] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
