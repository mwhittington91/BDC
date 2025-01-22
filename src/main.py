import asyncio
import json
import logging
import os
import sys

from dotenv import load_dotenv
from tqdm import tqdm

from bdc_api import BDC
from utils import extractZip, upload_file_to_zapier

sys.path.append("./")
from sqlalchemy import create_engine

from db.postgres_db import DBConnection
from db.schema import Base, copy_data_to_postgres

load_dotenv()

ZAPIER_WEBHOOK = str(os.getenv("ZAPIER_WEBHOOK"))

CONNECTION_STRING = str(os.getenv("UBUNTU_CONNECTION_STRING"))


logging.basicConfig(level=logging.ERROR)


async def main():
    engine = create_engine(CONNECTION_STRING)


    # Initialize the BDC class
    bdc = BDC()

    # Get the list of dates
    dates = await bdc.getlistofDates()

    # Seperate the data_type and dates
    data_type, dates = dates
    print(f"Data Type: {data_type}")
    print("Dates:")
    for date in dates:
        print(f"for {date} enter {dates.index(date) + 1}")

    # Get the date from the user from the index of the dates list
    date: str = input("Enter a date (eg.2023-12-31): ")
    if date not in [str(i) for i in range(1, len(dates) + 1)]:
        raise ValueError("Invalid date")
        sys.exit(1)

    # Convert the index to the date
    date: str = dates[int(date) - 1]

    # Drop and create tables
    Base.metadata.drop_all(engine)
    print("Tables dropped")
    Base.metadata.create_all(engine)
    print("Tables created")

    # Start ID for the 'id' column
    start_id: int = 1

    # Get the download list for a specific date
    downloadList: list[dict] = await bdc.getDownloadList(date=date, category="State")
    print("Download List:")
    for file in tqdm(downloadList[:5]):
        logging.info(json.dumps(file, indent=4, sort_keys=True))

        file_id = file["file_id"]
        response = await bdc.getDownloadFile(file_id)
        extractZip(response=response, file_id=file["file_id"])
        filename = f"{file['file_name']}"
        print(f"Extracted {filename}")

        upload_file_to_zapier(f"exports/{filename}.csv", ZAPIER_WEBHOOK, filename)

        db = DBConnection(CONNECTION_STRING)
        table_name = "bdc_info_sqlalchemy"

        copy_data_to_postgres(engine, "exports/{filename}.csv", table_name, start_id)

        start_id += db.get_rowcount(table_name)

        os.remove(f"exports/{filename}.csv")
        print(f"Deleted {filename}")

        print("-----" * 10)


if __name__ == "__main__":
    asyncio.run(main=main())
