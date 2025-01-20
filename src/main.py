import asyncio
import json
import sys

from bdc_api import BDC
from utils import extractZip

sys.path.append("./")
from db.postgres_db import DBConnection
from db.schema import Base, copy_data_to_postgres, engine


async def main():
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

    # Get the download list for a specific date
    date: str = input("Enter a date (eg.2023-12-31): ")
    if date not in [str(i) for i in range(1, len(dates) + 1)]:
        print("Invalid date")

    date = dates[int(date)]

    # Drop and create tables
    Base.metadata.drop_all(engine)
    print("Tables dropped")
    Base.metadata.create_all(engine)
    print("Tables created")
    start_id: int = 1
    downloadList: list[dict] = await bdc.getDownloadList(date=date, category="State")
    print("Download List:")
    for file in downloadList[:5]:
        print(json.dumps(file, indent=4, sort_keys=True))

        file_id = file["file_id"]
        response = await bdc.getDownloadFile(file_id)
        extractZip(response=response, file_id=file["file_id"])
        filename = f"{file['file_name']}.csv"
        print(f"Extracted {filename}")
        db = DBConnection()
        table_name = "bdc_info_sqlalchemy"

        copy_data_to_postgres(f"exports/{filename}", table_name, start_id)

        start_id += db.get_rowcount(table_name)
        print("---" * 10)


if __name__ == "__main__":
    asyncio.run(main=main())
