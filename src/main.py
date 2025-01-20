import asyncio
import json

from bdc_api import BDC
from utils import extractZip


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

    downloadList: list[dict] = await bdc.getDownloadList(date=date, category="State")
    print("Download List:")
    for file in downloadList[:5]:
        print(json.dumps(file, indent=4, sort_keys=True))

    for file in downloadList[:5]:
        file_id = file["file_id"]
        response = await bdc.getDownloadFile(file_id)
        extractZip(response=response, file_id=file["file_id"])
        print(f"Extracted {file['file_name']}")


if __name__ == "__main__":
    asyncio.run(main=main())
