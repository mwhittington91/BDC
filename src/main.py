import asyncio
import json

from bdc_api import BDC


async def main():
    bdc = BDC()
    dates = await bdc.getlistofDates()
    # Seperate the data_type and dates
    data_type, dates = dates
    print(f"Data Type: {data_type}")
    print("Dates:")
    for date in dates:
        print(date)

    date: str = input("Enter a date (eg.2023-12-31): ")
    if date not in dates:
        print("Invalid date")

    downloadList = await bdc.getDownloadList(date=date)
    print("Download List:")
    for file in downloadList[:5]:
        print(json.dumps(file, indent=4, sort_keys=True))

    for file in downloadList[:5]:
        await bdc.downloadFiles(file["file_id"])


if __name__ == "__main__":
    asyncio.run(main=main())
