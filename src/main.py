import asyncio
from src.bdc_api import BDC

async def main():
    bdc = BDC()
    dates = await bdc.getlistofDates()
    # Seperate the data_type and dates
    data_type, dates = dates
    print(f"Data Type: {data_type}")
    print("Dates:")
    for date in dates:
        print(date)

    date = input(prompt="Enter a date (eg.2023-12-31): ")
    # downloadList = await bdc.getDownloadList(date=date)
    # await bdc.downloadFiles(downloadList)


if __name__ == "__main__":
    asyncio.run(main=main())
