import asyncio
import json
import logging
import os
import sys

from dotenv import load_dotenv
from sqlalchemy import MetaData, create_engine, inspect, text
from tqdm import tqdm

from db.schema import copy_data_to_postgres, create_bdc_table
from src.bdc_api import BDC
from src.utils import check_file_size, extractZip, upload_file_to_zapier, zip_file

load_dotenv()

USERNAME = str(os.getenv("USERNAME"))
API_KEY = str(os.getenv("API_KEY"))

ZAPIER_WEBHOOK = str(os.getenv("ZAPIER_WEBHOOK"))

CONNECTION_STRING = str(os.getenv("CONNECTION_STRING"))

logging.basicConfig(level=logging.WARNING)


async def main():
    engine = create_engine(CONNECTION_STRING)
    inspector = inspect(engine)
    metadata = MetaData()

    # Initialize the BDC class
    bdc = BDC(USERNAME, API_KEY)

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

    # replace the - with _ in the date because postgres does not allow - in the table name
    table_name = f"bdc_{date}".replace("-", "_")

    # Check if table exists
    if table_name in inspector.get_table_names():
        logging.warning(f"Table {table_name} already exists")
        pass
    else:
        create_bdc_table(table_name, metadata)
        metadata.create_all(engine)
        logging.info(f"Table {table_name} created")

    # Get the download list for a specific date
    downloadList: list[dict] = await bdc.getDownloadList(date=date, category="State")
    if not downloadList:
        logging.error("No files to download")
        sys.exit(1)
    else:
        for file in tqdm(downloadList, desc="Downloading BDC files"):
            logging.info(json.dumps(file, indent=4, sort_keys=True))

            file_id = file["file_id"]
            response = await bdc.getDownloadFile(file_id)
            extractZip(response=response, file_id=file["file_id"])
            filename = f"{file['file_name']}"

            logging.info(f"Extracted {filename}")

            copy_data_to_postgres(engine, f"exports/{filename}.csv", table_name)

            if check_file_size(f"exports/{filename}.csv", 100):
                success = await upload_file_to_zapier(
                    f"exports/{filename}.csv", ZAPIER_WEBHOOK, filename, date
                )
                if success:
                    os.remove(f"exports/{filename}.csv")
                    logging.info(f"Deleted {filename}")
                else:
                    logging.error(
                        f"Failed to upload {filename}, keeping file for retry"
                    )
            else:
                try:
                    zipped_file = zip_file(f"exports/{filename}.csv")
                    zipped_filename = f"{filename}.zip"
                    success = await upload_file_to_zapier(
                        str(zipped_file), ZAPIER_WEBHOOK, zipped_filename, date
                    )
                    if success:
                        os.remove(zipped_file)
                        os.remove(f"exports/{filename}.csv")
                        logging.info(
                            f"Successfully uploaded and deleted {zipped_filename}"
                        )
                    else:
                        logging.error(
                            f"Failed to upload {zipped_filename}, keeping files for retry"
                        )
                except Exception as e:
                    logging.error(f"Error during zip and upload process: {str(e)}")

        print(
            f"Files uploaded to Box folder Broadband Data/{date} and {table_name} table created in database"
        )
        # Print the number of rows in the table
        with engine.connect() as connection:
            result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            for row in result:
                print(f"Number of rows in {table_name}: {row[0]}")

        # Close the connection
        engine.dispose()
        logging.info("Connection closed")


if __name__ == "__main__":
    asyncio.run(main=main())
