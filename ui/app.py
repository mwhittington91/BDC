import asyncio
import json
import logging
import os
import sys

import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import MetaData, create_engine, inspect

sys.path.append("./")
from db.schema import copy_data_to_postgres, create_bdc_table
from src.bdc_api import BDC
from src.utils import extractZip, upload_file_to_zapier

load_dotenv()

USERNAME = str(os.getenv("USERNAME"))
API_KEY = str(os.getenv("API_KEY"))

ZAPIER_WEBHOOK = str(os.getenv("ZAPIER_WEBHOOK"))

CONNECTION_STRING = str(os.getenv("CONNECTION_STRING"))

logging.basicConfig(level=logging.WARNING)


async def main():
    st.title("Broadband Data Collection")

    engine = create_engine(CONNECTION_STRING)
    inspector = inspect(engine)
    metadata = MetaData()

    # Initialize the BDC class
    bdc = BDC(USERNAME, API_KEY)

    # Get the list of dates
    dates = await bdc.getlistofDates()

    # Seperate the data_type and dates
    data_type, dates = dates
    st.write(f"Data Type: {data_type}")
    date = st.selectbox(
        "Select a date",
        dates,
    )

    st.write("You selected:", date)

    # Get the date from the user from the index of the dates list
    if date not in dates:
        raise ValueError("Invalid date")
        sys.exit(1)

    # Convert the index to the date

    table_name = f"bdc_{date}".replace("-", "_")

    if st.button("Download files then upload to Box and Postgres"):
        # Check if table exists
        if table_name in inspector.get_table_names():
            logging.warning(f"Table {table_name} already exists")
            pass
        else:
            create_bdc_table(table_name, metadata)
            metadata.create_all(engine)
            logging.info(f"Table {table_name} created")

        # Get the download list for a specific date
        downloadList: list[dict] = await bdc.getDownloadList(
            date=date, category="State"
        )
        if not downloadList:
            logging.error("No files to download")
            sys.exit(1)
        else:
            my_bar = st.progress(0)

            for file in downloadList[:5]:
                my_bar.progress(
                    downloadList.index(file) + 1,
                    text=f"{file['file_name']} {downloadList.index(file) + 1}/{len(downloadList)}",
                )
                logging.info(json.dumps(file, indent=4, sort_keys=True))

                file_id = file["file_id"]
                response = await bdc.getDownloadFile(file_id)
                extractZip(response=response, file_id=file["file_id"])
                filename = f"{file['file_name']}"

                logging.info(f"Extracted {filename}")

                await upload_file_to_zapier(
                    f"exports/{filename}.csv", ZAPIER_WEBHOOK, filename, date
                )

                copy_data_to_postgres(engine, f"exports/{filename}.csv", table_name)

                os.remove(f"exports/{filename}.csv")
                logging.info(f"Deleted {filename}")

            st.write(
                f"Files uploaded to Box folder Broadband Data/{date} and {table_name} table created in database"
            )


if __name__ == "__main__":
    asyncio.run(main=main())
