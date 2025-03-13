import io
import logging
import os
import tempfile
import zipfile

import httpx
import pandas as pd
from pandas import DataFrame
from tqdm import tqdm


async def upload_file_to_zapier(
    filepath: str, zapier_webhook: str, filename: str, table_name: str
) -> bool:
    """
    Upload a file to Zapier via webhook

    Args:
        filepath: Path to the file to upload
        zapier_webhook: Zapier webhook URL
        filename: Name of the file
        table_name: Name of the table

    Returns:
        bool: True if upload successful, False otherwise
    """
    # Check file size - 100MB limit
    file_size = os.path.getsize(filepath)
    max_size = 100 * 1024 * 1024  # 100MB in bytes

    if file_size > max_size:
        logging.error(
            f"File size ({file_size / 1024 / 1024:.2f}MB) exceeds the 100MB limit"
        )
        return False

    try:
        with open(filepath, "rb") as f:
            files = {"file": f}
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    url=f"{zapier_webhook}?filename={filename}&table_name={table_name}",
                    files=files,
                    timeout=30,  # Add timeout
                )

        if response.status_code == 200:
            logging.info("File uploaded to Zapier successfully")
            return True
        else:
            logging.error(f"Failed to upload file. Status code: {response.status_code}")
            return False
    except httpx.RequestError as e:
        # This will also catch TimeoutException since it's a subclass of RequestError
        if isinstance(e, httpx.TimeoutException):
            logging.error("Upload timed out. File may be too large for processing")
        else:
            logging.error(f"Request error during file upload: {str(e)}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error during file upload: {str(e)}")
        return False


def extractZip(response, file_id):
    zip_path = f"./{file_id}.csv.zip"
    extract_path = "./exports"

    # Save the content to a file
    with open(zip_path, "wb") as file:
        file.write(response.content)

    # Extract the zip file
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_path)

    # delete the zip file
    os.remove(path=zip_path)


def zip_exports_if_large(directory_path="./exports", max_size_mb=100):
    """
    Checks CSV files in the exports directory and zips them if they exceed the size limit.

    Args:
        directory_path (str): Path to the directory containing the CSV files
        max_size_mb (int): Maximum file size in MB before zipping

    Returns:
        list: Paths to files ready for upload (either original CSVs or new ZIP files)
    """
    max_size_bytes = max_size_mb * 1024 * 1024
    files_to_upload = []

    # Create a separate folder for zipped files if it doesn't exist
    zip_folder = os.path.join(directory_path, "zipped")
    os.makedirs(zip_folder, exist_ok=True)

    # Process each CSV file
    for filename in os.listdir(directory_path):
        if not filename.endswith(".csv"):
            continue

        filepath = os.path.join(directory_path, filename)
        file_size = os.path.getsize(filepath)

        # If file exceeds size limit, zip it
        if file_size > max_size_bytes:
            logging.info(f"File {filename} exceeds {max_size_mb}MB, zipping...")
            zip_filename = os.path.splitext(filename)[0] + ".zip"
            zip_filepath = os.path.join(zip_folder, zip_filename)

            with zipfile.ZipFile(zip_filepath, "w", zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(filepath, arcname=filename)

            # Check if the zip file is still too large
            if os.path.getsize(zip_filepath) > max_size_bytes:
                logging.warning(
                    f"Even after compression, {zip_filename} exceeds {max_size_mb}MB"
                )
                # You might want to implement chunking here if needed

            files_to_upload.append((zip_filepath, zip_filename))
            logging.info(f"Created zip file: {zip_filepath}")
        else:
            files_to_upload.append((filepath, filename))

    return files_to_upload


def upload_tempfile_to_zapier(
    file_data: io.BytesIO, zapier_webhook: str, filename: str, table_name: str
) -> bool:
    """
    Upload a file to Zapier via webhook

    Args:
        file_data: BytesIO object containing the file data
        zapier_webhook: Zapier webhook URL
        filename: Name of the file

    Returns:
        bool: True if upload successful, False otherwise
    """
    temp_file = None
    try:
        # Create a temporary file
        temp_file = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        temp_file_path = temp_file.name

        # Write the data to the temporary file
        file_data.seek(0)  # Ensure we're at the start of the BytesIO
        temp_file.write(file_data.read())
        temp_file.close()

        # Upload the file
        with open(temp_file_path, "rb") as f:
            files = {"file": f}
            response = httpx.post(
                url=f"{zapier_webhook}?filename={filename}&table_name={table_name}",
                files=files,
                timeout=30,  # Add timeout
            )

        if response.status_code == 200:
            logging.info("File uploaded to Zapier successfully")
            return True
        else:
            logging.error(f"Failed to upload file. Status code: {response.status_code}")
            return False

    except Exception as e:
        logging.error(f"Failed to upload file: {str(e)}")
        return False

    finally:
        # Clean up the temporary file
        if temp_file is not None:
            try:
                os.unlink(temp_file.name)
            except Exception as e:
                logging.warning(f"Failed to delete temporary file: {str(e)}")


def combineCSVsintoDataFrame(path: str = "./exports") -> DataFrame | None:
    """Combines all CSV files in a directory into a single DataFrame.

    Args:
        path (str): The path to the directory containing the CSV files.
    """
    column_types = {
        "frn": "category",
        "provider_id": "category",
        "brand_name": "category",
        "location_id": "int16",
        "technology": "category",
        "max_advertised_download_speed": "category",
        "max_advertised_upload_speed": "category",
        "low_latency": "category",
        "business_residential_code": "category",
        "state_usps": "category",
        "block_geoid": "int32",
        "h3_res8_id": "category",
    }

    frames = []
    files = [f for f in os.listdir(path) if f.endswith(".csv")]

    if len(files) == 0:
        print("No CSV files found")
        return None
    else:
        for file in tqdm(files, desc="Loading CSV files"):
            # Load the CSV files into DataFrames
            chunks = pd.read_csv(
                f"{path}/{file}", encoding="utf-8", chunksize=10000, index_col=False
            )
            for chunk in chunks:
                frames.append(chunk)

        # Combine the DataFrames
        combined_df = pd.concat(frames, ignore_index=True)
        combined_df = combined_df.astype(column_types)

        print("Files have been combined into a DataFrame")
        return combined_df
