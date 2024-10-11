import os
import pandas as pd
import sqlite3
import zipfile
from tqdm import tqdm
import time
import threading


def combineFilesintoDB(
    extract_path: str = "./exports", db_path: str = "./.db", table_name: str = "Table 1"
):
    # combined_path = "/Users/mwhittington/Documents/Code/BDC/combined.csv"
    frames = []

    for file in os.listdir(extract_path):
        # Load the CSV files into DataFrames
        df = pd.read_csv(f"{extract_path}/{file}", encoding="utf-8")
        frames.append(df)

    # Combine the DataFrames
    combined_df = pd.concat(frames, ignore_index=True)

    conn = sqlite3.connect(db_path)

    combined_df.to_sql(table_name, conn, if_exists="replace", index=False)

    conn.close()

    return "Files have been combined into a SQLite database"

# def combineFilesintoStata(
#     extract_path: str = "./exports", stata_path: str = "./.dta"):
#     frames = []
#     for file in os.listdir(extract_path):
#         # Load the CSV files into DataFrames
#         df = pd.read_csv(f"{extract_path}/{file}", encoding="utf-8")
#         frames.append(df)

#     # Combine the DataFrames
#     combined_df = pd.concat(frames, ignore_index=True)

#     combined_df.to_stata(stata_path)

#     return "Files have been combined into Stata format"
#
def combineFilesintoStata(
    extract_path: str = "/Users/mwhittington/Library/CloudStorage/Box-Box/BDC 2023-12-31", stata_path: str = "./bdc.dta"):
    frames = []
    files = [f for f in os.listdir(extract_path) if f.endswith('.csv')]

    # Progress bar for loading CSV files
    for file in tqdm(files, desc="Loading CSV files"):
        # Load the CSV files into DataFrames
        df = pd.read_csv(f"{extract_path}/{file}", encoding="utf-8")
        frames.append(df)

    # Progress bar for combining DataFrames
    with tqdm(total=len(frames), desc="Combining DataFrames") as pbar:
        combined_df = pd.concat(frames, ignore_index=True)
        pbar.update(len(frames))

    # Progress bar for saving to Stata
    def save_to_stata():
        nonlocal combined_df
        combined_df.to_stata(stata_path)

    saving_thread = threading.Thread(target=save_to_stata)
    saving_thread.start()

    with tqdm(desc="Saving to Stata", unit="s") as pbar:
        while saving_thread.is_alive():
            pbar.update(1)
            time.sleep(1)

    saving_thread.join()

    return "Files have been combined into Stata format"



def extractZip(response, file_id):

    zip_path = f"/Users/mwhittington/Documents/Code/BDC/{file_id}.csv.zip"
    extract_path = "/Users/mwhittington/Documents/Code/BDC/exports"

    # Save the content to a file
    with open(zip_path, "wb") as file:
        file.write(response.content)

    # Extract the zip file
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_path)

    # delete the zip file
    os.remove(path=zip_path)


if __name__ == "__main__":
    print(combineFilesintoStata())
