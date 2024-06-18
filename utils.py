import os
import pandas as pd
import sqlite3
import zipfile


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

    return "Files have been combined"


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
    pass
