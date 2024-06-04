# https://us-fcc.app.box.com/v/bdc-public-data-api-spec

import zipfile
import httpx
import os
import pandas as pd
import time
import asyncio
import sqlite3
import json
from dotenv import load_dotenv

load_dotenv()

username = os.getenv("USERNAME")
hash_value = os.getenv("API_KEY")

client = httpx.AsyncClient()

baseURL = "https://broadbandmap.fcc.gov/"

headersList: dict[str, str | None] = {
    "Accept": "application/json",
    "User-Agent": "0.0.0",
    "username": username,
    "hash_value": hash_value,
}


async def getlistofDates() -> list:
    reqUrl = f"{baseURL}api/public/map/listAsOfDates"
    data = await client.get(url=reqUrl, headers=headersList)
    dates = data.json()["data"]
    return dates


async def getDownloadList(
    date: str = "2023-12-31",
    category: str = "State",
    subcategory: str = "Fixed Broadband",
) -> list[str]:
    reqUrl = f"{baseURL}api/public/map/downloads/listAvailabilityData/{date}?category={category}&subcategory={subcategory}"
    data = await client.get(url=reqUrl, headers=headersList)
    downloadList = [i for i in data.json()["data"] if "Satellite" not in i["file_name"]]
    downloadList.sort(key=lambda x: x["state_fips"])
    downloadList = [json.dumps(i, indent=4, sort_keys=True) for i in downloadList]
    return downloadList


async def getDownloadFile(file_id: int):
    reqUrl = f"{baseURL}api/public/map/downloads/downloadfile/availability/{file_id}"
    response = await client.get(url=reqUrl, headers=headersList)

    return response


async def downloadFiles(files: list):
    missing = []
    for file in files:
        index: int = files.index(file)
        try:
            await getDownloadFile(file_id=file["file_id"])
            print(f"Downloaded {index + 1} of {len(files)}")
            time.sleep(1)
        except Exception as e:
            missing.append(index)
            print(e)
            pass
    print(f"Missing files: {missing}")


