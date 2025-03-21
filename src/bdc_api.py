import logging
import time

import httpx


class BDC:
    def __init__(self, username: str, api_key: str):
        self.client = httpx.AsyncClient()
        self.baseURL = "https://broadbandmap.fcc.gov/api/public/map"
        self.headersList = {
            "Accept": "application/json",
            "User-Agent": "0.0.0",
            "username": username,
            "hash_value": api_key,
        }

    async def getlistofDates(
        self, data_type: str = "availability"
    ) -> tuple[str, list[str]]:
        reqUrl = f"{self.baseURL}/listAsOfDates"
        data = await self.client.get(url=reqUrl, headers=self.headersList)
        dates = data.json()["data"]
        dates = [i["as_of_date"] for i in dates if i["data_type"] == data_type]
        dates.reverse()
        return data_type, dates

    async def getDownloadList(
        self,
        date: str,
        category: str,
        subcategory: str = "",
    ) -> list[dict]:
        reqUrl = f"{self.baseURL}/downloads/listAvailabilityData/{date}?category={category}&subcategory={subcategory}"
        data = await self.client.get(url=reqUrl, headers=self.headersList)
        downloadList = [
            i
            for i in data.json()["data"]
            if "Satellite" not in i["file_name"]
            and "Fixed Broadband" == i["technology_type"]
            and "csv" == i["file_type"]
        ]
        # downloadList.sort(key=lambda x: x["state_fips"])
        return downloadList

    async def getDownloadFile(self, file_id: int):
        reqUrl = f"{self.baseURL}/downloads/downloadfile/availability/{file_id}"
        response = await self.client.get(
            url=reqUrl,
            headers=self.headersList,
            timeout=httpx.Timeout(connect=5.0, read=30.0, write=5.0, pool=5.0),
        )
        return response

    async def downloadFiles(self, files: list):
        for file in files:
            index: int = files.index(file)
            try:
                await self.getDownloadFile(file_id=file["file_id"])
                print(f"Downloaded {index + 1} of {len(files)}")
                time.sleep(1)
            except Exception as e:
                logging.error(f"Failed to download file {index + 1}: {e}")
                pass
        logging.info("Completed downloads with some failures")
