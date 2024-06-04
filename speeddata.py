import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import re
 
def save_speedtest_data_to_excel(url):
    """Extracts data from Speedtest Global Index and saves it to an Excel file with separate sheets for each category."""

    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        script_tag = soup.find("script", string=lambda t: "var data =" in t if t else False)
 
        if script_tag:
            try:
                script_content = script_tag.string
                json_str_match = re.search(r"var data = ({.*?});", script_content, re.DOTALL)
                if json_str_match:
                    data = json.loads(json_str_match.group(1))
                    # Initialize Excel writer
                    with pd.ExcelWriter("speedtest_data.xlsx") as writer:
                        # Extract and save each category of data
                        for category in ['fixedMean', 'mobileMean', 'fixedMedian', 'mobileMedian']:
                            if category in data:
                                df = pd.DataFrame(data[category])
                                df.to_excel(writer, sheet_name=category, index=False)
 
                    print("Data extracted and saved successfully in 'speedtest_data.xlsx'")
                else:
                    print("JSON data not found in the script.")
            except Exception as e:
                print(f"Error processing the script content: {e}")
        else:
            print("Script with data not found.")
    else:
        print(f"Failed to make request to website, status code: {response.status_code}")
 
# URL of the Speedtest Global Index page for United States
url = "https://www.speedtest.net/global-index/united-states#fixed"
save_speedtest_data_to_excel(url=url)
 