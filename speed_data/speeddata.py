from bs4 import BeautifulSoup
import json
import pandas as pd
import re
import httpx
import io

# url = "https://www.speedtest.net/global-index/united-states#fixed"

def save_speedtest_data_to_excel(url: str):
    response = httpx.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        script_tag = soup.find("script", string=lambda t: "var data =" in t if t else False)
        if script_tag:
            try:
                script_content = script_tag.string
                json_str_match = re.search(r"var data = ({.*?});", script_content, re.DOTALL)
                if json_str_match:
                    data = json.loads(json_str_match.group(1))
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        for category in ["fixedMean", "mobileMean", "fixedMedian", "mobileMedian"]:
                            if category in data:
                                df = pd.DataFrame(data[category])
                                df.to_excel(writer, sheet_name=category, index=False)
                    output.seek(0)
                    return output
                else:
                    print("JSON data not found in the script.")
            except Exception as e:
                print(f"Error processing the script content: {e}")
        else:
            print("Script with data not found.")
    else:
        print(f"Failed to make request to website, status code: {response.status_code}")
    return None

# URL of the Speedtest Global Index page for United States
if __name__ == "__main__":
    pass
