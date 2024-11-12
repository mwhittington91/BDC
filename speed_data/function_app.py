import azure.functions as func
import logging
from speeddata import save_speedtest_data_to_excel
import httpx
import os
from dotenv import load_dotenv
import tempfile
import datetime as dt

load_dotenv()

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)
zapier_webhook = os.getenv("ZAPIER_WEBHOOK")
date = dt.datetime.now().strftime("%Y-%m-%d")

@app.route(route="speedtest")
def speedtest(req: func.HttpRequest) -> func.HttpResponse:
    logging.info(msg='Python HTTP trigger function processed a request.')

    url = "https://www.speedtest.net/global-index/united-states#fixed"
    file_data = save_speedtest_data_to_excel(url=url)

    if file_data:
        try:
            # Create a temporary file
            with tempfile.NamedTemporaryFile(delete=False, prefix=f"{date}-Speedata-", suffix=".xlsx") as tmp_file:

                tmp_file_name = tmp_file.name
                tmp_file.write(file_data.getvalue())
                tmp_file.flush()

                # Rewind the BytesIO object to read from the beginning
                file_data.seek(0)

                # Send the file to Zapier
                with open(tmp_file_name, mode='rb') as f:
                    files = {'file': f}
                    response = httpx.post(url=zapier_webhook, files=files)
                    if response.status_code == 200:
                        return func.HttpResponse(body=f"{tmp_file_name} saved and uploaded successfully.", status_code=200)
                    else:
                        logging.error(msg=f"Failed to upload file to Zapier. Status code: {response.status_code}")
                        return func.HttpResponse(body="Failed to upload file to Zapier.", status_code=500)

        except Exception as e:
            logging.error(msg=f"Failed to save or upload speedtest data: {e}")
            return func.HttpResponse(body="Failed to save or upload speedtest data.", status_code=500)
    else:
        logging.error(msg="Failed to save speedtest data.")
        return func.HttpResponse(body="Failed to save speedtest data.", status_code=500)


# Test
@app.route(route="hello")
def hello(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    name = req.params.get("name")
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get("name")

    if name:
        return func.HttpResponse(
            f"Hello, {name}! This HTTP triggered function executed successfully."
        )
    else:
        return func.HttpResponse(
            body="Please pass a name on the query string or in the request body",
            status_code=200,
        )
