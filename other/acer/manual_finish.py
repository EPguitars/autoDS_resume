import os
import logging
from datetime import datetime

from dotenv import load_dotenv
import httpx

from tools import SCRAPER_NAME

load_dotenv()

API_KEY = os.getenv("API_KEY")
client = httpx.Client() 

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

def work_notification(parameter, attempts=6):
    """ 2 parameters start, finish """
    db_url = f"https://parser.discount.one/sources/{SCRAPER_NAME}/{parameter}?apiKey={API_KEY}"

    try:
        response = client.post(db_url, timeout=300)

        if parameter == "finish":
            return response.json()

        if response.status_code == 200:
            logging.info(f"Successfully sended {parameter} notification")

    except (httpx.ConnectTimeout,
            httpx.ReadTimeout,
            httpx.ConnectError,
            httpx.ReadError) as error:
        if attempts > 0:
            attempts -= 1
            logging.warning("Connection error, retrying request")
            print(error)
            return work_notification(parameter, attempts)

        else:
            logging.warning("Connection error when updating actuality")
            return None

    # also cathcing something unexpected
    except Exception as exc:
        logging.warning("Exception when updating actuality : %s", exc)
        return None
    
    if response:
        print(response.status_code)
        print(response)
start_time = datetime.now()

work_notification("finish")