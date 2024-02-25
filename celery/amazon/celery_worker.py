import os
import json
import logging

from celery import Celery
import httpx
from dotenv import load_dotenv

from celeryconfig import CELERY_BROKER_URL, CELERY_RESULT_BACKEND
from tools import counter, SCRAPER_NAME
from models import Item

load_dotenv()
app = Celery('db_updater', broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND)
client = httpx.Client()
API_KEY = os.getenv("API_KEY")

@app.task
def celery_db_update(serialized: dict, attempts=3):
    global counter

    if not serialized:
        return None
    # item = json.loads(json_item)
    try:
        item_dict = json.loads(serialized)
        item = Item(**item_dict)
    

        result_json = {
            "sourceId": item.source_id,
            "sourceProductId": item.source_product_id,
            "url": item.url,
            "title": item.title,
            "oldPrice": item.old_price,
            "currentPrice": item.current_price,
            "images": item.images,
            "description": item.description,
            "originalCategory": item.original_category,
            "couponCode": item.coupon_code,
            "categories": item.categories
        }

        saving_url = f"https://parser.discount.one/products?apiKey={API_KEY}"
        response = None
        with httpx.Client() as db_client:

            try:
                response = db_client.post(saving_url, json=result_json)

            except (httpx.ConnectTimeout,
                    httpx.ReadTimeout,
                    httpx.ConnectError,
                    httpx.ReadError):
                if attempts > 0:
                    attempts -= 1
                    logging.warning("Connection error, retrying request")
                    celery_db_update(item, attempts)

                else:
                    logging.warning("Connection error when sending to db")
                    return None

            # also cathcing something unexpected
            except Exception as exc:
                logging.warning("Exception when sending to db : %s", exc)
                return None
            

            if response:
                # print(response.json())
                if response.status_code == 200:
                    logging.info("Successfully sended to db")
                    #print(result_json) 
                    #print(response.text)
                elif response.status_code == 500:
                    logging.warning("SERVER ERROR, here is sended data")
                    print(result_json)

                elif response.status_code == 400:
                    logging.warning("DATA ERROR, here is data")
                    print(result_json)
            else:
                logging.error("can't locate response object")
    
    except TypeError:
        print("===========================================================")
        print(serialized)
        print(type(serialized))
