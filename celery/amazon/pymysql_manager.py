import os
import logging

import httpx
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from rich import print

from models import Item
from tools import counter, SCRAPER_NAME
load_dotenv()

# key to target api for data sending
API_KEY = os.environ.get("API_KEY")

db_config = {
    "host": "localhost",
    "database": "admin_main",
    "user": "admin",
    "password": "ioHNS16HUQeLLtTv"
}

try:
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    if connection.is_connected():
        print("Connected to MySQL database.")

except Error as e:
    print(f"Error connecting to MySQL database: {e}")


def grab_website_items(scraper_name: str):
    """ grab items which is not scraped but on website """
    cursor.execute(f"""
        SELECT * FROM `products` 
        WHERE `sourceId` = '{scraper_name}' 
        AND `isPicked` = 1 
        AND `isActual` = 0""")

    result = cursor.fetchall()
    return result


def grab_all_items(scraper_name: str):
    """ grab items which is not scraped but on website """
    cursor.execute(f"""
        SELECT * FROM `products` 
        WHERE `sourceId` = '{scraper_name}' 
        """)

    result = cursor.fetchall()
    return result


def delete_row(scraper_name, source_product_id):
    logging.info("DELETING ROW!")
    cursor.execute(f"""
        DELETE FROM `products` 
        WHERE `sourceId` = '{scraper_name}'
        AND `sourceProductId` = '{source_product_id}' 
        """)
    connection.commit()
    logging.info("SUCCESS")

def check_db(item: Item):
    """ check if there an item in db """
    cursor.execute(f"""
    SELECT EXISTS (
        SELECT 1
        FROM `products`
        WHERE `sourceId` = '{item.source_id}'
        AND `sourceProductId` = '{item.source_product_id}'
    ) AS item_exists
    """)

    result = cursor.fetchone()

    item_exists = bool(result[0])

    return item_exists

def get_description_db(item: Item):
    """ get description from db """
    cursor.execute(f"""
        SELECT `description`
        FROM `products`
        WHERE `sourceId` = '{item.source_id}'
        AND `sourceProductId` = '{item.source_product_id}'""")

    result = cursor.fetchone()[0]
    return result

def get_category_db(item: Item):
    """ get category from db """
    cursor.execute(f"""
        SELECT `originalCategory`
        FROM `products`
        WHERE `sourceId` LIKE '{item.source_id}'
        AND `sourceProductId` LIKE '{item.source_product_id}'""")

    result = cursor.fetchone()[0]
    return result


def get_images_db(item: Item):
    """ get images from db """
    images = []
    cursor.execute(f"""
        SELECT * FROM `products_images`
        WHERE productId =
        ( SELECT id FROM products p
        WHERE p.sourceId = '{item.source_id}'
        AND p.sourceProductId = '{item.source_product_id}'
        LIMIT 1 )
        """)

    results = cursor.fetchall()
    for result in results:
        images.append(result[2])

    if images:
        return images
    else:
        logging.error("None of images in db!")
        return None

async def send_item_to_db(item, limiter, attempts=3):
    global counter

    result_json = {
        "sourceId": SCRAPER_NAME,
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

    #print(result_json)
    saving_url = f"https://parser.discount.one/products?apiKey={API_KEY}"
    
    async with httpx.AsyncClient() as db_client:

        async with limiter:
            try:  
                response = await db_client.post(saving_url, json=result_json)
                
            except (httpx.ConnectTimeout,
                    httpx.ReadTimeout,
                    httpx.ConnectError,
                    httpx.ReadError):
                if attempts > 0:
                    attempts -= 1
                    logging.warning("Connection error, retrying request")
                    return send_item_to_db(item, limiter, attempts)

                else:
                    logging.warning("Connection error when sending to db")
                    return None

            # also cathcing something unexpected
            except Exception as exc:
                print("problem")
                logging.warning("Exception when sending to db : %s", exc)
                return None
    
    print(response.status_code)
    if response.status_code == 200:
        #print(response.json())
        counter["filtered_and_sended"] += 1
        print(counter)

    elif response.status_code == 400:
        logging.warning(result_json)

    elif response.status_code == 500:
        print("11111111111111111111")
        print(response.json())
        print(result_json)

def close_db_connection():
    cursor.close()
    connection.close()
    logging.info("DB connection closed")