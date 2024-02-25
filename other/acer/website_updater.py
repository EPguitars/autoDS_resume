import ssl
import logging
import asyncio
import json
import logging
from datetime import datetime
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

import httpx
from aiolimiter import AsyncLimiter
from selectolax.parser import HTMLParser
from rich.logging import RichHandler
from rich import print

import tools
from db_operations import grab_website_items, get_images_db
from tools import SCRAPER_NAME
from headers import headers, personal_headers
from main_tools.proxy import proxy_gen
from models import Item
from image_validation import validate_image
# from parse_details import scrape_images, check_availability
from db_operations import send_item_to_db, get_images_db, get_description_db
from categories import categories


limiter = AsyncLimiter(70, 1)
DISCOUNT_SET = 20

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s",
                    handlers=[RichHandler()])


def grab_slugs(category):
    for meta, slugs in categories.items():
        if category == meta[1]:
            return slugs


def check_discount(old_price, current_price):
    """ check if discount is present """

    if old_price and current_price:
        discount = round((old_price - current_price) / (old_price / 100))

        if discount >= DISCOUNT_SET:
            return True
        else:
            return None
    else:
        return None


def update_item(item: tuple, json_data: dict):
    """ update info about item """

    current_price = None
    old_price = None
    
    try:
        current_price = None # path to current price

        old_price = None # path to mrp

        availability_status = None # path to availability status
    
    except KeyError:
        logging.warning("No searched key in json")
        return None
    except TypeError:
        logging.warning("Json is empty")
        return None
    
    print(current_price, old_price, availability_status)
    discount = check_discount(old_price, current_price)

    if discount and availability_status == "inStock":

        updated_item = Item(
            source_id=item[1],
            url=item[3],
            source_product_id=item[2],
            title=item[5],
            old_price=float(old_price),
            current_price=float(current_price),
            images=None,
            description=item[13],
            original_category=item[15],
            coupon_code=None,
            categories=grab_slugs(item[15])
        )

        # also get images
        updated_item.images = get_images_db(updated_item)
        print(updated_item)
        return updated_item

    else:
        logging.warning("No discount or item not in stock")
        return None


async def scrape_item_page(item: tuple, semaphore, attempts=6):
    """ making request to item page and parse it """
    proxy = next(proxy_gen)
    url = item[3]

    async with httpx.AsyncClient(headers=personal_headers, proxies=proxy) as client:
        try:

            response = await client.get(url, timeout=60)

        except ValueError:
            logging.warning("Value error when page requesting")
            return None

        except (httpx.ConnectTimeout,
                httpx.ReadTimeout,
                httpx.ConnectError,
                httpx.ReadError,
                httpx.RemoteProtocolError):

            if attempts > 0:
                attempts -= 1
                logging.warning("Connection error, retrying request")
                return await scrape_item_page(item,
                                              semaphore,
                                              attempts)

            else:
                logging.warning(
                    "Connection error when page requesting")
                return None

        except httpx.ProxyError:
            if attempts > 0:
                attempts -= 1
                logging.warning("Proxy error, retrying request")
                return await scrape_item_page(item,
                                              semaphore,
                                              attempts)
            else:
                logging.error("Proxy big fail")
                return None
        
        except ssl.SSLZeroReturnError:
            if attempts > 0:
                attempts -= 1
                logging.warning("SSL error, retrying request")
                return await scrape_item_page(item,
                                              semaphore,
                                              attempts)
            else:
                logging.error("SSL big fail")
                return None
            
    if response.status_code != 200:
        logging.error("Invalid url")
        return None

    markup = HTMLParser(response.text)
    with open("markup.html", "w", encoding="utf-8") as file:
        file.write(response.text)

    search_string = "window.__PRELOADED_STATE__ = "

    scripts = markup.css("script")
    target_script = None

    for script in scripts:
        if search_string.lower() in script.text().lower():
            target_script = script
            break

    if target_script:
        json_data = json.loads(target_script.text().replace(
            search_string, "").replace(";", ""))

    else:
        logging.warning("Cant's find price data on the page")
        return None

    updated_item = update_item(item, json_data)

    if updated_item:

        updated_item.images = get_images_db(updated_item)

        if not updated_item.current_price or not updated_item.old_price:
            return None
        if not updated_item.title:
            return None
        if not updated_item.source_product_id:
            return None
        if not updated_item.url:
            return None
        if not updated_item.images:
            return None
        elif len(updated_item.images) != 3:
            return None
        
        return updated_item

    else:
        logging.warning("price parsing failure in update_item")
        return None


async def process_batch(batch):
    """ function to process batch of items """
    start = datetime.now()
    prepared_data = await asyncio.gather(*batch)
    end = datetime.now()
    print(f"BATCH DONE, time spended - {end - start}")

    return prepared_data


async def update_website_items():
    db_items = grab_website_items(SCRAPER_NAME)

    tasks = []
    for item in db_items:
        tasks.append(scrape_item_page(item, limiter))

    total_tasks = len(tasks)
    batch_size = 100
    tasks_left = total_tasks

    print(f"TOTAL TASKS: {total_tasks}")
    for i in range(0, total_tasks, batch_size):
        batch = tasks[i:i+batch_size]
        prepared_to_db = await process_batch(batch)
        tasks_left -= batch_size

        # SEND RESULT TO DB
        db_tasks = []
        for item in prepared_to_db:
            if item:
                db_tasks.append(send_item_to_db(item, limiter))

        await asyncio.gather(*db_tasks)
        logging.info(f"TASKS LEFT: {tasks_left}")
# scrape their personal pages
# create item objects


if __name__ == "__main__":
    asyncio.run(update_website_items())
    # test_item = ("", "", "", "url",
    #              "Slim Fit Zip-Front Bomber Jacket")
    # asyncio.run(scrape_item_page(test_item, limiter))
