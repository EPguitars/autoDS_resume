import os
import json
import random
import asyncio
import logging
import traceback
from datetime import datetime
import dataclasses

import httpx
from dotenv import load_dotenv
from rich import print
from sqlmodel import Session, create_engine
from aiolimiter import AsyncLimiter

import tools
from categories import categories
from cards_scraper import get_pages_links, scrape_cards
from validator import send_item_to_db
from tools import counter, SCRAPER_NAME, proxy_gen, render
from scrape_amazon_items import update_items
from db_operations import *
from celery_worker import celery_db_update
from models import Item

from notificator.telegram_agent import send_error_report, send_success_report, parser_start_notification
from notificator.db_config import ScrapingSession
from main_tools.discount_api import get_discount

load_dotenv()

API_KEY = os.getenv("API_KEY")
client = httpx.Client()
semaphore = AsyncLimiter(80, 1)
BASE_URL = "https://www.amazon.in"
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# DB setup
db_path = "/home/one/parsers/eugene/statistics.db"
engine = create_engine(f"sqlite:///{db_path}", echo=True)


def update_statistics(scraping_session):
    with Session(engine) as db_session:
        db_session.add(scraping_session)
        db_session.commit()


def send_actual_time(parsing_time, attempts=3):
    parsing_info = {
        "sourceId": SCRAPER_NAME,
        "dateOfActuality": parsing_time
    }
    db_url = f"https://parser.discount.one/finish?apiKey={API_KEY}"

    try:
        client.post(db_url, json=parsing_info)
        logging.info("Successfully sended time of actuality in db")

    except (httpx.ConnectTimeout,
            httpx.ReadTimeout,
            httpx.ConnectError,
            httpx.ReadError) as error:
        if attempts > 0:
            attempts -= 1
            logging.warning("Connection error, retrying request")
            print(error)
            return send_actual_time(parsing_time, attempts)

        else:
            logging.warning("Connection error when updating actuality")
            return None

    # also cathcing something unexpected
    except Exception as exc:
        logging.warning("Exception when updating actuality : %s", exc)
        return None


def work_notification(parameter, attempts=3):
    """ 2 parameters start, finish """
    db_url = f"https://parser.discount.one/sources/{SCRAPER_NAME}/{parameter}?apiKey={API_KEY}"

    try:
        response = client.post(db_url, timeout=60000)

        if response.status_code == 200:
            if parameter == "finish":
                logging.info(f"Successfully sended {parameter} notification")
                return response.json()

        else:
            logging.warning(
                f"Got {response.status_code} when sending {parameter} notification")
            return None

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


async def send_items(db_manager: AsyncDatabaseManager,
                     item: Item,
                     semaphore):
    """ function for updating info about item"""
    async with semaphore:
        in_db = await db_manager.check_item_exists(item)

        if in_db:
            item.images = await db_manager.get_images_db(item)

            if not item.images:
                logging.warning("No images found for item %s",
                                item.source_product_id)
                query = await db_manager.save_asin(item, is_scraped=0)
                db_manager.transaction_group.append(query)

            else:
                item.description = await db_manager.get_description_db(item)
                serialized_item = json.dumps(dataclasses.asdict(item))
                celery_db_update.apply_async(args=[serialized_item])
                print("Sended to celery")
                counter["filtered_and_sended"] += 1
                print(counter)
                query = await db_manager.change_scraped_status(item.source_product_id, is_scraped=True, blacklisted=0)
                db_manager.transaction_group.append(query)

        else:
            logging.warning("Says that no item in db %s",
                            item.source_product_id)
            print(item)
            query = await db_manager.save_asin(item, is_scraped=0)
            db_manager.transaction_group.append(query)


async def update_asin_table(category,
                            url,
                            slugs,
                            db_manager: AsyncDatabaseManager,
                            option):
    """ function to scrape new items if they appear and update current items """
    global proxy_gen
    global render
    CONSTRAINT_PERCENT = 0.07
    pages_urls = await get_pages_links(url=url,
                                       proxy=next(proxy_gen),
                                       render_worker=next(render))
    if not pages_urls:
        pages_urls = [url]

    #random.shuffle(pages_urls)
    print(pages_urls)
    
    print(option)
    if option == "daily":
        current_discount = current_discount = get_discount(SCRAPER_NAME, category)
    elif option == "discounts":
        current_discount = 1.0

    elif option == "amounts":
        current_discount = 20.0
    
    # now scrape all items from pages
    tasks = []
    if pages_urls:
        for page_url in pages_urls:
            if page_url:
                tasks.append(scrape_cards(url=page_url,
                                          current_discount=current_discount,
                                          category=category,
                                          slugs=slugs,
                                          proxy=next(proxy_gen),
                                          render=next(render),
                                          semaphore=semaphore))
    else:
        logging.warning("No pages urls found")

    scraped_items = await asyncio.gather(*tasks)
    
    if scraped_items:
        cleaned_items = []
        
        for sublist in scraped_items:
            if sublist:
                for item in sublist:
                    if item:
                        cleaned_items.append(item)

        items = cleaned_items[:int(len(cleaned_items)*CONSTRAINT_PERCENT)]
        block_size = 100
        items = [items[i:i+block_size] for i in range(0, len(items), block_size)]
    
    else:
        print("No items found")
        return None

    if option == "discounts" or option == "amounts":
        return scraped_items

    db_semaphore = asyncio.Semaphore(200)

    tasks = []
    actual_pages = []
    items_should_be_scraped = []

    for block in scraped_items:
        if block:
            actual_pages.append("value")
            for item in block:
                if item:
                    items_should_be_scraped.append("value")
                    tasks.append(send_items(db_manager=db_manager,
                                            item=item,
                                            semaphore=db_semaphore))

    await asyncio.gather(*tasks)
    await db_manager.perform_transcation()
    print("Scraped pages")
    print(len(scraped_items))
    print("Actual pages")
    print(len(actual_pages))
    print("Items should be scraped")
    print(len(items_should_be_scraped))


async def scrape_amazon():
    start_time = datetime.now()
    work_notification("start")
    parser_start_notification(SCRAPER_NAME)
    try:
        db = AsyncDatabaseManager(
            host="localhost",
            port=3306,
            user="admin",
            password="ioHNS16HUQeLLtTv",
            db="admin_main"
        )

        await db.connect()
        await db.reset_asins()

        temp = 0
        tasks = []
        for meta, slugs in categories.items():
            if temp == 97:
                break

            path = meta[0]
            category = meta[1]
            url = "https://www.amazon.in/s?" + path

            tasks.append(update_asin_table(category,
                                           url,
                                           slugs,
                                           db_manager=db,
                                           option="daily"))
            temp += 1

            if len(tasks) == 2:
                await asyncio.gather(*tasks)
                tasks = []

        if tasks:
            await asyncio.gather(*tasks)

        # for _ in range(4):
        #     await update_items(db_manager=db, parameter="on website")

        logging.info("WEBSITE HAS BEEN UPDATED")
        #await update_items(db_manager=db, parameter="others")
        end_time = datetime.now()
        #info = None
        info = work_notification("finish")
        execution_time = end_time - start_time
        logging.info("Parsing is done. Congratulations!")
        print(f"Execution time: {execution_time} seconds")

        scraping_session = ScrapingSession(date=start_time,
                                           status="success",
                                           items_scraped=info["actualProductCount"],
                                           name=SCRAPER_NAME)

        # add successful session to statisctics
        update_statistics(scraping_session)
        send_success_report(parser_name=SCRAPER_NAME,
                            items_amount=counter["filtered_and_sended"],
                            info=info)

    except Exception:
        print(traceback.format_exc())
        # info = work_notification("finish")

        await db.close()
        print("Exiting...")
        scraping_session = ScrapingSession(date=start_time,
                                           status="fail",
                                           items_scraped=counter["filtered_and_sended"],
                                           name=SCRAPER_NAME,
                                           error_name=traceback.format_exc())

        # add failed session to statisctics
        update_statistics(scraping_session)
        send_error_report(parser_name=SCRAPER_NAME,
                          items_amount=counter["filtered_and_sended"],
                          traceback=traceback.format_exc())

    finally:
        await db.close()
        print("Exiting...")


if __name__ == "__main__":
    asyncio.run(scrape_amazon())
