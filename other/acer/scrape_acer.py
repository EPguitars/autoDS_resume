import os
import asyncio
import logging
import traceback
from datetime import datetime
from urllib.parse import urljoin

import httpx
from dotenv import load_dotenv
from rich import print
from rich.logging import RichHandler
from sqlmodel import Session, create_engine
from aiolimiter import AsyncLimiter

from categories import categories
from cards_scraper import get_pages_links, scrape_cards
from validator import parse_and_validate
from tools import counter, SCRAPER_NAME
from notificator.telegram_agent import send_error_report, send_success_report, parser_start_notification
from notificator.db_config import ScrapingSession
from db_operations import send_item_to_db, close_db_connection
from main_tools.discount_api import get_discount

load_dotenv()

API_KEY = os.getenv("API_KEY")
client = httpx.Client()
semaphore = AsyncLimiter(10, 1)
BASE_URL = "https://store.acer.com/en-in/"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s",
                    handlers=[RichHandler()])

# DB setup
db_path = "/home/one/parsers/eugene/statistics.db"
engine = create_engine(f"sqlite:///{db_path}", echo=True)


def update_statistics(scraping_session):
    with Session(engine) as db_session:
        db_session.add(scraping_session)
        db_session.commit()


def work_notification(parameter, attempts=3):
    """ 2 parameters start, finish """
    db_url = f"https://parser.discount.one/sources/{SCRAPER_NAME}/{parameter}?apiKey={API_KEY}"

    try:
        response = client.post(db_url, timeout=60)
        
        if parameter == "finish":
            return response.json()
        
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


async def run_acer_scraper():
    """ main logic for my scraper """

    start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for meta, slugs in categories.items():
        path = meta[0]
        category = meta[1]
        
        # Construct query to get all items from category with only one request
        url = BASE_URL + path + "?product_list_limit=all"
        
        #get discount for current category
        current_discount = get_discount(SCRAPER_NAME, category)
        
        # now scrape all items with this url
        items = await scrape_cards(url,
                                    current_discount,
                                    category,
                                    slugs,
                                    semaphore)

        # VALIDATE AND PARSE ITEMS
        tasks = []
        for item in items:
            
            if item:
                tasks.append(parse_and_validate(item, semaphore))
                
        
        prepared_to_db = await asyncio.gather(*tasks)

        # SEND RESULT TO DB
        tasks = []
        if prepared_to_db:
            for item in prepared_to_db:
                if item:
                    tasks.append(send_item_to_db(item, semaphore))

            await asyncio.gather(*tasks)


start_time = datetime.now()
try:
    work_notification("start")
    parser_start_notification(SCRAPER_NAME)
    asyncio.run(run_acer_scraper())
    end_time = datetime.now()

    execution_time = end_time - start_time
    logging.info("Parsing is done. Congratulations!")
    print(f"Execution time: {execution_time} seconds")

    scraping_session = ScrapingSession(date=start_time,
                                       status="success",
                                       items_scraped=counter["filtered_and_sended"],
                                       name=SCRAPER_NAME)


    close_db_connection()
    info = work_notification("finish")
    # add successful session to statisctics
    update_statistics(scraping_session)
    send_success_report(parser_name=SCRAPER_NAME,
                        items_amount=counter["filtered_and_sended"],
                       info=info)

except Exception as exception:
    print(traceback.format_exc())
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
