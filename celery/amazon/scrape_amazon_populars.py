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
from validator import parse_and_validate
from cards_scraper import (get_pages_links, 
                           scrape_cards, 
                           fetch_response, 
                           scrape_populars)

from tools import counter, SCRAPER_NAME
from notificator.telegram_agent import send_error_report, send_success_report, parser_start_notification
from notificator.db_config import ScrapingSession
from main_tools.discount_api import get_discount
from pymysql_manager import send_item_to_db, close_db_connection

load_dotenv()

API_KEY = os.getenv("API_KEY")
client = httpx.Client()
semaphore = AsyncLimiter(10, 1)
BASE_URL = "https://www.amazon.in"

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


async def run_SCRAPERNAME_scraper():
    """ main logic for my scraper """

    start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    limit = 97
    count = 0

    for meta, slugs in categories.items():
        
        if count == limit:
            break
        pages = range(1, 4)
        path = meta[0]
        category = meta[1]
        print(category)
        #current_discount = 20.0
        current_discount = get_discount(SCRAPER_NAME, category)
        # now get url for all pages of category

        tasks = []
        for page in pages:
            tasks.append(scrape_populars(page, path, category, 
                                       current_discount, slugs, semaphore))
        
        items = await asyncio.gather(*tasks)
        
        print(items)
        print("====================")
        print(len(items))

        results = []
        
        for block in items:
            if block:
                for item in block:
                    results.append(item)

        # VALIDATE AND PARSE ITEMS
        item_limiter = 100
        sended_items = 0
        
        for block in items:
            
            tasks = []
            if block:

                for item in block:
                    if sended_items >= item_limiter:
                        break
                    
                    if item:
                        tasks.append(parse_and_validate(item, semaphore))
                        sended_items += 1
                        
                
                prepared_to_db = await asyncio.gather(*tasks)

                # SEND RESULT TO DB
                tasks = []
                if prepared_to_db:
                    for item in prepared_to_db:
                        if item:
                            tasks.append(send_item_to_db(item, semaphore))

                    await asyncio.gather(*tasks)

        count += 1

    
if __name__ == "__main__":
    start_time = datetime.now()
    try:
        work_notification("start")
        parser_start_notification(SCRAPER_NAME)
        asyncio.run(run_SCRAPERNAME_scraper())
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
