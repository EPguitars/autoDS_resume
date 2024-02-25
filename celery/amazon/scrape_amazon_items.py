import asyncio
import logging
import json
from datetime import datetime

from db_operations import *
from validator import scrape_item_page, send_item_to_db
from tools import proxy_gen, render
from models import Item
import aiolimiter
from celery import chain, group


from categories import categories
from main_tools.discount_api import get_discount

semaphore = aiolimiter.AsyncLimiter(200, 1)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")


def item_generator(items):

    for item in items:
        data = {"source_product_id": item[0],
                "asin": item[1],
                "url": item[2],
                "source_id": "amazon_2",
                "category": item[4],
                "slugs": json.loads(item[5]),
                "title": item[6]}

        yield data


async def process_batch(batch):
    """ function to process batch of items """
    start = datetime.now()
    await asyncio.gather(*batch)
    end = datetime.now()
    print(f"BATCH DONE, time spended - {end - start}")


async def update_items(db_manager: AsyncDatabaseManager, parameter: str):
    """ function for updating info about item first script didn't met """

    # First get amount of item needed to update

    if parameter == "others":
        data = await db_manager.grab_items()
    elif parameter == "on website":
        data = await db_manager.grab_items_on_website()
        #data = await db_manager.grab_custom_items()


    items = item_generator(data)
    logging.info(f"ITEMS NEEDED TO BE UPDATED: {len(data)}")

    tasks = []

    for item in items:
        tasks.append(scrape_item_page(db_item=item,
                                      proxy=next(proxy_gen),
                                      render=next(render),
                                      semaphore=semaphore,
                                      db_manager=db_manager))

    total_tasks = len(tasks)
    batch_size = 150
    tasks_left = total_tasks

    for i in range(0, total_tasks, batch_size):
        batch = tasks[i:i+batch_size]
        await process_batch(batch)
        await db_manager.perform_transcation()
        tasks_left -= batch_size
        logging.info(f"TASKS LEFT: {tasks_left}")


async def run_item_scraper():
    db = AsyncDatabaseManager(
        host="localhost",
        port=3306,
        user="admin",
        password="ioHNS16HUQeLLtTv",
        db="admin_main"
    )

    await db.connect()
    await update_items(db_manager=db, parameter="on website")


if __name__ == "__main__":
    asyncio.run(run_item_scraper())
