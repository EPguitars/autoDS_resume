import os
import re
import asyncio
import logging
import hashlib
from urllib.parse import urljoin

import httpx
from rich.logging import RichHandler
from rich import print
from aiolimiter import AsyncLimiter
from selectolax.parser import HTMLParser, Selector
from dotenv import load_dotenv
import numpy as np

import tools
from tools import SCRAPER_NAME
from categories import categories
from cards_scraper import get_pages_links, scrape_cards
from main_tools.discount_api import send_discount

load_dotenv()

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s",
                    handlers=[RichHandler()])

BASE_URL = "https://store.acer.com/en-in/"
API_KEY = os.getenv("API_KEY")
limiter = AsyncLimiter(10, 1)
sync_client = httpx.Client()


def count_discount(old_price, current_price):
    """ check if discount is present """

    if old_price and current_price:
        discount = round((old_price - current_price) / (old_price / 100))

        if discount:
            return float(discount)
        else:
            return None
    else:
        return None


async def update_average_discounts():
    """ update average discounts in database """
    for meta, slugs in categories.items():

        path = meta[0]
        category = meta[1]
        discount_list = []
        current_discount = 1.0
        
        # Construct query to get all items from category with only one request
        url = BASE_URL + path + "?product_list_limit=all"
        page_urls = await get_pages_links(url)

        # now scrape all items with this url
        items = await scrape_cards(url,
                                    current_discount,
                                    category,
                                    slugs,
                                    limiter)
        if items:
            for item in items:
                if item:
                    discount_list.append(count_discount(
                        item.old_price, item.current_price))

        filtered_list = [
            item for item in discount_list if isinstance(item, float)]
        
        average_discount = np.mean(filtered_list)
        average_discount = 1.0 if np.isnan(
            average_discount) else average_discount

        products_amount = len(filtered_list)
        main_slug = slugs[0]
        sub_slug = slugs[1]
        print(f"Average discount for {category} is {average_discount} for {products_amount} products")
        send_discount(category,
		              average_discount,
		              products_amount,
		              main_slug,
		              sub_slug,
		              SCRAPER_NAME)


asyncio.run(update_average_discounts())
