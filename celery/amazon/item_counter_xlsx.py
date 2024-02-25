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
import pandas as pd

import tools
from tools import SCRAPER_NAME
from categories import categories
from scrape_amazon import update_asin_table
from main_tools.discount_api import send_discount

load_dotenv()

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s",
                    handlers=[RichHandler()])

BASE_URL = "https://www.amazon.in"
API_KEY = os.getenv("API_KEY")
limiter = AsyncLimiter(80, 1)
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
    json_for_excel = {"ID магазина" : [],
                      "Категория" : [],
                      "Количество товаров" : []}
    
    temp = 0
    for meta, slugs in categories.items():
        if temp == 97:
                break
        
        path = meta[0]
        category = meta[1]
        url = "https://www.amazon.in/s?" + path
        discount_list = []

        scraped_items = await update_asin_table(category, url, slugs, None, "amounts")

        if scraped_items:
            for block in scraped_items:
                if block:
                    for item in block:
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

        json_for_excel["ID магазина"].append(SCRAPER_NAME)
        json_for_excel["Категория"].append(category)
        json_for_excel["Количество товаров"].append(products_amount)
        temp += 1

    relative_path = 'myprojects/discounts_python_scrapers/items_amount_data.xlsx'

    # Create the absolute path by joining with the C disk identifier
    absolute_path = os.path.join('C:', os.sep, relative_path)
    
    df = pd.read_excel(absolute_path, sheet_name='Shops info')
    new_data = pd.DataFrame(json_for_excel)
    # Append new rows to the existing DataFrame
    df = pd.concat([df, new_data], ignore_index=True)

    # Save the modified DataFrame back to Excel
    df.to_excel(absolute_path, sheet_name='Shops info', index=False)  # Replace 'Sheet1' with your sheet name


asyncio.run(update_average_discounts())
