import re
import math
import json
import hashlib
import logging
from urllib.parse import (urlparse,
                          parse_qsl,
                          urlencode,
                          urlunparse,
                          urljoin)

import httpx
from selectolax.parser import HTMLParser, Selector
from rich import print

from headers import headers
from models import Item
from tools import SCRAPER_NAME
from main_tools.proxy import proxy_gen

DISCOUNT_SET = 20
BASE_URL = "https://www.WEBSITE.SOMETHING"


def add_query_to_url(existing_url, new_query_params):
    parsed_url = list(urlparse(existing_url))
    parsed_url[4] = urlencode(
        {**dict(parse_qsl(parsed_url[4])), **new_query_params})
    return urlunparse(parsed_url)


async def get_pages_links(url, attempts=3) -> list:
    """ generate all links for current category """
    # first know how many pages category have
    proxy = next(proxy_gen)
    async with httpx.AsyncClient(headers=headers, proxies=proxy) as client:
        try:
            response = await client.get(url, timeout=60, follow_redirects=True)

        except (httpx.ConnectTimeout,
                httpx.ReadTimeout,
                httpx.ConnectError,
                httpx.ReadError):
            if attempts > 0:
                attempts -= 1
                logging.warning("Connection error, retrying request")
                return await get_pages_links(url, attempts)

            else:
                logging.warning("Connection error when scraping pages")
                return None
        
        except httpx.ProxyError:
            if attempts > 0:
                attempts -= 1
                logging.warning("Proxy error when getting page urls")
                return await get_pages_links(url, attempts)

            else:
                logging.warning("Proxy error after 10 attempts when getting page urls")
                return None
        # also cathcing something unexpected
        except Exception as exc:
            logging.warning("Exception when scraping pages : %s", exc)
            return None

        selectolax = HTMLParser(response.text)
        # get number of pages
        items_amount_selector = selectolax.css_first(
            "span.category-total-item")
        # 1 page contains 50 items
        if items_amount_selector:
            items_amount = float(items_amount_selector.text())
            pages_amount = math.ceil(items_amount / 24)

            # create urls for each page
            pages_urls = []

            for page in range(1, pages_amount+1):
                query = {"page": page}
                page_url = add_query_to_url(url, query)
                pages_urls.append(page_url)

            return pages_urls


def check_discount(old_price: float, current_price: float, current_discount: float):
    """ calculate discount """
    discount = round((1 - current_price / old_price) * 100)

    if discount:
        if round(discount) >= current_discount:
            return True
        else:
            return False
    else:
        logging.warning("Can't calculate discount, requires recheck")
        return None


def parse_product_url(item: Selector):
    """ parse url of product """
    url_selector = item.css_first("a.product-item-link")

    if url_selector:
        url = url_selector.attributes["href"]
        return url

    else:
        logging.warning("Can't locate url, requires markup recheck")
        return None


def check_availability(item):
    """
    True if out of stock
    """
    sold_out_selector = item.css_first("div.product-item-actions > div.stock")

    if sold_out_selector:
        current_status = sold_out_selector.attributes["class"]
        
        if "available" in current_status:
            return True
        
        elif "unavailable" in current_status:
            return False
        
        else:
            logging.warning("Can't detect status of availability")
            return None
        
    else:
        logging.warning("cant locate product badge, requires recheck")
        return None


def parse_product_title(item: Selector):
    """ parse title of current product """
    title_selector = item.css_first("a.product-item-link")

    if title_selector:
        title = title_selector.text(strip=True)

        return title

    else:
        logging.warning("Can't locate url, requires markup recheck")
        return None


def parse_item_price(item: Selector,
                     parameter: str):
    """ parse prices of current item """

    if parameter == "old":
        old_price_selector = item.css_first("span[data-price-type='oldPrice']")

        if old_price_selector:
            old_price = float(old_price_selector.text()
                              .replace("₹", "")
                              .replace(",", ""))
            return old_price

        else:
            logging.warning("Can't locate old price, requres rechecking")
            return None

    elif parameter == "current":
        current_price_selector = item.css_first("span[data-price-type='finalPrice']")

        if current_price_selector:
            current_price = float(current_price_selector.text()
                                  .replace("₹", "")
                                  .replace(",", ""))
            return current_price

        else:
            logging.warning("Can't locate current price, requires rechecking")
            return None


def parse_card(item: Selector, current_discount: float, 
               category: str, slugs: list):
    """ parse item card """

    old_price = parse_item_price(item, "old")
    current_price = parse_item_price(item, "current")
    
    if not old_price or not current_price:
        return None
    # check for discount
    discount = check_discount(old_price, current_price, current_discount)
    available = check_availability(item)

    if discount and available:
        url = parse_product_url(item)
        product_id = hashlib.md5(url.encode()).hexdigest()

        parsed_item = Item(
            source_id=SCRAPER_NAME,
            source_product_id=product_id,
            url=url,
            title=parse_product_title(item),
            old_price=old_price,
            current_price=current_price,
            images=None,
            description=None,
            original_category=category,
            coupon_code=None,
            categories=slugs
        )

        if not parsed_item.current_price or not parsed_item.old_price:
            return None
        if not parsed_item.title:
            return None
        if not parsed_item.source_product_id:
            return None
        if not parsed_item.url:
            return None
        
        return parsed_item

    else:
        return None


async def scrape_cards(url,
                       current_discount,
                       category,
                       slugs,
                       semaphore,
                       attempts=7):
    """ scrape all cards of current category """
    proxy = next(proxy_gen)
    async with semaphore:
        async with httpx.AsyncClient(headers=headers, proxies=proxy) as client:
            try:
                response = await client.get(url, timeout=60, follow_redirects=True)

            except (httpx.ConnectTimeout,
                    httpx.ReadTimeout,
                    httpx.ConnectError,
                    httpx.ReadError):
                if attempts > 0:
                    attempts -= 1
                    logging.warning("Connection error, retrying request")
                    return await scrape_cards(url,
                                              current_discount,
                                              category,
                                              slugs,
                                              semaphore,
                                              attempts)

                else:
                    logging.warning("Connection error when scraping pages")
                    return None
            
            except httpx.ProxyError:
                if attempts > 0:
                    attempts -= 1
                    logging.warning("Proxy error when getting page urls")
                    return await scrape_cards(url,
                                              current_discount,
                                              category,
                                              slugs,
                                              semaphore,
                                              attempts)

                else:
                    logging.warning("Proxy error after 7 attempts when scraping card")
                    return None
                # also cathcing something unexpected
            
            except Exception as exc:
                logging.warning("Exception when scraping pages : %s", exc)
                return None

            selectolax = HTMLParser(response.text)
            
            # get all item cards
            item_selectors = selectolax.css("li.product-item")
            # parse each card and extract all possible info
            parsed_items = []

            if item_selectors:
                for item in item_selectors:
                    parsed_item = parse_card(item,
                                             current_discount,
                                             category,
                                             slugs)
                    parsed_items.append(parsed_item)

            return parsed_items
