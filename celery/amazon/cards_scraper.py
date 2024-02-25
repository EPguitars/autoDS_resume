import re
import math
import asyncio
import logging
import random
import aiohttp
from urllib.parse import (urlparse,
                          parse_qsl,
                          urlencode,
                          urlunparse,
                          urljoin)

from aiolimiter import AsyncLimiter
import httpx
from selectolax.parser import HTMLParser, Selector
from rich import print
import requests

from headers import headers
from models import Item
from tools import SCRAPER_NAME, proxy_gen, ua_gen, render
from db_operations import *
from main_tools.discount_api import get_discount
from main_tools.proxy import aiohttp_proxy
from standart_page_parser import parse_popular_card

DISCOUNT_SET = 50
BASE_URL = "https://www.amazon.in"


async def fetch_response(url, proxy, limiter):
    # PROXY_END_POINT = "http://" + proxy.split("@")[-1]
    # USERNAME = proxy.get("username")
    # PASSWORD = proxy.split("@")[0].replace("http://", "").split(":")[1]

    try:
        async with limiter:
            async with aiohttp.ClientSession(headers=headers) as client:
                async with client.get(url, timeout=60, proxy=proxy) as response:
                    print(response.status)
                    
                    if response.status != 200:
                        return "not 200"
                    
                    else:
                        
                        return await response.text()
    
    except aiohttp.client_exceptions.ClientPayloadError:
        return None

    except aiohttp.client_exceptions.ClientConnectorError:
        logging.warning("ClientConnectorError in cards_scraper")
        return None


def add_query_to_url(existing_url, new_query_params):
    parsed_url = list(urlparse(existing_url))
    parsed_url[4] = urlencode(
        {**dict(parse_qsl(parsed_url[4])), **new_query_params})
    return urlunparse(parsed_url)


async def get_pages_links(url: str,
                          proxy: dict,
                          render_worker: str,
                          attempts=3) -> int:
    """ generate all links for current category """
    # first know how many pages category have
    user_agent = next(ua_gen)
    lua_proxy = f"""
    function main(splash, args)
        -- Define the proxy server and port
        local proxy_server = "{proxy["host"]}"
        local proxy_port = {proxy['port']}

        -- Define the target URL
        local url = args.url

        -- Specify user-agent
        local custom_headers = {{
        ['authority'] = 'www.amazon.in',
        ['accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        ['accept-language'] = 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        ['cookie'] = 'session-id=261-2598564-2341616; i18n-prefs=INR; ubid-acbin=261-8681770-6744109; ld=AZINSOANavDesktop_T3; AMCV_A7493BC75245ACD20A490D4D%40AdobeOrg=1585540135%7CMCIDTS%7C19602%7CMCMID%7C14372875062132278590998275670879462650%7CMCAAMLH-1694198007%7C6%7CMCAAMB-1694198007%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1693600407s%7CNONE%7CvVersion%7C4.4.0; session-id-time=2082787201l; session-token=qQ/UIK0urZpEfHdwxTl1pXpdB+pjU3SCkPrUoujZdtKAS2bKk5cjD4uXgRk7aO2kuqvs5BazdZE30tYSO/heLRAY3ZFoaW/tBVZzEPzOEmaZfm0BPjxmhe4bGTaOQUkh5Y0jIdu+XUxYOjHQdj5Z2mD1kqvV44QoYhToVb6N+SDSz1Da0JcNP2YFRp/ickXM0na0T8alUhqF4ggzXKbedZwgzzEqT9FP2ufsDPh3HkBaGYY1bMZ6qTGKR2UQQiTc0VLUjybxpbEulpjHxW/crOwkBEtFoO00G9jUIn36ZaXrIpmdp1qMqaUSsUs0fI68eAof4w7qwMt5swmYCTRQU/AS+A/L5VH9; csm-hit=tb:s-HA2V6WH0SHAK96067SE3|1693755816065&t:1693755817727&adb:adblk_no',
        ['device-memory'] = '8',
        ['downlink'] = '8.05',
        ['dpr'] = '1',
        ['ect'] = '4g',
        ['rtt'] = '50',
        ['sec-ch-device-memory'] = '8',
        ['sec-ch-dpr'] = '1',
        ['sec-ch-ua'] = '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
        ['sec-ch-ua-mobile'] = '?0',
        ['sec-ch-ua-platform'] = '"Windows"',
        ['sec-ch-ua-platform-version'] = '"10.0.0"',
        ['sec-ch-viewport-width'] = '811',
        ['sec-fetch-dest'] = 'document',
        ['sec-fetch-mode'] = 'navigate',
        ['sec-fetch-site'] = 'none',
        ['sec-fetch-user'] = '?1',
        ['upgrade-insecure-requests'] = '1',
        ['user-agent'] = '{user_agent}',
        ['viewport-width'] = '811'
        }}
        
        -- Intercept requests and set up a proxy
        splash:on_request(function(request)
            request:set_proxy{{
                type = "HTTP",
                host = proxy_server,
                port = proxy_port,
                username = "{proxy['username']}",  -- Optional: If the proxy requires authentication
                password = "{proxy['password']}"  -- Optional: If the proxy requires authentication
            }}
        end)
        splash:set_custom_headers(custom_headers)
        -- Navigate to the target URL
        response = splash:go(url)

        -- Wait for the page to load (add more time if needed)
        splash:wait({random.randint(2,5)})
        -- Return the HTML content
        return splash:html()
    end
    """
    logging.info(f"GETTING PAGES LINKS FOR {url}")
    async with httpx.AsyncClient(headers=headers) as client:
        try:
            response = await client.post(render_worker, json={"url": url, "wait": 15, "timeout": 300, "resource_timeout": 299, "lua_source": lua_proxy}, timeout=300)

            if response.status_code == 504:
                logging.warning("504 ERROR")
                return await get_pages_links(url, proxy, render_worker, attempts)

            elif response.status_code == 503:
                proxy = next(proxy_gen)
                render_worker = next(render)
                return await get_pages_links(url, proxy, render_worker, attempts)

        except (httpx.ConnectTimeout,
                httpx.ReadTimeout,
                httpx.ConnectError,
                httpx.ReadError):
            if attempts > 0:
                attempts -= 1
                logging.warning("Connection error, retrying request")
                return await get_pages_links(url, proxy, render_worker, attempts)

            else:
                logging.warning("Connection error when scraping pages")
                return None

        # also cathcing something unexpected
        except Exception as exc:
            logging.warning("Exception when scraping pages : %s", exc)
            return None

    selectolax = HTMLParser(response.text)

    if "automated access" in response.text or "requires authentication" in response.text:
        logging.warning("CAPTCHA")
        proxy = next(proxy_gen)
        render_worker = next(render)
        return await get_pages_links(url, proxy, render_worker, attempts)

    page_amount_selectors_span = selectolax.css("span.s-pagination-item")
    page_amount_selectors_a = selectolax.css("a.s-pagination-item")
    print([x.text() for x in page_amount_selectors_span])
    print([x.text() for x in page_amount_selectors_a])
    # q: is that correct syntax for selectolax selector?
    results_bar = selectolax.css_first(
        'div[cel_widget_id="UPPER-RESULT_INFO_BAR-0"] >> span')

    if results_bar:
        try:
            items_number = int(results_bar.text().split(" ")
                               [-2].replace(",", ""))
            print(items_number)
        except ValueError:
            if results_bar == "results":
                items_number = int(results_bar.text().split(
                    " ")[-3].replace(",", ""))
                print(items_number)

    else:
        logging.warning("Can't locate results bar, blocking expecting")

        if attempts > 0:
            await asyncio.sleep(2)
            proxy = next(proxy_gen)
            render_worker = next(render)
            attempts -= 1
            print(attempts)

            return await get_pages_links(url, proxy, render_worker, attempts)

        else:
            logging.warning("Can't locate results bar, blocking")
            return None

    if page_amount_selectors_span:
        print("SPAN")
        # create urls for each page
        try:
            page_amount = int(page_amount_selectors_span[-1].text())
        except ValueError:
            page_amount = int(page_amount_selectors_span[-2].text())

        pages_urls = []

        for page in range(1, page_amount+1):
            query = {"page": page, "ref": f"sr_pg_{page}"}
            page_url = add_query_to_url(url, query)
            pages_urls.append(page_url)

        if len(pages_urls) > 1:
            return pages_urls

        elif len(pages_urls) == 1 and items_number <= 24:
            pages_urls.append(url)
            return pages_urls

        elif page_amount_selectors_a:
            print("A")
            # create urls for each page
            page_amount = int(page_amount_selectors_a[-2].text())
            pages_urls = []

            for page in range(1, page_amount+1):
                query = {"page": page, "ref": f"sr_pg_{page}"}
                page_url = add_query_to_url(url, query)
                pages_urls.append(page_url)

            if len(pages_urls) > 1:
                return pages_urls

            elif len(pages_urls) == 1 and items_number <= 24:
                return pages_urls

            else:
                print("NOTHING DETECTED")
                return [url]

        else:
            return [url]


def check_discount(old: float, sell: float, current_discount: float):
    """ checks if there any discount on this item """
    if old and sell:
        discount = (old - sell) / (old / 100)

    else:
        #print(old, sell)
        return None

    if discount >= current_discount and discount < 91.0 :
        return True

    else:
        #print(old, sell, discount)
        return None


def parse_asin(item: Selector):
    """ parse id of current item """
    asin = item.attributes["data-asin"]

    if asin:
        return asin

    else:
        logging.warning("Can't locate asin, requires markup recheck")
        return None


def parse_product_url(asin: str):
    """ parse url of product """
    url = BASE_URL + f"/dp/{asin}"
    return url


def check_availability(item):
    """
    True if out of stock
    """
    sold_out_selector = item.css_first("div.t4s-product-badge")

    if sold_out_selector:
        for element in sold_out_selector.css("span"):
            if element.text().upper() == "SOLD OUT":
                sold_out = True

                return sold_out
        return False

    else:
        logging.warning("cant locate product badge, requires recheck")
        return None


def parse_product_title(item: Selector):
    """ parse title of current product """
    title_selector = item.css("h2")

    if title_selector:
        title = " - ".join([x.text() for x in title_selector])

        return title

    else:
        logging.warning("Can't locate url, requires markup recheck")
        return None


def parse_item_price(item: Selector, parameter: str):
    """ parse prices of current item """

    if parameter == "old":
        old_price_selector = item.css_first(".a-text-price > span")

        if old_price_selector:
            old_price = float(old_price_selector.text()
                              .replace("₹", "")
                              .replace(",", ""))
            return old_price

        else:
            logging.warning("Can't locate old price, requres rechecking")
            return None

    elif parameter == "current":
        current_price_selector = item.css_first(".a-price-whole")

        if current_price_selector:
            current_price = float(
                current_price_selector.text().replace(",", ""))
            return current_price

        else:
            logging.warning("Can't locate current price, requires rechecking")
            return None


def parse_card(item: Selector, current_discount: float, category: str, slugs: list):
    """ parse item card """
    # check for discount
    old_price = parse_item_price(item, "old")
    current_price = parse_item_price(item, "current")

    discount = check_discount(
        old=old_price, sell=current_price, current_discount=current_discount)

    if discount:
        asin = parse_asin(item)
        parsed_item = Item(
            source_id=SCRAPER_NAME,
            source_product_id=asin,
            url=parse_product_url(asin),
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
        print(discount)
        return None


async def scrape_cards(url,
                       current_discount,
                       category,
                       slugs,
                       proxy,
                       render,
                       semaphore,
                       attempts=3):
    """ scrape all cards of current category """
    user_agent = next(ua_gen)
    lua_proxy = f"""
    function main(splash, args)
        -- Define the proxy server and port
        local proxy_server = "{proxy['host']}"
        local proxy_port = {proxy['port']}

        -- Define the target URL
        local url = args.url

        -- Specify user-agent
        local custom_headers = {{
        ['authority'] = 'www.amazon.in',
        ['accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        ['accept-language'] = 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        ['cookie'] = 'session-id=261-2598564-2341616; i18n-prefs=INR; ubid-acbin=261-8681770-6744109; ld=AZINSOANavDesktop_T3; AMCV_A7493BC75245ACD20A490D4D%40AdobeOrg=1585540135%7CMCIDTS%7C19602%7CMCMID%7C14372875062132278590998275670879462650%7CMCAAMLH-1694198007%7C6%7CMCAAMB-1694198007%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1693600407s%7CNONE%7CvVersion%7C4.4.0; session-id-time=2082787201l; session-token=qQ/UIK0urZpEfHdwxTl1pXpdB+pjU3SCkPrUoujZdtKAS2bKk5cjD4uXgRk7aO2kuqvs5BazdZE30tYSO/heLRAY3ZFoaW/tBVZzEPzOEmaZfm0BPjxmhe4bGTaOQUkh5Y0jIdu+XUxYOjHQdj5Z2mD1kqvV44QoYhToVb6N+SDSz1Da0JcNP2YFRp/ickXM0na0T8alUhqF4ggzXKbedZwgzzEqT9FP2ufsDPh3HkBaGYY1bMZ6qTGKR2UQQiTc0VLUjybxpbEulpjHxW/crOwkBEtFoO00G9jUIn36ZaXrIpmdp1qMqaUSsUs0fI68eAof4w7qwMt5swmYCTRQU/AS+A/L5VH9; csm-hit=tb:s-HA2V6WH0SHAK96067SE3|1693755816065&t:1693755817727&adb:adblk_no',
        ['device-memory'] = '8',
        ['downlink'] = '8.05',
        ['dpr'] = '1',
        ['ect'] = '4g',
        ['rtt'] = '50',
        ['sec-ch-device-memory'] = '8',
        ['sec-ch-dpr'] = '1',
        ['sec-ch-ua'] = '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
        ['sec-ch-ua-mobile'] = '?0',
        ['sec-ch-ua-platform'] = '"Windows"',
        ['sec-ch-ua-platform-version'] = '"10.0.0"',
        ['sec-ch-viewport-width'] = '811',
        ['sec-fetch-dest'] = 'document',
        ['sec-fetch-mode'] = 'navigate',
        ['sec-fetch-site'] = 'none',
        ['sec-fetch-user'] = '?1',
        ['upgrade-insecure-requests'] = '1',
        ['user-agent'] = '{user_agent}',
        ['viewport-width'] = '811'
        }}
        
        -- Intercept requests and set up a proxy
        splash:on_request(function(request)
            request:set_proxy{{
                type = "HTTP",
                host = proxy_server,
                port = proxy_port,
                username = "{proxy['username']}",  -- Optional: If the proxy requires authentication
                password = "{proxy['password']}"  -- Optional: If the proxy requires authentication
            }}
        end)
        splash:set_custom_headers(custom_headers)
        -- Navigate to the target URL
        response = splash:http_get(url)

        -- Wait for the page to load (add more time if needed)
        splash:wait({random.randint(2, 5)})
        -- Return the HTML content
        return response.body
    end
    """
    async with semaphore:
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(render,
                                             json={"url": url,
                                                   "wait": 15,
                                                   "timeout": 300,
                                                   "resource_timeout": 299,
                                                   "lua_source": lua_proxy},
                                             timeout=300)

                if response.status_code == 504:
                    logging.warning("504 ERROR")
                    proxy = next(proxy_gen)
                    return await scrape_cards(url,
                                              current_discount,
                                              category,
                                              slugs,
                                              proxy,
                                              render,
                                              semaphore,
                                              attempts)
                elif response.status_code == 400:
                    print(url)
            except (httpx.ConnectTimeout,
                    httpx.ReadTimeout,
                    httpx.ConnectError,
                    httpx.ReadError):
                if attempts > 0:
                    attempts -= 1
                    logging.warning("Connection error, retrying request")
                    print(url)
                    print(render)
                    proxy = next(proxy_gen)
                    return await scrape_cards(url,
                                              current_discount,
                                              category,
                                              slugs,
                                              proxy,
                                              render,
                                              semaphore,
                                              attempts)

                else:
                    logging.warning("Connection error when scraping pages")
                    return None

            except httpx.ProxyError:
                if attempts > 0:
                    attempts -= 1
                    logging.warning("Proxy error, retrying request")
                    print(url)
                    print(render)
                    proxy = next(proxy_gen)
                    return await scrape_cards(url,
                                              current_discount,
                                              category,
                                              slugs,
                                              proxy,
                                              render,
                                              semaphore,
                                              attempts)
                else:
                    logging.error("PROXY FAIL")
                    return None
            # also cathcing something unexpected
            except Exception as exc:
                logging.warning("Exception when scraping pages : %s", exc)
                return None

    selectolax = HTMLParser(response.text)

    if "automated access" in response.text or "requires authentication" in response.text:
        logging.warning("CAPTCHA")
        proxy = next(proxy_gen)
        await asyncio.sleep(1)
        return await scrape_cards(url,
                                  current_discount,
                                  category,
                                  slugs,
                                  proxy,
                                  render,
                                  semaphore,
                                  attempts)
    # get all item cards
    item_selectors = selectolax.css("div.s-result-item.s-asin")

    # parse each card and extract all possible info
    parsed_items = []

    if item_selectors:
        for item in item_selectors:
            parsed_item = parse_card(item,
                                     current_discount,
                                     category,
                                     slugs)

            if parsed_item == "no discount":
                logging.warning(f"NO DISCOUNT HERE {url}")
                continue
            elif parsed_item == "no price":
                logging.warning(f"NO PRICE HERE {url}")
                continue
            elif parsed_item == "no title":
                logging.warning(f"NO TITLE HERE {url}")
                continue
            elif parsed_item == "no asin":
                logging.warning(f"NO ASIN HERE {url}")
                continue
            elif parsed_item == "no url":
                logging.warning(f"NO URL HERE {url}")
                continue
            else:
                parsed_items.append(parsed_item)
                continue

        return parsed_items

    else:
        logging.warning("Can't locate item cards, requires markup recheck")


async def scrape_populars(page: int, 
                        path: str, 
                        category: str, 
                        current_discount: float, 
                        slugs: list[str], 
                        limiter,
                        attempts=7):
    
    url = "https://www.amazon.in/s?" + add_query_to_url(path, {"page": page, "ref": f"sr_pg_{page}"})
    
    proxy = next(aiohttp_proxy)

    try: 
        response = await fetch_response(url, proxy, limiter)
    
    except aiohttp.client_exceptions.ClientHttpProxyError:
        if attempts > 0:
            attempts -= 1
            logging.warning("Proxy error when getting page urls")
            return await scrape_populars(page, 
                                        path, 
                                        category, 
                                        current_discount, 
                                        slugs, 
                                        limiter,
                                        attempts)
        
        else:
            logging.warning("Proxy error after 20 attempts when getting page urls")
            return None  
    
    except aiohttp.client_exceptions.ServerDisconnectedError:
        if attempts > 0:
            attempts -= 1
            logging.warning("Server disconnected error when getting page in validator")
            return await scrape_populars(page, 
                                        path, 
                                        category, 
                                        current_discount, 
                                        slugs, 
                                        limiter,
                                        attempts)
        
        else:
            logging.warning("Server disconnected error after 10 attempts when getting page in validator")
            return None

    except asyncio.TimeoutError:
        if attempts > 0:
            attempts -= 1
            logging.warning("Timeout error when getting page urls")
            return await scrape_populars(page, 
                                        path, 
                                        category, 
                                        current_discount, 
                                        slugs, 
                                        limiter,
                                        attempts)
        
        else:
            logging.warning("Timeout error after 10 attempts when getting page urls")
            return None

    if response == "not 200" or not response:
        if attempts > 0:
            attempts -= 1
            logging.warning("Not 200")
            return await scrape_populars(page, 
                                        path, 
                                        category, 
                                        current_discount, 
                                        slugs, 
                                        limiter,
                                        attempts)
        
        else:
            logging.error("Not 200")
            return None  

    if not response:
        logging.warning("Empty response")
        
        if attempts > 0:
            attempts -= 1
            logging.warning("Not 200")
            return await scrape_populars(page, 
                                        path, 
                                        category, 
                                        current_discount, 
                                        slugs, 
                                        limiter,
                                        attempts)
        
        else:
            logging.warning("Not 200")
            return None  
    

    selectolax = HTMLParser(response)
    # get all item cards
    item_selectors = selectolax.css("div.s-result-item.s-asin")

    # parse each card and extract all possible info
    parsed_items = []

    if item_selectors:
        for item in item_selectors:
            parsed_item = parse_popular_card(item,
                                     current_discount,
                                     category,
                                     slugs)
            parsed_items.append(parsed_item)

    return parsed_items

# LABORATORY FOR NEW MARKUPS
if __name__ == "__main__":

    # url = "https://www.amazon.in/s?i=shoes&rh=n%3A1983634031%2Cp_n_pct-off-with-tax%3A2665400031&dc&fs=true&page=2&qid=1695514414&rnid=2665398031&ref=sr_pg_2"
    # url = "https://www.amazon.in/s?k=wedges&i=shoes&rh=n%3A1983578031%2Cp_n_feature_seven_browse-bin%3A27400655031%2Cp_n_pct-off-with-tax%3A2665400031&dc&page=2&qid=1695514474&rnid=2665398031&ref=sr_pg_2"
    url = "https://www.amazon.in/s?i=apparel&rh=n%3A1968526031%2Cp_n_pct-off-with-tax%3A2665400031&dc&fs=true&page=3&qid=1695514573&rnid=2665398031&ref=sr_pg_3"

    proxy = next(proxy_gen)
    render_worker = next(render)

    result = asyncio.run(get_pages_links(url,
                                         proxy,
                                         render_worker,
                                         attempts=3))

    print(result)
