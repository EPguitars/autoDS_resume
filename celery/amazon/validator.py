import os
import re
import json
import math
import aiohttp
import random
import anyio
import asyncio
import logging
from asyncio import Semaphore
from urllib.parse import (urlparse,
                          parse_qsl,
                          urlencode,
                          urlunparse,)
import dataclasses

import httpx
import selenium
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
# i need to import capability type from selenium
from selectolax.parser import HTMLParser, Selector
from rich import print
from celery import Celery
from aiolimiter import AsyncLimiter
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.proxy import Proxy, ProxyType

from image_validation import validate_image
from headers import headers
from models import Item
from tools import SCRAPER_NAME, proxy_gen, ua_gen, counter#, discounts_info
from celery_worker import celery_db_update
from db_operations import AsyncDatabaseManager
from pymysql_manager import check_db, get_images_db, get_description_db
from main_tools.proxy import aiohttp_proxy
from cards_scraper import fetch_response
from standart_page_parser import standart_itempage_parser

load_dotenv()
db_limiter = asyncio.Semaphore(200)
DISCOUNT_SET = 50
BASE_URL = "https://www.amazon.in"
API_KEY = os.getenv("API_KEY")


def scrape_description(markup: Selector):
    """ scrape and parse item's category """
    description_selectors_1 = markup.css(
        "div.featureBulletsExpanderContent >> ul")
    description_selectors_2 = markup.css("div#feature-bullets >> ul")
    description_selectors_3 = markup.css("div#detailBullets_feature_div >> ul")
    description_selectors_4 = 'markup.css("")'

    description = ""

    if description_selectors_1:
        for element in description_selectors_1:
            description += element.text() + " \n"

        return description

    elif description_selectors_2:
        for element in description_selectors_2:
            description += element.text() + " \n"

        return description

    elif description_selectors_3:
        for element in description_selectors_3:
            description += element.text() + " \n"

        return description

    else:
        logging.warning("Can't locate description, requires markup recheck")
        return None


def add_query_to_url(existing_url, new_query_params):
    """ reconstruct url with new query data """
    parsed_url = list(urlparse(existing_url))
    parsed_url[4] = urlencode(
        {**dict(parse_qsl(parsed_url[4])), **new_query_params})
    return urlunparse(parsed_url)


def scrape_images(markup: Selector):
    """ scrape and parse all images urls """
    images = []
    image_selectors = markup.css("div#altImages >> img")

    if image_selectors:

        for image in image_selectors:
            image_url = image.attributes["src"].replace(",", "")
            if not ".png" in image_url and not "overlay" in image_url:
                pattern = r"\._.+?_\."
                # Replacement text
                replacement = "."
                # Use re.sub to replace the pattern with the replacement text
                new_url = re.sub(pattern, replacement, image_url)
                images.append(new_url)

        if len(images) >= 3:
            return images[:3]
        else:
            logging.info("Less than 3 images")
            return "blacklisted"
    else:
        logging.warning("Can't locate images, requires markup recheck")
        return None


def parse_item_price(item: Selector, parameter: str):
    """ parse prices of current item """

    if parameter == "old":
        old_price_selector = item.css_first(".basisPrice > span.a-offscreen")

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


def check_discount(old: float, sell: float, current_discount: float):
    """ checks if there any discount on this item """
    if old and sell:
        discount = (old - sell) / (old / 100)

    else:
        return None

    if discount >= current_discount and discount < 91.0:
        return True

    else:
        return None


def flexible_price_parser(markup: Selector, parameter, scenario):
    out_of_stock = markup.css_first("div#outOfStock")

    if out_of_stock:
        return "out of stock"

    if scenario == 1 and parameter == "old":
        price_selector_1 = markup.css_first("div#corePrice_desktop >> tr")
        price_selector_2 = markup.css(
            "span.a-text-price >> span.a-offscreen")

        if price_selector_1:
            try:
                price = float(price_selector_1.css_first("td > span > span").text()
                              .replace("₹", "")
                              .replace(",", ""))
                return price

            except ValueError:
                logging.error(
                    "Float convertation error in flexible_price_parser")
                return None

        elif price_selector_2:
            try:
                price = float(price_selector_2[-1].text()
                              .replace("₹", "")
                              .replace(",", ""))
                return price

            except ValueError:
                logging.error(
                    "Float convertation error in flexible_price_parser")
                return None

        else:
            logging.warning("No price sleector")
            return "undetected"


def scrape_title(markup: Selector):
    """ Scrape item's title from page """
    title_selector = markup.css_first("h2 > a > span")

    if title_selector:
        title = title_selector.text(strip=True)

        return title

    else:
        logging.warning("Can't locate url, requires markup recheck")
        return None


def get_current_discount(db_item: dict):
    """ temporary function """
    try:
        current_discount = discounts_info[db_item["category"]]
        return current_discount, db_item["category"]
    except KeyError:

        if "womens-fashion" in db_item["slugs"]:
            try:
                current_discount = discounts_info[f"Women {db_item['category']}"]
                db_item['category'] = f"Women {db_item['category']}"
                return current_discount, db_item["category"]

            except KeyError:
                current_discount = discounts_info[f"Women's {db_item['category']}"]
                db_item['category'] = f"Women's {db_item['category']}"
                return current_discount, db_item["category"]

        elif "boys-clothes" in db_item["slugs"]:
            try:
                current_discount = discounts_info[f"Boy's {db_item['category']}"]
                db_item['category'] = f"Boy's {db_item['category']}"
                return current_discount, db_item["category"]

            except KeyError:

                current_discount = discounts_info[f"Boys {db_item['category']}"]
                db_item['category'] = f"Boys {db_item['category']}"
                return current_discount, db_item["category"]

        elif "mens-fashion" in db_item["slugs"]:
            try:
                current_discount = discounts_info[f"Men's {db_item['category']}"]
                db_item['category'] = f"Men's {db_item['category']}"
                return current_discount, db_item["category"]

            except KeyError:
                current_discount = discounts_info[f"Men {db_item['category']}"]
                db_item['category'] = f"Men {db_item['category']}"
                return current_discount, db_item["category"]

        elif "girl" in db_item["slugs"][1]:
            try:
                current_discount = discounts_info[f"Girl's {db_item['category']}"]
                db_item['category'] = f"Girl's {db_item['category']}"
                return current_discount, db_item["category"]

            except KeyError:
                current_discount = discounts_info[f"Girls {db_item['category']}"]
                db_item['category'] = f"Girls {db_item['category']}"
                return current_discount, db_item["category"]
        else:
            return 20.0, db_item["category"]


def parse_breabcrumbs(markup: Selector):
    """ parse item's breadcrumbs """
    breadcrumbs = markup.css("div#wayfinding-breadcrumbs_feature_div >> span.a-list-item")

    if breadcrumbs:
        breadcrumbs_content = set()

        for element in breadcrumbs:
            breadcrumbs_content.add(element.text(strip=True))

        return breadcrumbs_content

    else:
        logging.warning("Can't locate breadcrumbs, requires markup recheck")
        return None


def parse_page(markup: Selector, db_item: dict):
    """ parse item card """
    # check for discount
    old_price = parse_item_price(markup, "old")
    current_price = parse_item_price(markup, "current")

    # this part of code exists only for cleaning Oral care category from candies
    # ========================================================================
    original_category = db_item["category"]
    
    if original_category == "Oral Care":
        breadcrumbs_content = parse_breabcrumbs(markup)
        
        if breadcrumbs_content:
            if original_category not in breadcrumbs_content:
                return "no discount"

    # =========================================================================    
    current_discount, db_item["category"] = get_current_discount(db_item)

    discount = check_discount(
        old=old_price, sell=current_price, current_discount=current_discount)

    if not discount:
        old_price = flexible_price_parser(markup, "old", 1)

        if old_price == "out of stock":
            return old_price

        elif old_price == "undetected":
            return "undetected"

        discount = check_discount(
            old=old_price, sell=current_price, current_discount=current_discount)

    if not current_price:
        return "undetected"

    if discount:
        asin = db_item["asin"]
        parsed_item = Item(
            source_id=SCRAPER_NAME,
            source_product_id=asin,
            url=db_item["url"],
            title=db_item["title"],
            old_price=old_price,
            current_price=current_price,
            images=scrape_images(markup),
            description=scrape_description(markup),
            original_category=original_category,
            coupon_code=None,
            categories=db_item["slugs"]
        )

        if not parsed_item.current_price or not parsed_item.old_price:
            logging.warning("No price in parsed item")
            return None
        if not parsed_item.title:
            logging.warning("No title in parsed item")
            return None
        if not parsed_item.source_product_id:
            logging.warning("No asin in parsed item")
            return None
        if not parsed_item.url:
            logging.warning("No url in parsed item")
            return None
        return parsed_item

    else:
        logging.warning("No discount on this item")
        print(db_item["url"])
        return "no discount"


async def send_item_to_db(item: dict, attempts=3):
    global counter

    if not item:
        return None
    # item = json.loads(json_item)

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

    saving_url = f"https://parser.discount.one/products?apiKey={API_KEY}"
    response = None
    async with db_limiter:
        async with httpx.AsyncClient() as db_client:

            try:
                response = await db_client.post(saving_url, json=result_json)

            except (httpx.ConnectTimeout,
                    httpx.ReadTimeout,
                    httpx.ConnectError,
                    httpx.ReadError):
                if attempts > 0:
                    attempts -= 1
                    logging.warning("Connection error, retrying request")
                    await send_item_to_db(item, attempts)

                else:
                    logging.warning("Connection error when sending to db")
                    return None

            # also cathcing something unexpected
            except Exception as exc:
                logging.warning("Exception when sending to db : %s", exc)
                return None

        if response:
            # print(response.json())
            if response.status_code == 200:
                counter["filtered_and_sended"] += 1
                print(counter)

            elif response.status_code == 500:
                logging.warning("SERVER ERROR, here is sended data")
                print(result_json)

            elif response.status_code == 400:
                logging.warning("DATA ERROR, here is data")
                print(result_json)
        else:
            logging.error("can't locate response object")


async def scrape_item_page(db_item: Item,
                           proxy: dict,
                           render: str,
                           semaphore: Semaphore,
                           db_manager: AsyncDatabaseManager,
                           attempts=3):

    global counter

    url = db_item["url"]
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
        ['cookie'] = 'session-id=261-2598564-2341616; i18n-prefs=INR; ubid-acbin=261-8681770-6744109; ld=AZINSOANavDesktop_T3; lc-acbin=en_IN; urtk=%7B%22at%22%3A%22Atna%7CEwICIG-vbvypue3mVLKnuCUS1ZNHdFE2uYNenxsktRomEeubA4a5mdtf1tqqWQ98uVcZ5xK7spEVOQ7oEsK28r8SZViVgTOfGPJ33nJd339yJvBQsh738Ob-mwq_lTy1GOQARV14tNgCUfyyIrkPfEgVXHqc8kIr5BiVtIeACa0sfdmAwBL-b3gv31wZnBwAH9Sx5ejDxRIpO2MVO4pWu3F08EC4WnrmluCm5Pg8j5Cac2jEY6hPNCAXhVl51S11Qt2o3rbYoQP3PDrzj0p_EztMZnE97WTtyHUr07ChmxxbafhquWWIzkhRFkgf_uksRTtSH_w%22%2C%22atTtl%22%3A1694008425328%2C%22rt%22%3A%22Atnr%7CEwICIMm79r0u2UKWjTx_x3yEHiGlrbwDjwPwUuyMsO646dHA0J1HgYy1xveSJR4BtAeNmDsyKXlcNItNFKxDqvh-6rQcBSq9bgcdEoWVx9pxxtSajOmZriG4tS9awZEz8ElKnG7_zbYAat3ijFMqkuvI1vFfh5u824qwCr7VTTgfi9eE4NElcnKBIUyRZZ4jgIWN2Ntmwwh81Hwe5USk0y_K1m3lKfmfaWLCZGlc_rlt9Y-0RiAt4o0up8BiZg4iVnaIiaPb7qfCwk70oA9boOhhOt5X67jmYz3YKQjKE86JKwI9J2dy91Z7GUdnStnrCo4z7oEsucYwVfhoZIRStUqvXDul%22%7D; AMCV_A7493BC75245ACD20A490D4D%40AdobeOrg=1585540135%7CMCIDTS%7C19613%7CMCMID%7C14372875062132278590998275670879462650%7CMCAAMLH-1695123340%7C6%7CMCAAMB-1695123340%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1694525740s%7CNONE%7CvVersion%7C4.4.0%7CMCAID%7CNONE; s_vnc365=1726054540457%26vn%3D1; s_nr365=1694518540461-New; session-id-time=2082787201l; session-token=8Wt2rVlBrYq1VV9zGfrF3B0fcCPic2bD5Dt6lT1GlNpmotUxlQSR/RYhbs92MV2MPPHlNDoiSsN8vPlRN3qt8cCtAU2BsfuvV6jpzsQXZbLNe7aPAZhvBax1di6a5NJe9OOXlqevb3PU8ux/tGWCYEkJXv+qZb0+BFwIER3hrP+UL0Qbtl3n5B5JN5AXSD4R1y+EIxtPeaAE3ZMlLV6BJKtBwmiZbteT541RqG2JVoWid2yVHomnpEI/ZdiQRcVMLXsAPJ2Vwr4HNkHYChJ4LECDcgmoJ4sGZpHGOdgON5sXaLQjnD/ZHV09VvlB7VGV7FAKjR8jRvBLz0dNTp2OYl2BxkjRQBOn; csm-hit=tb:KYTJMYNMSC07GSTCXYHJ+s-KYTJMYNMSC07GSTCXYHJ|1695357494588&t:1695357494588&adb:adblk_no; session-token=q4nxK9yqUe2COpgRuN3t25Kmts73+cAsSDPFe8p9Gktdf285cxmdJZZxBPIKFhcIRiXyyncW1HjwDY4t5pCHK1lh2slCNO0CbhOOnRvwtT3xWwR7B/a2LG/oPez/Px+I/FpR9mmZ75WxUbd0ZAmKHbeSY8VxirzVLRuZsYtWD1OG1HxX2lIYU01k0PdVLWWAmrfdgQ81xDymYxviixO8coPLwfBesiTWXlPJXsshqMmJ3VmclynJn10ItJpYkhIB3lgUwdWPmwurvPmRbJiYdYq/x0FhDeILcocPg9GdjKpDFk3HYe5oZjNkIskUWrNgMchqBhw3iM+wwZWHb0x302rQITffgsUb',
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
        splash:wait({random.randint(2, 4)})
        -- Wait for the page to load (add more time if needed)
        -- Return the HTML content
        return response.body
    end
    """

    async with semaphore:
        async with httpx.AsyncClient(headers=headers) as client:
            try:
                response = await client.post(render, json={"url": url, "wait": 15, "timeout": 300, "resource_timeout": 299, "lua_source": lua_proxy}, timeout=300)

                if response.status_code == 504:
                    proxy = next(proxy_gen)
                    logging.warning(
                        "Response status is 504 in validator scrape_item_page")
                    return await scrape_item_page(db_item,
                                                  proxy,
                                                  render,
                                                  semaphore,
                                                  db_manager,
                                                  attempts)
                elif response.status_code == 400:
                    print(response.content)
                elif response.status_code != 200:
                    proxy = next(proxy_gen)
                    logging.warning(
                        "Status response in not 200 in vaiidator scrape_item_page")
                    return await scrape_item_page(db_item,
                                                  proxy,
                                                  render,
                                                  semaphore,
                                                  db_manager,
                                                  attempts)
                elif response.status_code == 404:
                    logging.warning("404 in validator scrape_item_page")
                    print(url)
                    await db_manager.to_blacklist(db_item["asin"])
                    logging.info("BLACKLISTED")
                    return None

            except ValueError:
                logging.warning("Value error when page requesting")
                return None

            except anyio.EndOfStream:
                if attempts > 0:
                    attempts -= 1
                    logging.warning("End of stream error, retrying request in image validation")
                    return await scrape_item_page(db_item,
                                                  proxy,
                                                  render,
                                                  semaphore,
                                                  db_manager,
                                                  attempts)

                else:
                    logging.warning("Problems with connection when image checking")
                    return None

            except (httpx.ConnectTimeout,
                    httpx.ReadTimeout,
                    httpx.ConnectError,
                    httpx.ReadError,
                    httpx.RemoteProtocolError):

                if attempts > 0:
                    attempts -= 1
                    logging.warning(
                        f"Connection error, retrying request in scrape_item_page {render}")
                    proxy = next(proxy_gen)
                    return await scrape_item_page(db_item,
                                                  proxy,
                                                  render,
                                                  semaphore,
                                                  db_manager,
                                                  attempts)

                else:
                    logging.warning(
                        "Connection error when page requesting")
                    return None

        markup = HTMLParser(response.text)
        detection_one = markup.css_first("h1")
        detection_two = markup.css_first("p")

        if detection_one:
            if "Proxy requires authentication" in detection_one.text():
                if attempts > 0:
                    attempts -= 1
                    logging.warning("proxy authentication")
                    proxy = next(proxy_gen)

                    return await scrape_item_page(db_item,
                                                  proxy,
                                                  render,
                                                  semaphore,
                                                  db_manager,
                                                  attempts)

                else:
                    logging.warning("HARD DETECTION")
                    await db_manager.to_blacklist(db_item["asin"])
                    logging.info("BLACKLISTED")
                    return None

        elif detection_two:
            if "not a robot" in detection_two.text():
                if attempts > 0:
                    attempts -= 1
                    asyncio.sleep(2)
                    logging.warning("Robot check, not a robot")
                    proxy = next(proxy_gen)

                    return await scrape_item_page(db_item,
                                                  proxy,
                                                  render,
                                                  semaphore,
                                                  db_manager,
                                                  attempts)
                else:
                    logging.error("HARD DETECTION")
                    await db_manager.to_blacklist(db_item["asin"])
                    logging.info("BLACKLISTED")
                    return None
            else:
                return None

        elif not response.text:
            logging.warning("Empty response in validator scrape_item_page")
            if attempts > 0:
                attempts -= 1
                asyncio.sleep(2)
                logging.warning("empty response, retrying...")
                proxy = next(proxy_gen)

                return await scrape_item_page(db_item,
                                              proxy,
                                              render,
                                              semaphore,
                                              db_manager,
                                              attempts)
            else:
                logging.error("ALWAYS EMPTY")
                print(url)
                await db_manager.to_blacklist(db_item["asin"])
                logging.info("BLACKLISTED")
                return None
        else:
            logging.warning(
                "unexpected behaviour in validator scrape_item_page")
            await db_manager.to_blacklist(db_item["asin"])
            logging.info("BLACKLISTED")
            return None

    # create an item object
    item = parse_page(markup, db_item)

    if not item:
        logging.warning("Item is none")
        print(db_item["url"])
        return None

    elif item == "out of stock":
        await db_manager.to_blacklist(db_item["asin"])
        logging.info("BLACKLISTED")
        return None

    elif item == "no discount":
        await db_manager.to_blacklist(db_item["asin"])
        logging.info("BLACKLISTED")
        return None

    elif item == "undetected":
        logging.warning("UNDETECTED price")
        await db_manager.to_blacklist(db_item["asin"])
        logging.info("BLACKLISTED")
        return None

    item.description = scrape_description(markup)

    if not item.description:
        item.description = item.title

    # parse img url from item's page
    scraped_images = scrape_images(markup)

    # now validate images
    tasks = []
    if scraped_images and scraped_images != "blacklisted":
        for image in scraped_images:
            tasks.append(validate_image(image))

        checked_images = await asyncio.gather(*tasks)
        valid_images = list(
            filter(lambda x: x is not False and x is not None, checked_images))

        if valid_images and len(valid_images) == 3:
            item.images = valid_images
            serialized_item = json.dumps(dataclasses.asdict(item))
            celery_db_update.apply_async(args=[serialized_item])
            print("Sended to celery")
            counter["filtered_and_sended"] += 1
            print(counter)
            query = await db_manager.change_scraped_status(item.source_product_id, True, blacklisted=0)
            db_manager.transaction_group.append(query)

        else:
            logging.warning("Fail to validate images")
            print(item.images)
            return None

    elif scraped_images == "blacklisted":
        print(item.url)
        await db_manager.to_blacklist(db_item["asin"])
        logging.info("BLACKLISTED")
        return None

    else:
        logging.warning(f"No images {item.url}")
        return None


async def render_page(url, proxy):
    chrome_options = webdriver.ChromeOptions()
    PROXY = proxy
    print(PROXY)

    chrome_options.set_capability('proxy', {"httpProxy": PROXY,
                                            "proxyType": "manual"})
    
    driver = webdriver.Remote(command_executor="http://localhost:4444",
                              options=chrome_options)

    try:
        
        await driver.get(url)
        # wait for page load
        #driver.implicitly_wait(10)
        result = driver.page_source


    except selenium.common.exceptions.InvalidSessionIdException:
        print("Exception")
        result = None
    
    finally:
        driver.quit()

    return result


async def parse_and_validate(item,
                             limiter,
                             attempts=7):
    """ function for parsing and validating item """
    # first check if item in db
    
    item_in_db = check_db(item)

    if item_in_db:
        # don't validate image
        # and don't parse description and category
        logging.info("Item in DB")

        item.description = get_description_db(item)
        item.images = get_images_db(item)

        return item

    else:
        url = item.url
        proxy = next(aiohttp_proxy)

        try: 
            response = await fetch_response(url, proxy, limiter)
            #response = await render_page(url, proxy)

        except aiohttp.client_exceptions.ClientHttpProxyError:
            if attempts > 0:
                attempts -= 1
                logging.warning("Proxy error when getting page urls")
                return await parse_and_validate(item,
                                                limiter,
                                                attempts)
            
            else:
                logging.warning("Proxy error after 20 attempts when getting page urls")
                return None  
        
        except aiohttp.client_exceptions.ServerDisconnectedError:
            if attempts > 0:
                attempts -= 1
                logging.warning("Server disconnected error when getting page in validator")
                return await parse_and_validate(item,
                                                limiter,
                                                attempts)
            
            else:
                logging.warning("Server disconnected error after 10 attempts when getting page in validator")
                return None

        except asyncio.TimeoutError:
            if attempts > 0:
                attempts -= 1
                logging.warning("Timeout error when getting page urls")
                return await parse_and_validate(item,
                                                limiter,
                                                attempts)
            
            else:
                logging.warning("Timeout error after 10 attempts when getting page urls")
                return None

        if response == "not 200":
            if attempts > 0:
                attempts -= 1
                logging.warning("Not 200")
                return await parse_and_validate(item,
                                                limiter,
                                                attempts)
            
            else:
                logging.error("Not 200")
                return None  

        if not response:
            
            if attempts > 0:
                attempts -= 1
                logging.warning("Empty response")
                return await parse_and_validate(item,
                                                limiter,
                                                attempts)
            
            else:
                logging.error("Empty response")
                return None  
        
        if "Enter the characters you see below" in response:
            if attempts > 0:
                attempts -= 1
                logging.warning("Robot check")
                return await parse_and_validate(item,
                                                limiter,
                                                attempts)
            
            else:
                logging.error("Robot check")
                return None
        
        
        markup = HTMLParser(response)
        # item should become a dict
        
        result = await standart_itempage_parser(markup, item)

        if not result:
            if attempts > 0:
                attempts -= 1
                logging.warning("No images")
                return await parse_and_validate(item,
                                                limiter,
                                                attempts)
            
            else:
                logging.error("No images")
                return None
        
        print(result)
        return result


if __name__ == "__main__":
# uncomment discounts info from imports
    markup = None

    with open("amazon.html", "r") as file:
        markup = file.read()

    selectolax = HTMLParser(markup)

    parse_page(selectolax, dict())
