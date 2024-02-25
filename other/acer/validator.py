import re
import json
import asyncio
import logging
from urllib.parse import (urlparse,
                          parse_qsl,
                          urlencode,
                          urlunparse,)

import httpx
from selectolax.parser import HTMLParser, Selector
from rich import print

from db_operations import (check_db,
                           get_description_db,
                           get_images_db)
from image_validation import validate_image
from headers import headers
from models import Item
from main_tools.proxy import proxy_gen


def scrape_description(markup: Selector):
    """ scrape and parse item's category """
    description_selector = markup.css_first("div[itemprop='description']")

    if description_selector:
        description_lines = description_selector.css("li")
        description = "\n".join([line.text(deep=True, strip=True) for line in description_lines])
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
    # image information can be found in script tag
    script_tags = markup.css("script[type='text/x-magento-init']")
    
    # now detect concrete script tag
    pattern = re.compile(r'"data":\s*(\[.*?\]|"[^"]+"|true|false|null|\d+)')

    for script in script_tags:
        if data := pattern.search(script.text()):
            json_images = json.loads(data.group(1).replace('"data": ', ""))
            
    
    if json_images:
        for block in json_images:
            image_link = block.get("img")
            images.append(image_link)
            
            if len(images) == 3:
                break

        return images


    else:
        logging.warning("Can't locate images, requires markup recheck")
        return None
    

async def parse_and_validate(item: Item,
                             semaphore,
                             attempts=3):
    """
    this function validates image links
    and scrapes all information that 
    don't appears on search page
    """

    
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
        async with semaphore:
            url = item.url
            proxy = next(proxy_gen)
            async with httpx.AsyncClient(headers=headers, proxies=proxy) as client:
                try:
                    response = await client.get(url, follow_redirects=True, timeout=60)

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
                        return await parse_and_validate(item,
                                                        semaphore,
                                                        attempts)

                    else:
                        logging.warning(
                            "Connection error when page requesting")
                        return None
                
                except httpx.ProxyError:
                    if attempts > 0:
                        attempts -= 1
                        logging.warning("Proxy error when getting page urls")
                        return await parse_and_validate(item, semaphore, attempts)

                    else:
                        logging.warning("Proxy error after 10 attempts when getting page urls")
                        return None
        
            markup = HTMLParser(response.text)

            item.description = scrape_description(markup)
            # parse img url from item's page
            scraped_images = scrape_images(markup)

            # now validate images
            tasks = []

            for image in scraped_images:
                tasks.append(validate_image(image))

            checked_images = await asyncio.gather(*tasks)
            valid_images = list(
                filter(lambda x: x is not False and x is not None, checked_images))

            if valid_images and len(valid_images) == 3:
                item.images = valid_images

            else:
                return None

            return item
