# -*- coding: utf-8 -*-
import gc
import time
from urllib.parse import urljoin

import boto3
import logging
import urllib.request
from scrapy import signals
from scrapy.http import Response

from pydispatch import dispatcher
from bhfutils.crawler.spiders.redis_spider import RedisSpider
from bhfutils.crawler.items import BrandProductDetailsResponseItem

BASIC_URL = "https://support.apple.com/"


class AppleTemplateDetailsSpider(RedisSpider):
    idle_state_counter = 0
    spider_state = 'initial'

    name = 'AppleTemplateDetails'
    allowed_domains = ['apple.com']

    def __init__(self, *args, **kwargs):
        gc.set_threshold(100, 3, 3)

        logging.getLogger('filelock').setLevel(logging.WARNING)
        logging.getLogger('asyncio').setLevel(logging.WARNING)
        logging.getLogger('kazoo').setLevel(logging.WARNING)
        logging.getLogger('kafka').setLevel(logging.WARNING)
        logging.getLogger('boto3').setLevel(logging.WARNING)
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('botocore').setLevel(logging.WARNING)
        logging.getLogger('scrapy.core.scraper').setLevel(logging.WARNING)

        dynamo_db = boto3.resource('dynamodb')
        self.spiders_table = dynamo_db.Table('BhfSpiders')
        try:
            self.public_ip = urllib.request.urlopen("http://169.254.169.254/latest/meta-data/public-ipv4",
                                                    timeout=5).read().decode('utf-8')
            self.private_ip = urllib.request.urlopen("http://169.254.169.254/latest/meta-data/local-ipv4",
                                                     timeout=5).read().decode('utf-8')
        except:
            self.public_ip = "34.206.1.137"
            self.private_ip = "127.0.0.1"
        dispatcher.connect(self.spider_opened, signals.spider_opened)
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        dispatcher.connect(self.log_idle, signals.spider_idle)
        dispatcher.connect(self.log_working, signals.request_received)
        super(AppleTemplateDetailsSpider, self).__init__(*args, **kwargs)

    def sync_spider_state(self, state):
        pass
        self.spiders_table.put_item(
            Item={
                'public_ip': self.public_ip,
                'private_ip': self.private_ip,
                'name': 'AppleTemplateDetails',
                'status': state,
                'status_date': int(round(time.time() * 1000)),
            }
        )

    def spider_opened(self):
        self.sync_spider_state('opened')

    def log_working(self):
        self.idle_state_counter = 0
        if self.spider_state != 'working':
            self.spider_state = 'working'
            self.sync_spider_state('working')

    def log_idle(self):
        if self.spider_state != 'idle':
            self.idle_state_counter = self.idle_state_counter + 1
            if self.idle_state_counter > 3:
                self.spider_state = 'idle'
                self.idle_state_counter = 0
                self.sync_spider_state('idle')

    def spider_closed(self):
        self.sync_spider_state('closed')

    def parse_name(self, response: Response):
        """ parse name from item's card """
        name = response.css("h1#main-title ::text").get()

        if name:
            return name

        else:
            logging.warning("Can't locate product name, requires recheck")
            return None

    def parse_image(self, response: Response):
        """ get product's image """
        if response.css("img").get() is not None:
            images_data = response.css("img")
            images = [urljoin(BASIC_URL, image.css("::attr(src)").get()) for image in images_data]

            return images
        else:
            logging.warning("Can't locate image url, requires recheck")
            return None

    def parse_name(self, response: Response):
        """ parse name from item's card """
        name = response.css("h1#main-title ::text").get()

        if name:
            return name

        else:
            logging.warning("Can't locate product name, requires recheck")
            return None

    def parse_info(self, response: Response, parameter):
        """ parse variations """
        if parameter == "variations":
            all_tags = response.css("div.SPECIFICATION > *")
        elif parameter == "specs":
            all_tags = response.css("div.SPECIFICATION > *")[2:]
        else:
            logging.error("Wrong parameter passed!")

        result = dict()
        categories = []
        subcategories = []
        for tag in all_tags:
            if tag.root.tag == "h3":
                categories.append(subcategories)
                subcategories = []
                subcategories.append(tag.css("::text").get())

            elif tag.root.tag != "ul":
                continue
            else:
                subcategories.append(tag)

        for category in categories[2:]:
            key = category[0]
            content = category[1:]
            for ul in content:
                result[key] = []
                for li in ul.css("li"):
                    if li.css("b"):
                        name = li.css("::text").getall()[0]
                        text = ", ".join(x.strip()
                                         for x in li.css("::text").getall()[1:])

                        result[key].append(f"{name} : {text}")
                    else:
                        result[key].append(li.css("::text").get())

        if parameter == "variations":
            for key, value in result.items():
                if "finish" in key.lower():
                    return {key: value}

        elif parameter == "specs":
            return result

        else:
            logging.error("Impossible scenario, recheck code!")

    def parse(self, response, **kwargs):
        try:
            item = BrandProductDetailsResponseItem()

            item["appid"] = response.meta['appid']
            item["crawlid"] = response.meta['crawlid']
            item["url"] = response.request.url
            item["responseUrl"] = response.url
            item["statusCode"] = response.status
            item["brandId"] = 1
            item["groupId"] = response.meta["attrs"]["groupId"]
            item["productUrl"] = response.request.url
            item["imageUrls"] = self.parse_image(response)
            item["variations"] = self.parse_info(response, "variations")
            item["productParameters"] = self.parse_info(response, "specs")
            yield item

        except Exception as e:
            logging.exception("Apple error when parse product details")
            raise