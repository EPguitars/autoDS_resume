# -*- coding: utf-8 -*-
import gc
import time
import boto3
import logging
import json
from urllib.parse import urljoin
from datetime import datetime

import urllib.request
from scrapy.http import Response
from pydispatch import dispatcher
from scrapy import signals, Request
from bhfutils.crawler.spiders.redis_spider import RedisSpider
from bhfutils.crawler.items import BrandProductDetailsResponseItem

basic_url = 'https://consumer.huawei.com'

class HuaweiTemplatesSpider(RedisSpider):
    spider_state = 'initial'

    name = 'HuaweiTemplates'
    allowed_domains = ['huawei.com']

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
        self.spider_messages_table = dynamo_db.Table('BhfSpiderMessages')
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
        dispatcher.connect(self.log_error, signals.spider_error)
        dispatcher.connect(self.log_error, signals.item_error)
        dispatcher.connect(self.log_idle, signals.spider_idle)
        dispatcher.connect(self.log_working, signals.request_received)
        super(HuaweiTemplatesSpider, self).__init__(*args, **kwargs)

    def log_spider_message(self, message):
        self.spider_messages_table.put_item(
            Item={
                'public_ip': self.public_ip,
                'private_ip': self.private_ip,
                'messageDate': int(round(time.time() * 1000)),
                'name': 'HuaweiTemplates',
                'message': message,
            }
        )

    def sync_spider_state(self, state):
        self.spiders_table.put_item(
            Item={
                'public_ip': self.public_ip,
                'private_ip': self.private_ip,
                'name': 'HuaweiTemplates',
                'status': state,
                'status_date': int(round(time.time() * 1000)),
            }
        )

    def spider_opened(self):
        self.sync_spider_state('opened')

    def log_working(self):
        if self.spider_state != 'working':
            self.spider_state = 'working'
            self.sync_spider_state('working')

    def log_idle(self):
        if self.spider_state != 'idle':
            self.spider_state = 'idle'
            self.sync_spider_state('idle')

    def spider_closed(self):
        self.sync_spider_state('closed')

    def log_error(self, failure):
        self.log_spider_message(failure.getErrorMessage())

    def parse_image(self, response):
        """ Scrape product image """
        image_path = response.css("img.img-lazy-load ::attr('data-src')").get()

        if image_path:
            image = urljoin(basic_url, image_path)
            return [image]

        else:
            logging.warning("No image found for product, requires recheck")
            return None

    def parse_url(self, response):
        """ Scrape product url """
        path = response.css("a.left-img-wrapper ::attr('href')").get()

        if path:
            return urljoin(basic_url, path)

        else:
            logging.warning("No url found for product, requires recheck")
            return None

    def parse_code(self, response):
        """ Scrape product code """
        code = response.css(
            "div.more-product-item ::attr('data-ecproductid')").get()

        if code:
            return code

        else:
            logging.warning("No code found for product, requires recheck")
            return None

    def parse_name(self, response):
        """ Scrape product name """
        name = response.css("div.product-item_name ::text").get()

        if name:
            return name

        else:
            logging.warning("No name found for product, requires recheck")
            return None

    def parse_minprice(self, item: dict):
        """ Scrape product minprice """
        price = item.get("minUnitPrice")

        if price:
            return price

        else:
            logging.warning("No price found for product, requires recheck")
            return None

    def parse_variations(self, item: dict):
        """ Scrape product variations """
        variations_selector = item.get("minPriceByColors")
        variations = []

        if variations_selector:

            for item in variations_selector:
                variations.append(item.get("color"))

            return variations

        else:
            logging.warning(
                "No variations found for product, requires recheck")
            return None

    def parse_additional_data(self, response: Response):
        item = BrandProductDetailsResponseItem()
        json_data = json.loads(response.body)
        
        try:
            if json_data["data"]["minPriceAndInvList"]:
                target_block = json_data["data"]["minPriceAndInvList"][0]

                item["appid"] = response.meta['appid']
                item["crawlid"] = response.meta['crawlid']
                item["url"] = response.request.url
                item["responseUrl"] = response.url
                item["statusCode"] = response.status
                item["brandId"] = 7
                item["groupId"] = response.meta["attrs"]["groupId"]
                item["productUrl"] = response.meta["attrs"]["productUrl"]
                item["minPrice"] = self.parse_minprice(target_block)
                item["variations"] = self.parse_variations(target_block)

                yield item

        except KeyError:
            logging.info("No additional data for current product")
            yield item  

    def parse(self, response, **kwargs):
        try:
            cards = response.css("div.product-card")
            if cards:
                for card in cards:
                    productUrl = self.parse_url(card)
                    item = BrandProductDetailsResponseItem()

                    item["appid"] = response.meta['appid']
                    item["crawlid"] = response.meta['crawlid']
                    item["url"] = response.request.url
                    item["responseUrl"] = response.url
                    item["statusCode"] = response.status
                    item["brandId"] = 7
                    item["groupId"] = response.meta["attrs"]["groupId"]
                    item["productUrl"] = productUrl
                    item["code"] = self.parse_code(card)
                    item["name"] = self.parse_name(card)
                    item["imageUrls"] = self.parse_image(card)

                    yield item

                    timestamp_for_request = str(
                        round(datetime.now().timestamp(), 3)).replace(".", "")
                    api_url = f"https://itrinity-ru.c.huawei.com/eCommerce/queryMinPriceAndInv?productIds={item.get('code')}&siteCode=RU&loginFrom=1&_={timestamp_for_request}"

                    response.meta["attrs"]["productUrl"] = productUrl
                    yield Request(api_url, callback=self.parse_additional_data,
                                  dont_filter=True)
            else:
                cards = response.css("div.series-item-card")
                for card in cards:
                    item = BrandProductDetailsResponseItem()

                    item["appid"] = response.meta['appid']
                    item["crawlid"] = response.meta['crawlid']
                    item["url"] = response.request.url
                    item["responseUrl"] = response.url
                    item["statusCode"] = response.status
                    item["brandId"] = 7
                    item["groupId"] = response.meta["attrs"]["groupId"]
                    item["productUrl"] = urljoin(basic_url, card.css("a ::attr('href')").get())
                    item["code"] = card.css("div > div ::attr('data-ecproductid')").get()
                    item["name"] = card.css("a ::text").get().strip()
                    item["imageUrls"] = [urljoin(basic_url, card.css("img ::attr('data-src')").get())]

                    yield item

        except Exception as e:
            logging.exception("Huawei error when parse product page")
            raise
