# -*- coding: utf-8 -*-
import gc
import time
import boto3
import logging
import json
import urllib.request
from urllib.parse import (urlparse,
                          parse_qsl,
                          urlencode,
                          urlunparse,
                          urljoin,
                          parse_qs)

from pydispatch import dispatcher
from scrapy import signals, Request
from bhfutils.crawler.items import BrandProductResponseItem
from bhfutils.crawler.spiders.redis_spider import RedisSpider

BASIC_URL = "https://support.apple.com/"


class AppleTemplatesSpider(RedisSpider):
    spider_state = 'initial'

    name = 'AppleTemplates'
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
        super(AppleTemplatesSpider, self).__init__(*args, **kwargs)

    def log_spider_message(self, message):
        self.spider_messages_table.put_item(
            Item={
                'public_ip': self.public_ip,
                'private_ip': self.private_ip,
                'messageDate': int(round(time.time() * 1000)),
                'name': 'AppleTemplates',
                'message': message,
            }
        )

    def sync_spider_state(self, state):
        self.spiders_table.put_item(
            Item={
                'public_ip': self.public_ip,
                'private_ip': self.private_ip,
                'name': 'AppleTemplates',
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

    def parse_name(self, card: dict):
        """ parse name from item's card """
        name = card["prodName"]

        if name:
            return name

        else:
            logging.warning("Can't locate product url, requires recheck")
            return None

    def parse_product_url(self, card: dict):
        """ parse url from json """

        path = card["url"]
        if path:
            url = urljoin(BASIC_URL, path)
            return url.replace('viewlocale', 'locale')

        else:
            logging.warning("Can't locate product url, requires recheck")
            return None

    def parse_code(self, card: dict):
        """ parse item's code """
        code = card["id"]

        if code:
            return code

        else:
            logging.warning("Can't locate product url, requires recheck")
            return None

    def add_query_to_url(self, existing_url, new_query_params):
        """ recompose url with new parameters """
        parsed_url = list(urlparse(existing_url))
        parsed_url[4] = urlencode(
            {**dict(parse_qsl(parsed_url[4])), **new_query_params})
        return urlunparse(parsed_url)

    def parse(self, response, **kwargs):
        try:
            response_string = response.text \
                .replace("ACSpecSearch.showResults(", "") \
                .replace(");", "").strip()

            json_data = json.loads(response_string)

            cards = json_data["specs"]

            if cards:

                for card in cards:
                    item = BrandProductResponseItem()
                    item["appid"] = response.meta['appid']
                    item["crawlid"] = response.meta['crawlid']
                    item["url"] = response.request.url
                    item["responseUrl"] = response.url
                    item["statusCode"] = response.status
                    item["brandId"] = 1
                    item["groupId"] = response.meta["attrs"]["groupId"]
                    item["productUrl"] = self.parse_product_url(card)
                    item["code"] = self.parse_code(card)
                    item["name"] = self.parse_name(card)
                    yield item

                # now get next page url
                offset = int(
                    parse_qs(urlparse(response.url).query)["offset"][0])
                next_page_url = self.add_query_to_url(
                    response.url, {"offset": offset + 1})
                # request next page
                yield Request(next_page_url,
                              callback=self.parse,
                              dont_filter=True)

            else:
                logging.warning("No cards on page, scraping is done!")

        except Exception as e:
            logging.exception("Apple error when parse product page")
            raise