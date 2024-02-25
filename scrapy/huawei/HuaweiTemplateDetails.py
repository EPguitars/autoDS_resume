# -*- coding: utf-8 -*-
import gc
import time
import json

import boto3
import logging
import urllib.request
from scrapy import signals
from scrapy.http import Response

from pydispatch import dispatcher
from bhfutils.crawler.spiders.redis_spider import RedisSpider
from bhfutils.crawler.items import BrandProductDetailsResponseItem


class HuaweiTemplateDetailsSpider(RedisSpider):
    idle_state_counter = 0
    spider_state = 'initial'

    name = 'HuaweiTemplateDetails'
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
        super(HuaweiTemplateDetailsSpider, self).__init__(*args, **kwargs)

    def sync_spider_state(self, state):
        pass
        self.spiders_table.put_item(
            Item={
                'public_ip': self.public_ip,
                'private_ip': self.private_ip,
                'name': 'HuaweiTemplateDetails',
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
    
    def parse_description(self, response: Response):
        """ Parse product description from html """
        description = response.css(
            "meta[name='description'] ::attr('content')").get()

        if description:
            return description

        else:
            logging.info("No description found for product")
            return None

    def parse_parameters(self, response: Response):
        """ Parse product parameters from html """
        block = response.css("ul.large-accordion__list")
        specs = dict()

        if block:
            elements = block.css("li.large-accordion__item")

            for element in elements:
                title = element.css("span.large-accordion__title ::text").get()
                content = element.css("div.large-accordion__inner")

                if content:
                    specs[title] = dict()

                    for line in content:
                        subtitle = line.css(
                            "div.large-accordion-subtitle ::text").get()

                        if not subtitle:
                            main_params = [x.css("::text").get() for x in line.css(
                                "p") if not x.css("p ::attr('class')").get()]
                            if main_params:
                                specs[title]["main"] = main_params

                        else:
                            params = [x.css("::text").get() for x in line.css(
                                "p") if not x.css("p ::attr('class')").get()]

                            if params:
                                specs[title][subtitle] = params
                
                if isinstance(specs.get(title), dict) and not specs.get(title):
                    del specs[title]
        else:    
            logging.info("No parameters found for product")
            return None             
        
        return specs
    
    def parse(self, response, **kwargs):
        try:
            item = BrandProductDetailsResponseItem()

            item["appid"] = response.meta['appid']
            item["crawlid"] = response.meta['crawlid']
            item["url"] = response.request.url
            item["responseUrl"] = response.url
            item["statusCode"] = response.status
            item["brandId"] = 7
            item["groupId"] = response.meta["attrs"]["groupId"]
            item["productUrl"] = response.meta["attrs"]["productUrl"]
            item["description"] = self.parse_description(response)
            item["productParameters"] = self.parse_parameters(response)

            yield item

        except Exception as e:
            logging.exception("Huawei error when parse product details")
            raise