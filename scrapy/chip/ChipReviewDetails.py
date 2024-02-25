# -*- coding: utf-8 -*-
import gc
import re
import time
from typing import Union

import boto3
import logging
import datefinder
import pandas as pd
import urllib.request
from scrapy import signals, Request
from scrapy.http import Response

from pydispatch import dispatcher
from bhfutils.crawler.spiders.redis_spider import RedisSpider
from bhfutils.crawler.items import ReviewDetailsResponseItem


class ChipReviewDetailsSpider(RedisSpider):
    idle_state_counter = 0
    spider_state = "initial"

    name = "ChipReviewDetails"
    allowed_domains = ["chip.de"]

    def __init__(self, *args, **kwargs):
        gc.set_threshold(100, 3, 3)

        logging.getLogger("filelock").setLevel(logging.WARNING)
        logging.getLogger("asyncio").setLevel(logging.WARNING)
        logging.getLogger("kazoo").setLevel(logging.WARNING)
        logging.getLogger("kafka").setLevel(logging.WARNING)
        logging.getLogger("boto3").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("botocore").setLevel(logging.WARNING)
        logging.getLogger("scrapy.core.scraper").setLevel(logging.WARNING)

        dynamo_db = boto3.resource("dynamodb")
        self.spiders_table = dynamo_db.Table("BhfSpiders")
        try:
            self.public_ip = (
                urllib.request.urlopen(
                    "http://169.254.169.254/latest/meta-data/public-ipv4", timeout=5
                )
                    .read()
                    .decode("utf-8")
            )
            self.private_ip = (
                urllib.request.urlopen(
                    "http://169.254.169.254/latest/meta-data/local-ipv4", timeout=5
                )
                    .read()
                    .decode("utf-8")
            )
        except:
            self.public_ip = "127.0.0.1"
            self.private_ip = "127.0.0.1"
        dispatcher.connect(self.spider_opened, signals.spider_opened)
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        dispatcher.connect(self.log_idle, signals.spider_idle)
        dispatcher.connect(self.log_working, signals.request_received)
        super(ChipReviewDetailsSpider, self).__init__(*args, **kwargs)

    def sync_spider_state(self, state):
        self.spiders_table.put_item(
            Item={
                'public_ip': self.public_ip,
                'private_ip': self.private_ip,
                'name': 'ChipReviewDetails',
                'status': state,
                'status_date': int(round(time.time() * 1000)),
            }
        )

    def spider_opened(self):
        self.sync_spider_state("opened")

    def log_working(self):
        self.idle_state_counter = 0
        if self.spider_state != "working":
            self.spider_state = "working"
            self.sync_spider_state("working")

    def log_idle(self):
        if self.spider_state != "idle":
            self.idle_state_counter = self.idle_state_counter + 1
            if self.idle_state_counter > 3:
                self.spider_state = "idle"
                self.idle_state_counter = 0
                self.sync_spider_state("idle")

    def spider_closed(self):
        self.sync_spider_state("closed")

    def parse_date(self, response: Response) -> Union[str, None]:
        """parse date"""
        date_location = response.css("time ::text").get()

        if date_location:
            date = next(datefinder.find_dates(date_location.strip()))
            return date.strftime("%Y-%m-%d %H:%M:%S")

        else:
            logging.warning("Website changed structure, need refactor")
            return None

    def parse_author(self, response: Response) -> Union[str, None]:
        """parse name of the article author"""
        author = response.css("span.author.meta.caps > span > a ::text").get()

        if author:
            return author

        else:
            logging.warning("Website changed structure, need refactor")
            return author

    def parse_title(self, response: Response) -> Union[str, None]:
        """parse title of the article"""
        title = response.css("header > h1 ::text").get()

        if title:
            return title
        else:
            logging.warning("Website changed structure, need refactor")
            return title

    def parse_keywords(self, response: Response) -> list:
        """parse keywords of the article"""
        keywords_scraped = response.css("div.Tags ::text").getall()
        keywords = list(map(lambda x: x.strip(), keywords_scraped))

        if keywords:
            return keywords
        else:
            logging.warning("Website changed structure, need refactor")
            return keywords

    def parse_category(self, response: Response) -> Union[str, None]:
        """parse categery of the item in article"""
        category_location = response.css(
            "ol.BreadcrumbList " + "> li " + "> a ::attr(title)"
        ).getall()

        if category_location and len(category_location) > 1:
            category = category_location[-2]

            return category

        else:
            logging.warning("Website changed structure, need refactor")
            return None

    def parse_description(self, response: Response) -> Union[str, None]:
        """parse description of the article"""
        description = response.css("div.mt-lg > p.mt-lg ::text").get()

        if description:
            return description

        else:
            logging.warning("Website changed structure, need refactor")
            return description

    def parse_pros_and_cons(self, response: Response) -> dict:
        """parse advantages and disadvantages block"""
        pros_cons = dict()
        pros_cons["pros"] = None
        pros_cons["cons"] = None
        block = response.css("div.TestReport__ProsCons")

        if block:
            pros_cons["pros"] = block.css(
                "dl.List.List--Definition.is-pro " + "> dd ::text"
            ).getall()
            pros_cons["cons"] = block.css(
                "dl.List.List--Definition.is-con " + "> dd ::text"
            ).getall()
            return pros_cons

        else:
            logging.warning("Website changed structure, need refactor")
            return pros_cons

    def parse_tech_parameters(self, response: Response) -> dict:
        """parse technical parameters from hiden table"""
        script_with_table = response.css("script#tpl-technicalData ::text").get()
        technical_data = dict()

        if script_with_table:
            table = pd.read_html(script_with_table)
            if table:
                technical_data = table[0].set_index(0)[1].to_dict()

                return technical_data

            else:
                logging.warning("Website changed structure, need refactor")
                return technical_data

        else:
            logging.warning("Website changed structure, need refactor")
            return technical_data

    def parse_verdict_table(self, verdicts: dict, comparsion_table: str):
        """detailed parsing of comparsion table"""
        table = pd.read_html(comparsion_table)
        parsed_table = table[0].set_index(0)[1].to_dict()
        pattern = r"\(\d+,\d+\)"

        for key, value in parsed_table.copy().items():
            if not isinstance(key, str) or not re.search(pattern, value):
                del parsed_table[key]

        for key, value in parsed_table.items():
            float_value = float(re.search(r"\d+(,|.)\d+", value)[0].replace(",", "."))

            if key == "Gesamtwertung":
                verdicts["verdict"] = float_value

            else:
                verdicts["baseParameters"][key] = float_value

    def parse_verdicts(self, response: Response):
        """
        parse all marks for product include main verdict
        this info exists in comparsion table
        """
        verdicts = dict()
        verdicts["verdict"] = None
        verdicts["baseParameters"] = dict()
        # first let's locate comparsion table
        comparsion_table = response.css("table").get()

        if comparsion_table:
            self.parse_verdict_table(verdicts, comparsion_table)

        else:
            logging.warning("Website changed structure, need refactor")

        return verdicts

    def parse(self, response, **kwargs):
        try:
            item = ReviewDetailsResponseItem()

            item["appid"] = response.meta['appid']
            item["crawlid"] = response.meta['crawlid']
            item["url"] = response.request.url
            item["responseUrl"] = response.url
            item["statusCode"] = response.status
            item["reviewUrl"] = response.request.url
            item["createDate"] = self.parse_date(response)
            item["author"] = self.parse_author(response)
            item["name"] = self.parse_title(response)
            item["keywords"] = self.parse_keywords(response)
            item["category"] = self.parse_category(response)
            item["technoBlogId"] = 4
            item["description"] = self.parse_description(response)
            pros_and_cons = self.parse_pros_and_cons(response)
            item["pros"] = pros_and_cons["pros"]
            item["cons"] = pros_and_cons["cons"]
            # verdicts will contain info about verdict and baseParameters
            verdicts = self.parse_verdicts(response)
            item["baseParameters"] = verdicts["baseParameters"]
            item["verdict"] = verdicts["verdict"]
            item["productName"] = item["name"].replace("im Test", "")
            item["productParameters"] = self.parse_tech_parameters(response)

            yield item

        except Exception as e:
            logging.exception("Chip error when parse review details")
            raise