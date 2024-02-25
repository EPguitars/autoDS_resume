# -*- coding: utf-8 -*-
import gc
import time
import urllib.request
from typing import Union

import boto3
import logging
import datefinder
from scrapy import signals, Request
from scrapy.http import Response

from pydispatch import dispatcher
from bhfutils.crawler.spiders.redis_spider import RedisSpider
from bhfutils.crawler.items import ReviewDetailsResponseItem


class GadgetReviewReviewDetailsSpider(RedisSpider):
    idle_state_counter = 0
    spider_state = "initial"

    name = "GadgetReviewReviewDetails"
    allowed_domains = ["gadgetreview.com"]

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
        super(GadgetReviewReviewDetailsSpider, self).__init__(*args, **kwargs)

    def sync_spider_state(self, state):

        self.spiders_table.put_item(
            Item={
                'public_ip': self.public_ip,
                'private_ip': self.private_ip,
                'name': 'GadgetReviewReviewDetails',
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

    def parse_product_name(self, name: str) -> Union[str, None]:
        """ get product name """

        if "Review" in name:
            return name.replace("Review", "").strip()

        elif "Reviews" in name:
            return name.replace("Reviews", "").strip()
        else:
            logging.info("No product name")
            return None

    def parse_title(self, response: Response) -> Union[str, None]:
        """ get review's title """
        title = response.css("h1 ::text").get()

        if title:
            return title.strip()

        else:
            logging.info("No author name")
            return title

    def parse_description(self, response: Response) -> Union[str, None]:
        """ get review's desscription """
        first_location = response.css("div.entry-content > p")
        second_location = response.selector.xpath("//main" +
                                                  "//div[contains(@class, 'has-text-color')]" +
                                                  "/p/text()").getall()

        if first_location:
            description = first_location.css("::text").get()
            return description

        elif second_location:
            description = second_location[2]
            return description

        else:
            logging.info("No author name")
            return None

    def parse_author(self, response: Response) -> Union[str, None]:
        """ get review's author name """
        name = response.css("a.wp-block-post-author-name__link ::text").get()

        if name:
            return name.strip()

        else:
            logging.info("No author name")
            return name

    def parse_pros_cons(self, response: Response):
        """ parses pros and cons of item """
        pros_cons = dict()
        pros_cons["pros"] = None
        pros_cons["cons"] = None
        pros_cons_locator = response.css("div#snapshot") \
            .css("ul.wp-block-crucial-blocks-acf-critic-consensus-topics")

        if pros_cons_locator:
            pros = pros_cons_locator[0].css("li")
            cons = pros_cons_locator[1].css("li")
            pros_cons["pros"] = [text.css("::text").get() for text in pros]
            pros_cons["cons"] = [text.css("::text").get() for text in cons]

            return pros_cons

        else:
            return pros_cons

    def parse_verdict(self, response: Response):
        """ parse review's verdict """
        first_verdict_locator = response.selector.xpath(
            "//p[text()='True Score']")
        second_verdict_locator = response.selector.xpath("//strong[text()='Expert Rating'] " +
                                                         "/ ancestor::div[1] " +
                                                         "/ div " +
                                                         "/ text()").get()
        if first_verdict_locator:
            try:
                verdict = float(first_verdict_locator[0]
                                .xpath("ancestor::div[2]/div[2]/div/text()").get()) / 10
                return verdict

            except ValueError:
                logging.info("Verdict is NR")
                return None

        elif second_verdict_locator:
            verdict = float(second_verdict_locator) / 10
            return verdict

        else:
            logging.warning("Can't locate verdict, requires recheck")
            return None

    def parse_product_parameters(self, response: Response):
        """ parse product technical specifications """
        specs = dict()
        specs_locators = response.selector.xpath("//strong[text()='Specifications'] " +
                                                 "/ ancestor::div[2] " +
                                                 "/ div[2] " +
                                                 "/ table " +
                                                 "/ tbody " +
                                                 "/ tr")

        if specs_locators:
            for locator in specs_locators:
                key = locator.css("td::text").getall()[0]
                value = locator.css("td::text").getall()[1]
                specs[key] = value

            return specs

        else:
            logging.info("No specifications on page")
            return None

    def parse_category(self, response: Response):
        """ parse category of item """
        category_selector = response.css("nav.crucial-breadcrumb " +
                                         "> ul " +
                                         "> li " +
                                         "> a " +
                                         "> span")

        if category_selector:
            category = category_selector.css("::text").getall()[-2]
            return category

        else:
            logging.warning("Can't locate category, requires recheck")
            return None

    def parse_date(self, response: Response):
        """ parse review's last update date """
        date_selector = response.css("meta[property='og:updated_time']::attr('content')").get()

        if date_selector:
            date = next(datefinder.find_dates(date_selector)
                        ).strftime("%Y-%m-%d %H:%M:%S")
            return date

        else:
            logging.warning("Can't locate date url, requires rechecking")
            return None

    def parse(self, response, **kwargs):
        try:
            item = ReviewDetailsResponseItem()

            item["appid"] = response.meta['appid']
            item["crawlid"] = response.meta['crawlid']
            item["url"] = response.url
            item["responseUrl"] = response.url
            item["statusCode"] = response.status
            item["reviewUrl"] = response.request.url
            item["author"] = self.parse_author(response)
            item["name"] = self.parse_title(response)
            item["category"] = self.parse_category(response)
            item["createDate"] = self.parse_date(response)
            item["keywords"] = None
            item["technoBlogId"] = 7
            item["productName"] = self.parse_product_name(item["name"])
            item["description"] = self.parse_description(response)
            pros_cons = self.parse_pros_cons(response)
            item["pros"] = pros_cons["pros"]
            item["cons"] = pros_cons["cons"]
            item["baseParameters"] = None
            item["verdict"] = self.parse_verdict(response)
            item["productParameters"] = self.parse_product_parameters(
                response)
            yield item
        except Exception as e:
            logging.exception("GadgetReview error when parse review details")
            raise