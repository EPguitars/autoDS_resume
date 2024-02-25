# -*- coding: utf-8 -*-
import gc
import time
import boto3
import logging
from datetime import datetime
from urllib.parse import urljoin

import datefinder
import urllib.request
from pydispatch import dispatcher
from scrapy import signals, Request
from bhfutils.crawler.items import ReviewResponseItem
from bhfutils.crawler.spiders.redis_spider import RedisSpider

STOP_DATE = datetime(2013, 1, 1)


class ChipReviewsSpider(RedisSpider):
    spider_state = "initial"

    name = "ChipReviews"
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
        self.spider_messages_table = dynamo_db.Table("BhfSpiderMessages")
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
        dispatcher.connect(self.log_error, signals.spider_error)
        dispatcher.connect(self.log_error, signals.item_error)
        dispatcher.connect(self.log_idle, signals.spider_idle)
        dispatcher.connect(self.log_working, signals.request_received)
        super(ChipReviewsSpider, self).__init__(*args, **kwargs)

    def log_spider_message(self, message):
        self.spider_messages_table.put_item(
            Item={
                'public_ip': self.public_ip,
                'private_ip': self.private_ip,
                'messageDate': int(round(time.time() * 1000)),
                'name': 'ChipReviews',
                'message': message,
            }
        )

    def sync_spider_state(self, state):
        self.spiders_table.put_item(
            Item={
                'public_ip': self.public_ip,
                'private_ip': self.private_ip,
                'name': 'ChipReviews',
                'status': state,
                'status_date': int(round(time.time() * 1000)),
            }
        )

    def spider_opened(self):
        self.sync_spider_state("opened")

    def log_working(self):
        if self.spider_state != "working":
            self.spider_state = "working"
            self.sync_spider_state("working")

    def log_idle(self):
        if self.spider_state != "idle":
            self.spider_state = "idle"
            self.sync_spider_state("idle")

    def spider_closed(self):
        self.sync_spider_state("closed")

    def log_error(self, failure):
        self.log_spider_message(failure.getErrorMessage())

    def check_next_page(self, response):
        """checks for next page and returns url"""
        button = response.css(
            "a.Button.Button--Pagination.Button--Primary.is-next")
        button_class = button.css("::attr(class)").get()

        if button and "is-disabled" not in button_class:
            href = button.css("::attr(href)").get()
            new_url = urljoin("https://www.chip.de", href)
            return new_url

        else:
            return None

    def filter_articles(self, articles) -> list:
        """ filter articles and only TEST and ARTIKEL category """
        filtered_articles = []

        for article in articles:
            article_category = article.css(
                "div.meta.caps ::text").get().strip()

            if "test" in article_category or "artikel" in article_category:
                filtered_articles.append(article)

        return filtered_articles

    def not_too_old(self, article) -> bool:
        """ Checks if review is old or not """
        if not article:
            return True

        # parse string with date info
        date_string = article.css("time ::text").get().strip()
        # now make it datetime object
        review_datetime = next(datefinder.find_dates(date_string))

        # compare article time with limit time
        if review_datetime > STOP_DATE:
            return True
        else:
            return False

    def parse(self, response, **kwargs):
        try:
            # first get all articles from page
            filtered_articles = []
            all_articles = response.css("div.Listing > ul > li")
            # check if we grab something
            if all_articles:
                # if we do filter
                filtered_articles = self.filter_articles(all_articles)
                # later need last article to check actuality of content
                last_article = all_articles[-1]

            else:
                logging.warning("Website changed structure, need refactor")

            for article in filtered_articles:
                item = ReviewResponseItem()

                item["appid"] = response.meta["appid"]
                item["crawlid"] = response.meta["crawlid"]
                item["url"] = response.request.url
                item["statusCode"] = response.status
                item["technoBlogId"] = 4
                item["responseUrl"] = response.url
                item["reviewUrl"] = article.css("a ::attr(href)").get()
                item["name"] = article.css("h4 ::text").get()
                if article.css("img"):
                    item["imageUrl"] = article.css(
                        "img")[-1].css("::attr(src)").get()

                yield item

            # check if next page exists
            next_page = self.check_next_page(response)
            # check if we working with too old reviews
            not_too_old = self.not_too_old(last_article)

            if next_page and not_too_old:
                yield Request(next_page, callback=self.parse, dont_filter=True)

            next_page = None
            # if we have next pages we scraping them too with new request
            if next_page is not None:
                yield Request(
                    next_page,
                    callback=self.parse,
                    dont_filter=True,
                    meta={"proxy": response.meta["attrs"]["proxy"]},
                )

        except Exception as e:
            logging.exception("Chip error when parse review page")
            raise