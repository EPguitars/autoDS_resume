# -*- coding: utf-8 -*-
import gc
import boto3
import time
import logging
from typing import Union
from urllib.parse import (urlparse,
                          parse_qs,
                          urlencode,
                          urlunparse)

import datefinder
import urllib.request
from pydispatch import dispatcher
from scrapy import signals, Request
from scrapy.selector.unified import Selector

from bhfutils.crawler.items import ReviewResponseItem
from bhfutils.crawler.spiders.redis_spider import RedisSpider


class GadgetReviewReviewsSpider(RedisSpider):
    spider_state = "initial"

    name = "GadgetReviewReviews"
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
        super(GadgetReviewReviewsSpider, self).__init__(*args, **kwargs)

    def log_spider_message(self, message):
        self.spider_messages_table.put_item(
            Item={
                'public_ip': self.public_ip,
                'private_ip': self.private_ip,
                'messageDate': int(round(time.time() * 1000)),
                'name': 'GadgetReviewReviews',
                'message': message,
            }
        )

    def sync_spider_state(self, state):
        self.spiders_table.put_item(
            Item={
                'public_ip': self.public_ip,
                'private_ip': self.private_ip,
                'name': 'GadgetReviewReviews',
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

    def scrape_url(self, item: Selector) -> Union[str, None]:
        """ scrape and parse review's url """
        url = item.css("a ::attr(href)").get()

        if url:
            return url

        else:
            logging.warning("Can't locate url, requires rechecking!")
            return None

    def scrape_name(self, item: Selector) -> Union[str, None]:
        """ scrape and parse review's title """
        title = item.css("a ::text").get()

        if title:
            return title

        else:
            logging.warning("Can't locate title, requires rechecking")
            return None

    def scrape_image(self, item: Selector) -> Union[str, None]:
        """ scrape review's image url """
        img_url = item.css("img ::attr(src)").get()

        if img_url:
            return img_url

        else:
            logging.warning("Can't locate image url, requires rechecking")
            return None

    def parse(self, response, **kwargs):
        try:
            # first get all cards from page
            cards = response.css("li.wp-block-post")

            if cards:
                for card in cards:
                    item = ReviewResponseItem()
                    item["appid"] = response.meta["appid"]
                    item["crawlid"] = response.meta["crawlid"]
                    item["url"] = response.request.url
                    item["statusCode"] = response.status
                    item["technoBlogId"] = 7
                    item["responseUrl"] = response.request.url
                    item["reviewUrl"] = self.scrape_url(card)
                    item["name"] = self.scrape_name(card)
                    item["imageUrl"] = self.scrape_image(card)

                    yield item

                # Page scraped
                # Now we reconstructing url
                # and constructing new one
                parsed_url = urlparse(response.request.url)
                query_params = parse_qs(parsed_url.query)
                page_number = int(query_params["page"][0])
                query_params["page"] = [page_number + 1]
                modified_query_string = urlencode(query_params, doseq=True)
                next_page = urlunparse(
                    parsed_url._replace(query=modified_query_string))

                yield Request(next_page, callback=self.parse, dont_filter=True,
                              meta={'proxy': response.meta["attrs"]["proxy"]})

            else:
                logging.warning("No cards on page, scraping is done!")
        except Exception as e:
            logging.exception("GadgetReview error when parse review page")
            raise