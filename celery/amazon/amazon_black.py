import logging

import httpx
from selectolax.parser import HTMLParser, Selector

from headers import headers


def scrape_page(url: str, attempts = 3):
    with httpx.Client(headers=headers) as client:
        try:
            response = client.get(url, follow_redirects=True, timeout=60)

        except ValueError:
            logging.warning(
                "Value error when page requesting in validator")
            return None

        except (httpx.ConnectTimeout,
                httpx.ReadTimeout,
                httpx.ConnectError,
                httpx.ReadError,
                httpx.RemoteProtocolError):

            if attempts > 0:
                attempts -= 1
                logging.warning(
                    "Connection error, retrying request in validator")
                return scrape_page(url, attempts)

            else:
                logging.warning(
                    "Connection error when page requesting, in validator")
                return None


    with open("amazon.html", "w", encoding="UTF=8") as file:
        file.write(response.text)

if __name__ == "__main__":
    scrape_page("https://www.amazon.in/dp/B0BTDKWNVM")