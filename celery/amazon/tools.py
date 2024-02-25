# ==================== PATH TO NOTIFICATOR ====================
from random import shuffle
from itertools import cycle
import os
import sys

script_dir = os.path.dirname(os.path.abspath(__file__))
grandparent_dir = os.path.abspath(os.path.join(script_dir, "../../"))
sys.path.append(grandparent_dir)

# ==================== IMPORTS ====================

# COUNTER SETUP
counter = dict()
counter["filtered_and_sended"] = 0


# ==================== SCRAPER NAME ====================
SCRAPER_NAME = "amazon_in"


# ==================== DISCOUNTS INFO ====================
from main_tools.discount_api import get_discount
from categories import categories


def grab_discounts_info():
    """ grab discounts info from discount api """

    discounts_info = dict()

    temp = 0
    for category in categories.keys():
        if temp == 97:
            break
        current_discount = get_discount(SCRAPER_NAME, category[1])
        discounts_info[category[1]] = current_discount

        temp += 1
    return discounts_info

#discounts_info = grab_discounts_info()
#discounts_info = None

# ==================== RENDER ====================

render_urls = [
    "http://51.75.81.209:8050/execute",
    "http://51.75.81.209:8051/execute",
    "http://51.75.81.209:8052/execute",
    "http://51.75.81.209:8053/execute",
    "http://51.75.81.209:8054/execute",
    "http://51.75.81.209:8055/execute",
    "http://51.75.81.209:8056/execute",
    "http://51.75.81.209:8057/execute",
    "http://51.75.81.209:8058/execute",
    "http://51.75.81.209:8059/execute",
    "http://51.75.81.209:8060/execute",
    "http://51.75.81.209:8061/execute",
    "http://51.75.81.209:8062/execute",
    "http://51.75.81.209:8063/execute",
    "http://51.75.81.209:8064/execute",
    "http://51.75.81.209:8065/execute",
    "http://51.75.81.209:8066/execute",
    "http://51.75.81.209:8067/execute",
    "http://51.75.81.209:8068/execute",
    "http://51.75.81.209:8069/execute"
]

shuffle(render_urls)
render = cycle(render_urls)


# ==================== USER AGENT ====================
user_agent_list = [
    # "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
    "Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15"
]

ua_gen = cycle(user_agent_list)

#==================== PROXY ====================
from main_tools.proxy import proxies as imported_proxies
from main_tools.proxy import blacklisted_proxies

imported_proxies = [d for d in imported_proxies if d["all://"] not in blacklisted_proxies] 

proxies = []
for proxy in imported_proxies:
    temp = proxy["all://"].replace("http://", "").split("@")
    z = [x for x in temp[0].split(":")] + [x for x in temp[1].split(":")]
    proxies.append({"username": z[0], "password": z[1], "host": z[2], "port": z[3]})

#killer_proxy = {"username": "EPguitars", "password": "holocron2_country-in", "host" : "geo.iproyal.com", "port": "12321"}
# proxies = [
#     {"username": "EPguitars", "password": "holocron2_country-in",
#         "host": "geo.iproyal.com", "port": "12321"},
#  ]
#old_proxies = proxies[:-100]
#web_unlocker = [{"username": "brd-customer-hl_42e53bb2-zone-unblocker", "password": "18gm9sljwtn1", "host" : "brd.superproxy.io", "port": "22225"}]
shuffle(proxies)
proxy_gen = cycle(proxies)
