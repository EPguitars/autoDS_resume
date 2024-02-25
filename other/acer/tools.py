# Extend path to the root of the project and notificator
import os
import sys
from random import shuffle

script_dir = os.path.dirname(os.path.abspath(__file__))
paths = [
    os.path.abspath(os.path.join(script_dir, "../../")),
    os.path.abspath(os.path.join(script_dir, "."))
    ]

sys.path.extend(paths)


# ========================== SETUP COUNTER ==========================
counter = dict()
counter["filtered_and_sended"] = 0

# Setup proxy generator
from itertools import cycle

# =========================== SCRAPER NAME ===========================
SCRAPER_NAME = "acer"


# =================================== DISCOUNTS INFO ===================================
from main_tools.discount_api import get_discount
from categories import categories

def grab_discounts_info():
    """ grab discounts info from discount api """

    discounts_info = dict()

    for category in categories.keys():
        
        current_discount = get_discount(SCRAPER_NAME, str(category[0]))
        discounts_info[str(category[0])] = current_discount

    return discounts_info

# discounts_info = grab_discounts_info()


#=============================== PROXY ===============================
from main_tools.proxy import proxies as imported_proxies
from main_tools.proxy import blacklisted_proxies

imported_proxies = [d for d in imported_proxies if d["all://"] not in blacklisted_proxies] 

proxies = []
for proxy in imported_proxies:
    temp = proxy["all://"].replace("http://", "").split("@")
    z = [x for x in temp[0].split(":")] + [x for x in temp[1].split(":")]
    proxies.append({"username": z[0], "password": z[1], "host": z[2], "port": z[3]})


shuffle(proxies)
proxy_splash = cycle(proxies)


playwright_proxies = []
for proxy in proxies:
    playwright_proxies.append({
        "server" : "http://" + proxy["host"] + ":" + proxy["port"],
        "username" : proxy["username"],
        "password" : proxy["password"]}
        )

shuffle(playwright_proxies)     
playwright_proxy_gen = cycle(playwright_proxies)
