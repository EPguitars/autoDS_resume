import logging
import asyncio
import re

from rich import print
from selectolax.parser import Selector

from models import Item
from image_validation import validate_image
from tools import SCRAPER_NAME

BASE_URL = "https://www.amazon.in"


def parse_item_price(item: Selector, parameter: str):
    """ parse prices of current item """

    if parameter == "old":
        old_price_selector = item.css_first(".a-section > .a-text-price > .a-offscreen")

        if old_price_selector:
            old_price = float(old_price_selector.text()
                              .replace("₹", "")
                              .replace(",", ""))
            return old_price

        else:
            logging.warning("Can't locate old price, requres rechecking")
            return None

    elif parameter == "current":
        current_price_selector = item.css_first(".a-price-whole")

        if current_price_selector:
            current_price = float(
                current_price_selector.text().replace(",", ""))
            return current_price

        else:
            logging.warning("Can't locate current price, requires rechecking")
            return None


def parse_asin(item: Selector):
    """ parse id of current item """
    asin = item.attributes["data-asin"]

    if asin:
        return asin

    else:
        logging.warning("Can't locate asin, requires markup recheck")
        return None


def parse_product_url(asin: str):
    """ parse url of product """
    url = BASE_URL + f"/dp/{asin}"
    return url


def check_availability(item):
    """
    True if out of stock
    """
    sold_out_selector = item.css_first("div.t4s-product-badge")

    if sold_out_selector:
        for element in sold_out_selector.css("span"):
            if element.text().upper() == "SOLD OUT":
                sold_out = True

                return sold_out
        return False

    else:
        logging.warning("cant locate product badge, requires recheck")
        return None


def parse_product_title(item: Selector):
    """ parse title of current product """
    title_selector = item.css("h2")

    if title_selector:
        title = " - ".join([x.text() for x in title_selector])

        return title

    else:
        logging.warning("Can't locate url, requires markup recheck")
        return None




def check_discount(old: float, sell: float, current_discount: float):
    """ checks if there any discount on this item """
    if old and sell:
        discount = (old - sell) / (old / 100)

    else:
        #print(old, sell)
        return None

    if discount >= current_discount and discount < 91.0 :
        return True

    else:
        #print(old, sell, discount)
        return None


def parse_popular_card(item: Selector, current_discount: float, category: str, slugs: list):
    """ parse item card """
    # check for discount
    old_price = parse_item_price(item, "old")
    current_price = parse_item_price(item, "current")
    print("==============================")

    discount = check_discount(
        old=old_price, sell=current_price, current_discount=current_discount)

    if discount:
        asin = parse_asin(item)
        parsed_item = Item(
            source_id=SCRAPER_NAME,
            source_product_id=asin,
            url=parse_product_url(asin),
            title=parse_product_title(item),
            old_price=old_price,
            current_price=current_price,
            images=None,
            description=None,
            original_category=category,
            coupon_code=None,
            categories=slugs
        )

        if not parsed_item.current_price or not parsed_item.old_price:
            return None
        if not parsed_item.title:
            return None
        if not parsed_item.source_product_id:
            return None
        if not parsed_item.url:
            return None
        return parsed_item

    else:
        print(discount)
        return None


def scrape_description(markup: Selector):
    """ scrape and parse item's category """
    description_selectors_1 = markup.css(
        "div.featureBulletsExpanderContent >> ul")
    description_selectors_2 = markup.css("div#feature-bullets >> ul")
    description_selectors_3 = markup.css("div#detailBullets_feature_div >> ul")
    description_selectors_4 = 'markup.css("")'

    description = ""

    if description_selectors_1:
        for element in description_selectors_1:
            description += element.text() + " \n"

        return description

    elif description_selectors_2:
        for element in description_selectors_2:
            description += element.text() + " \n"

        return description

    elif description_selectors_3:
        for element in description_selectors_3:
            description += element.text() + " \n"

        return description

    else:
        logging.warning("Can't locate description, requires markup recheck")
        return None


def scrape_images(markup: Selector):
    """ scrape and parse all images urls """
    images = []
    image_selectors = markup.css("div#altImages >> img")

    if image_selectors:

        for image in image_selectors:
            image_url = image.attributes["src"].replace(",", "")
            if not ".png" in image_url and not "overlay" in image_url:
                pattern = r"\._.+?_\."
                # Replacement text
                replacement = "."
                # Use re.sub to replace the pattern with the replacement text
                new_url = re.sub(pattern, replacement, image_url)
                images.append(new_url)

        if len(images) >= 3:
            return images[:3]
        else:
            logging.info("Less than 3 images")
            return None
    else:
        logging.warning("Can't locate images, requires markup recheck")
        return None



async def standart_itempage_parser(markup: Selector, item: Item):
    """ parse item card """
    # check for discount

    scraped_images = scrape_images(markup)
    
    tasks = []
    if scraped_images:
        for image in scraped_images:
            tasks.append(validate_image(image))

        checked_images = await asyncio.gather(*tasks)
        valid_images = list(
            filter(lambda x: x is not False and x is not None, checked_images))

        if valid_images and len(valid_images) == 3:
            item.images = valid_images

        else:
            return None
    
    else:
        return None
    
    item.description = scrape_description(markup)

    return item


if __name__ == "__main__":
    html = """"<div data-asin="B0CQP6Q9TJ" data-index="27" data-uuid="4e67f672-ba9b-4e65-bbc5-8c4b62f1a580" data-component-type="s-search-result" class="sg-col-4-of-24 sg-col-4-of-12 s-result-item s-asin sg-col-4-of-16 sg-col 
        s-widget-spacing-small sg-col-4-of-20"><div class="sg-col-inner"><div cel_widget_id="MAIN-SEARCH_RESULTS-27" class="s-widget-container s-spacing-small s-widget-container-height-small celwidget slot=MAIN
        template=SEARCH_RESULTS widgetId=search-results_24" data-csa-c-pos="24" data-csa-c-item-id="amzn1.asin.1.B0CQP6Q9TJ" data-csa-op-log-render="" data-csa-c-type="item"><span class="a-declarative"
        data-version-id="v1x9xtdrqwkf2b2sy3gv3bhvhpn" data-render-id="rdma0a7ssjem029o9yaa4i2932" data-action="puis-card-container-declarative" data-csa-c-type="widget"
        data-csa-c-func-deps="aui-da-puis-card-container-declarative"><div class="puis-card-container s-card-container s-overflow-hidden aok-relative puis-expand-height puis-include-content-margin puis
        puis-v1x9xtdrqwkf2b2sy3gv3bhvhpn s-latency-cf-section puis-card-border"><div class="a-section a-spacing-base"><div class="a-section a-spacing-none puis-status-badge-container aok-relative
        s-grid-status-badge-container puis-expand-height"><a class="a-link-normal s-underline-text s-underline-link-text s-link-style"
        href="/Yogabar-Variety-Protein-Vitamins-Immunity/dp/B0CQP6Q9TJ/ref=sr_1_24?qid=1706653820&amp;refinements=p_n_pct-off-with-tax%3A2665400031&amp;rnid=2665398031&amp;s=hpc&amp;sr=1-24"><span
        data-component-type="s-in-cart-badge-component" class="rush-component"
        data-component-props="{&quot;quantityPlaceholder&quot;:&quot;${quantity}&quot;,&quot;asin&quot;:&quot;B0CQP6Q9TJ&quot;,&quot;quantity&quot;:&quot;0&quot;,&quot;messageTemplate&quot;:&quot;${quantity} in
        cart&quot;}" data-version-id="v1x9xtdrqwkf2b2sy3gv3bhvhpn" data-render-id="rdma0a7ssjem029o9yaa4i2932"><span class="s-in-cart-badge-position aok-hidden"><span data-component-type="s-status-badge-component"        
        class="rush-component" data-component-props="{&quot;asin&quot;:&quot;B0CQP6Q9TJ&quot;,&quot;badgeType&quot;:&quot;in-cart&quot;}" data-version-id="v1x9xtdrqwkf2b2sy3gv3bhvhpn"
        data-render-id="rdma0a7ssjem029o9yaa4i2932"><span id="B0CQP6Q9TJ-in-cart" class="a-badge" data-a-badge-type="status"><span id="B0CQP6Q9TJ-in-cart-label" class="a-badge-label" data-a-badge-color="sx-summit"><span  
        class="a-badge-label-inner a-text-ellipsis"><span class="a-badge-text" data-a-badge-color="sx-granite">0 in cart</span></span></span></span></span></span></span> </a> </div><div class="s-product-image-container   
        aok-relative s-text-center s-image-overlay-grey puis-image-overlay-grey s-padding-left-small s-padding-right-small puis-spacing-small s-height-equalized puis puis-v1x9xtdrqwkf2b2sy3gv3bhvhpn"><span
        data-component-type="s-product-image" class="rush-component" data-version-id="v1x9xtdrqwkf2b2sy3gv3bhvhpn" data-render-id="rdma0a7ssjem029o9yaa4i2932"><a class="a-link-normal s-no-outline" target="_blank"
        href="/Yogabar-Variety-Protein-Vitamins-Immunity/dp/B0CQP6Q9TJ/ref=sr_1_24?qid=1706653820&amp;refinements=p_n_pct-off-with-tax%3A2665400031&amp;rnid=2665398031&amp;s=hpc&amp;sr=1-24"><div class="a-section
        aok-relative s-image-square-aspect"><img class="s-image" src="https://m.media-amazon.com/images/I/51KCoFRGsjL._AC_UL320_.jpg" srcset="https://m.media-amazon.com/images/I/51KCoFRGsjL._AC_UL320_.jpg 1x,
        https://m.media-amazon.com/images/I/51KCoFRGsjL._AC_UL480_FMwebp_QL65_.jpg 1.5x, https://m.media-amazon.com/images/I/51KCoFRGsjL._AC_UL640_FMwebp_QL65_.jpg 2x,
        https://m.media-amazon.com/images/I/51KCoFRGsjL._AC_UL800_FMwebp_QL65_.jpg 2.5x, https://m.media-amazon.com/images/I/51KCoFRGsjL._AC_UL960_FMwebp_QL65_.jpg 3x" alt="Yogabar Variety Pack 10g Protein Bars [Pack of  
        6], Protein Blend &amp; Premium Whey, 100% Veg, Rich Protein Bar with Date, Vit..." data-image-index="24" data-image-load="" data-image-latency="s-product-image"
        data-image-source-density="1"></div></a></span></div><div class="a-section a-spacing-small puis-padding-left-small puis-padding-right-small"><div data-cy="title-recipe" class="a-section a-spacing-none
        a-spacing-top-small s-title-instructions-style"><h2 class="a-size-mini a-spacing-none a-color-base s-line-clamp-3"><a class="a-link-normal s-underline-text s-underline-link-text s-link-style a-text-normal"        
        target="_blank" href="/Yogabar-Variety-Protein-Vitamins-Immunity/dp/B0CQP6Q9TJ/ref=sr_1_24?qid=1706653820&amp;refinements=p_n_pct-off-with-tax%3A2665400031&amp;rnid=2665398031&amp;s=hpc&amp;sr=1-24"><span
        class="a-size-base-plus a-color-base a-text-normal">Yogabar Variety Pack 10g Protein Bars [Pack of 6], Protein Blend &amp; Premium Whey, 100% Veg, Rich Protein Bar with Date, Vitamins, Fiber, Energy &amp; Immunity
        for fitness,Pack of 2</span> </a> </h2><div class="a-row a-size-base a-color-secondary"><div class="a-row a-color-base"><span class="a-size-base a-color-base s-background-color-platinum a-padding-mini aok-nowrap  
        aok-align-top aok-inline-block a-spacing-top-micro puis-medium-weight-text">Variety Pack</span></div></div></div><div data-cy="price-recipe" class="a-section a-spacing-none a-spacing-top-small
        s-price-instructions-style"><div class="a-row a-size-base a-color-base"><div class="a-row"><a class="a-link-normal s-no-hover s-underline-text s-underline-link-text s-link-style a-text-normal" target="_blank"     
        href="/Yogabar-Variety-Protein-Vitamins-Immunity/dp/B0CQP6Q9TJ/ref=sr_1_24?qid=1706653820&amp;refinements=p_n_pct-off-with-tax%3A2665400031&amp;rnid=2665398031&amp;s=hpc&amp;sr=1-24"><span class="a-price"
        data-a-size="xl" data-a-color="base"><span class="a-offscreen">₹549</span><span aria-hidden="true"><span class="a-price-symbol">₹</span><span class="a-price-whole">549</span></span></span> <span class="a-size-base
        a-color-secondary">(<span class="a-price a-text-price" data-a-size="b" data-a-color="secondary"><span class="a-offscreen">₹91.50</span><span aria-hidden="true">₹91.50</span></span>/100 g)</span> <div
        class="a-section aok-inline-block"><span class="a-size-base a-color-secondary">M.R.P: </span><span class="a-price a-text-price" data-a-size="b" data-a-strike="true" data-a-color="secondary"><span
        class="a-offscreen">₹900</span><span aria-hidden="true">₹900</span></span></div></a> <span class="a-letter-space"></span><span>(39% off)</span></div><div
        class="a-row"></div></div></div></div></div></div></span></div></div></div>"""
    from selectolax.parser import HTMLParser
    item = HTMLParser(html)

    old_price = parse_item_price(item, "old")
    current_price = parse_item_price(item, "current")

    print(old_price, current_price)