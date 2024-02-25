from dataclasses import dataclass

@dataclass
class Item:
    """ target values for parsing """
    source_id: str
    source_product_id: str
    url: str
    title: str
    old_price: str
    current_price: str
    images: list
    description: str
    original_category: str
    coupon_code: str
    categories: str