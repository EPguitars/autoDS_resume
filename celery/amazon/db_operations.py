import os
import logging
import json

import httpx
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from rich import print
import aiomysql
import pymysql

from models import Item
from tools import counter, SCRAPER_NAME
load_dotenv()

# key to target api for data sending
API_KEY = os.environ.get("API_KEY")


class AsyncDatabaseManager:
    def __init__(self, host, port, user, password, db):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self.pool = None
        self.transaction_group = []

    async def connect(self):
        self.pool = await aiomysql.create_pool(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.db,
            autocommit=True,  # Set autocommit to True if needed
        )
        print("Connected to MySQL database.")

    async def execute_query(self, sql):
        if not self.pool:
            raise ValueError(
                "Connection pool not initialized. Call connect() first.")

        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(sql)
                result = await cursor.fetchall()

        return result

    async def reset_asins(self):
        if not self.pool:
            raise ValueError(
                "Connection pool not initialized. Call connect() first.")

        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("""
                                    UPDATE amazon_asin
                                    SET isScraped = 0                   
                                    """)
        logging.info("Asin table reseted")

    async def check_item_exists(self, item):
        if not self.pool:
            raise ValueError(
                "Connection pool not initialized. Call connect() first.")

        async with self.pool.acquire() as connection:
            query = """
                SELECT EXISTS (
                    SELECT 1
                    FROM `products`
                    WHERE `sourceId` = %s
                    AND `sourceProductId` = %s
                ) AS item_exists
            """

            async with connection.cursor() as cursor:
                await cursor.execute(query, (item.source_id, item.source_product_id))
                result = await cursor.fetchone()

                item_exists = bool(result[0])

            return item_exists

    async def get_images_db(self, item: Item):
        """ get images from db """
        if not self.pool:
            raise ValueError(
                "Connection pool not initialized. Call connect() first.")
        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(f"""
                    SELECT * FROM `products_images`
                    WHERE productId =
                    ( SELECT id FROM products p
                    WHERE p.sourceId = '{item.source_id}'
                    AND p.sourceProductId = '{item.source_product_id}'
                    LIMIT 1 )
                    """)

                results = await cursor.fetchall()
        images = []
        for result in results:
            images.append(result[2])

        if images:
            return images
        else:
            logging.error("None of images in db!")
            return None

    async def save_asin(self, item, is_scraped):
        if not self.pool:
            raise ValueError(
                "Connection pool not initialized. Call connect() first.")
        url = item.url
        asin = item.source_product_id
        categories = item.original_category
        slugs = json.dumps(item.categories)
        title = item.title
        blacklisted = 0

        query = """
        INSERT INTO `amazon_asin` (`asin`, `url`, `isScraped`, `originalCategory`, `categories`, `title`, `blacklisted`) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE url = %s, isScraped = %s, originalCategory = %s, categories = %s, title = %s
        """
        data_to_insert = (asin, url, is_scraped, categories, slugs,
                          title, blacklisted, url, is_scraped, categories, slugs, title)

        return [query, data_to_insert]

    async def get_description_db(self, item: Item):
        """ get description from db """
        if not self.pool:
            raise ValueError(
                "Connection pool not initialized. Call connect() first.")

        async with self.pool.acquire() as connection:
            query = (f"""
                SELECT description
                FROM products
                WHERE sourceId = '{item.source_id}'
                AND sourceProductId = '{item.source_product_id}'""")

            async with connection.cursor() as cursor:
                await cursor.execute(query)
                result = await cursor.fetchone()
        return result[0]

    async def change_scraped_status(self, asin, is_scraped, blacklisted):
        if not self.pool:
            raise ValueError(
                "Connection pool not initialized. Call connect() first.")

        if is_scraped is True:

            query = f"""
            UPDATE amazon_asin
            SET isScraped = 1, blacklisted = {blacklisted}
            WHERE asin = '{asin}'

            """
            return query

        else:
            query = f"""
            UPDATE amazon_asin
            SET isScraped = 0
            WHERE asin = '{asin}'
            """
            return query

    async def count_items_to_update(self):
        """ count items needed to be updated """
        if not self.pool:
            raise ValueError(
                "Connection pool not initialized. Call connect() first.")

        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:

                query = ("""
                SELECT COUNT(*) FROM amazon_asin WHERE isScraped = 0
                AND blacklisted = 0
                AND originalCategory IN ('Loafers & Moccasins', 
                         'Health Drinks & Nutrition Bars',
                         'Strollers & Prams', 
                         'Buggies',
                         'Finger Sleeves',
                         'Smart Televisions',
                         'Fashion Sandals',
                         'Sports & Outdoor Shoes',
                         'Clogs & Mules',
                         'Diapering & Nappy Changing',
                         'Oral Care',
                         'Casual Shoes',
                         'Traditional Laptops',
                         'Handkerchiefs',
                         'Baby Safety',
                         'Standard Televisions',
                         'Refrigerators',
                         'Ceiling Lighting',
                         'Washing Machines & Dryers',
                         'Food',
                         'Toe Rings',
                         'Cleaning Kits',
                         'Feeding',
                         'Controllers',
                         'Chains',
                         'Drawing',
                         'Paintings',
                         'Art Prints',
                         'Drawings',
                         'Paintings',
                         'Photographs',
                         'Baby Care',
                         'Innerwear',
                         'Batteries & Accessories',
                         'Carry Cots',
                         'Jeans & Jeggings',
                         'Sweatshirts & Hoodies',
                         'Sarees',
                         'Handbags',
                         'Sneakers',
                         'Bedding Sets',
                         'Cleaning Supplies',
                         'Fragrance',
                         'Make-up',
                         'Vitamins',
                         'Jeans',
                         'Dresses & Jumpsuits',
                         'Cradles',
                         'Chargers',
                         'Split-System Air Conditioners',
                         'Smartphones',
                         'T-shirts & Polos',
                         'Shirts',
                         'Jeans',
                         'Sunglasses',
                         'Belts',
                         'Fashions Smartwatches',
                         'Formal Shoes',
                         'Tops, T-Shirts & Shirts',
                         'Trousers',
                         'Smart watches',
                         'In-Ear',
                         'On-Ear',
                         'Over-Ear',
                         'Joysticks',
                         'Screen Expanders & Magnifiers',
                         'Triggers',
                         'Gamepads',
                         'Playstation 5',
                         'Xbox Series X',
                         'Engine Care',
                         'Fillers, Adhesives & Sealants',
                         'Glass Care',
                         'Computer Cases',
                         'Charging Stations',
                         'Microwave Ovens',
                         'Bluetooth',
                         'Adhesive Card Holders',
                         'Audio Adapters',
                         'Lightning Cables',
                         'OTG Adapters',
                         'USB Cables',
                         'Bumpers',
                         'Basic Cases',
                         'Flip & Wallet Cases',
                         'Case & Cover Bundles',
                         'Phone Socks',
                         'Battery Charger Cases',
                         'Holsters',
                         'Automobile Chargers',
                         'Induction Chargers',
                         'Formal Shoes (Women)',
                         'Wedges',
                         'Suits & Blazers'
                         )
                """)
                await cursor.execute(query)
                result = await cursor.fetchone()
        return result[0]

    async def grab_items_on_website(self):
        if not self.pool:
            raise ValueError(
                "Connection pool not initialized. Call connect() first.")
        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:

                query = ("""SELECT * FROM `amazon_asin` 
                            WHERE asin IN (
                                SELECT sourceProductId
                                FROM `products`
                                WHERE sourceId = 'amazon_in'
                                AND isActual = 0
                                AND isPicked = 1
                                )
                         """)
                await cursor.execute(query)
                results = await cursor.fetchall()
        
        return results


    async def grab_items(self):
        """ grab every items which need update """
        if not self.pool:
            raise ValueError(
                "Connection pool not initialized. Call connect() first.")
        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:

                query = ("""
                SELECT * FROM `amazon_asin` 
                WHERE isScraped = 0
                AND blacklisted = 0
                AND originalCategory IN ('Loafers & Moccasins',
                         'Health Drinks & Nutrition Bars', 
                         'Strollers & Prams', 
                         'Buggies',
                        'Finger Sleeves',
                         'Smart Televisions',
                         'Fashion Sandals',
                         'Sports & Outdoor Shoes',
                         'Clogs & Mules',
                         'Diapering & Nappy Changing',
                         'Oral Care',
                         'Casual Shoes',
                         'Traditional Laptops',
                         'Handkerchiefs',
                         'Baby Safety',
                         'Standard Televisions',
                         'Refrigerators',
                         'Ceiling Lighting',
                         'Washing Machines & Dryers',
                         'Food',
                         'Toe Rings',
                         'Cleaning Kits',
                         'Feeding',
                         'Controllers',
                         'Chains',
                         'Drawing',
                         'Paintings',
                         'Art Prints',
                         'Drawings',
                         'Paintings',
                         'Photographs',
                         'Baby Care',
                         'Innerwear',
                         'Batteries & Accessories',
                         'Carry Cots',
                         'Jeans & Jeggings',
                         'Sweatshirts & Hoodies',
                         'Sarees',
                         'Handbags',
                         'Sneakers',
                         'Bedding Sets',
                         'Cleaning Supplies',
                         'Fragrance',
                         'Make-up',
                         'Vitamins',
                         'Jeans',
                         'Dresses & Jumpsuits',
                         'Cradles',
                         'Chargers',
                         'Split-System Air Conditioners',
                         'Smartphones',
                         'T-shirts & Polos',
                         'Shirts',
                         'Jeans',
                         'Sunglasses',
                         'Belts',
                         'Fashions Smartwatches',
                         'Formal Shoes',
                         'Tops, T-Shirts & Shirts',
                         'Trousers',
                         'Smart watches',
                         'In-Ear',
                         'On-Ear',
                         'Over-Ear',
                         'Joysticks',
                         'Screen Expanders & Magnifiers',
                         'Triggers',
                         'Gamepads',
                         'Playstation 5',
                         'Xbox Series X',
                         'Engine Care',
                         'Fillers, Adhesives & Sealants',
                         'Glass Care',
                         'Computer Cases',
                         'Charging Stations',
                         'Microwave Ovens',
                         'Bluetooth',
                         'Adhesive Card Holders',
                         'Audio Adapters',
                         'Lightning Cables',
                         'OTG Adapters',
                         'USB Cables',
                         'Bumpers',
                         'Basic Cases',
                         'Flip & Wallet Cases',
                         'Case & Cover Bundles',
                         'Phone Socks',
                         'Battery Charger Cases',
                         'Holsters',
                         'Automobile Chargers',
                         'Induction Chargers',
                         'Formal Shoes (Women)',
                         'Wedges',
                         'Suits & Blazers'
                         )
                """)
                await cursor.execute(query)
                results = await cursor.fetchall()
        return results

    async def grab_custom_items(self):
        """ grab every items which need update """
        if not self.pool:
            raise ValueError(
                "Connection pool not initialized. Call connect() first.")
        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:

                query = ("""
                SELECT * FROM `amazon_asin` 
                WHERE originalCategory = 'Suits & Blazers'
                AND isScraped = 0
                """)
                #AND isScraped = 0
                
                await cursor.execute(query)
                results = await cursor.fetchall()
        return results
    
    async def perform_transcation(self):
        """ perform transcation with queries """
        if not self.pool:
            raise ValueError(
                "Connection pool not initialized. Call connect() first.")

        try:
            async with self.pool.acquire() as connection:
                async with connection.cursor() as cursor:

                    await connection.begin()
                    for query in self.transaction_group:

                        try:
                            if isinstance(query, list):
                                await cursor.execute(query[0], query[1])
                            else:

                                await cursor.execute(query)
                                
                        except Exception as exc:
                            logging.error("Exception in transcation: %s", exc)
                            continue

                    await connection.commit()
                    logging.info("Transaction commited")
                    self.transaction_group = []
            return True

        except Exception as exc:
            logging.error("Exception in transcation: %s", exc)
            await connection.rollback()
            return False

    async def to_blacklist(self, asin):
        """ function to blacklist item """
        if not self.pool:
            raise ValueError(
                "Connection pool not initialized. Call connect() first.")

        query = f"""
            UPDATE amazon_asin
            SET blacklisted = 1
            WHERE asin = '{asin}'
            """

        self.transaction_group.append(query)
        print(len(self.transaction_group))

    async def close(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            logging.info("DB connection closed")
