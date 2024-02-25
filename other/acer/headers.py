import asyncio

from aiolimiter import AsyncLimiter

from automated_browser import scrape_cookies


url = "https://store.acer.com/en-in/laptops?p=2"
limiter = AsyncLimiter(10, 1)

cookie = asyncio.run(scrape_cookies(url, limiter))


headers = {
  'authority': 'store.acer.com',
  'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
  'accept-language': 'en-GB,en;q=0.9,ru-RU;q=0.8,ru;q=0.7,en-US;q=0.6',
  'cache-control': 'max-age=0',
  'cookie': cookie.replace("\n", ""),
  'referer': 'https://store.acer.com/en-in/laptops?p=2&product_list_limit=25',
  'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Windows"',
  'sec-fetch-dest': 'document',
  'sec-fetch-mode': 'navigate',
  'sec-fetch-site': 'same-origin',
  'sec-fetch-user': '?1',
  'upgrade-insecure-requests': '1',
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}