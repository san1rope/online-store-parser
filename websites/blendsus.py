import asyncio
import csv
import re
import time
import traceback

import gspread_asyncio
import lxml
import user_agent

import aiocfscrape
from bs4 import BeautifulSoup

from tools import get_data, connect_to_gs, get_proxy, fixed_price, formatted_content, get_creds, upload_to_gs


async def parser_blendsus(session: aiocfscrape.CloudflareScraper, item_url: str, proxylist: list, multiplier: float,
                          request_delay: float = None, retry: int = 3):
    proxies = await get_proxy(proxylist)
    headers = {"user-agent": user_agent.generate_user_agent()}
    try:
        item_markup = await get_data(session, item_url, request_delay, headers, proxies)
        soup_item = BeautifulSoup(item_markup, "lxml")

        title = soup_item.find("a", class_="product__title").text.strip().upper()
        image_link = "https:" + soup_item.find(class_="product__media media media--transparent").find("img")["src"]

        sizes = []
        try:
            selector = soup_item.find(class_="js product-form__input").find_all("input")
            for size in selector:
                size_price = ""
                size_name = soup_item.find(
                    "label", {"for": size["id"]}).text.replace(
                    "Variant sold out or unavailable", "").strip().replace(' ', '_')
                if size_name == "OS":
                    size_name = "ONE_SIZE"

                try:
                    if "disabled" in size["class"]:
                        size_price = "SOLD_OUT"
                except KeyError:
                    full_size_price = soup_item.find(
                        "div", {"class": "no-js-hidden", "id": re.compile("price-template--")}).find(
                        class_="price-item price-item--sale price-item--last").text.strip()
                    for ch in full_size_price:
                        if ch.isdigit() or ch == ".":
                            size_price += ch

                    size_price = await fixed_price((float(size_price) * multiplier), 2)

                sizes.append(f"{size_name} = {size_price}")
        except AttributeError:
            sizes.append("ONE_SIZE = SOLD_OUT")

        return [title, image_link, item_url, '\n'.join(sizes)]
    except Exception:
        print(f"[-] {item_url} {traceback.format_exc()}")
        await asyncio.sleep(5)
        if retry:
            return await parser_blendsus(session, item_url, proxylist, multiplier, request_delay, (retry - 1))


async def blendsus(proxylist: list, proxy_all_values: list):
    main_url, file_name = "https://www.blendsus.com", "blendsus"
    request_delay = 0.1
    headers = {"user-agent": user_agent.generate_user_agent()}

    agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

    primary_id = proxy_all_values[23][4]
    multiplier = float(proxy_all_values[4][7].replace(',', '.'))
    worksheet_id = proxy_all_values[23][3]

    worktable = await connect_to_gs(worksheet_id, agcm)
    worksheet = await worktable.get_worksheet(0)
    all_records = await worksheet.get_all_values()
    if len(all_records) > 0:
        all_records.pop(0)

    start = time.time()

    async with aiocfscrape.CloudflareScraper() as session:
        proxies = await get_proxy(proxylist)
        sitemap_url = f"{main_url}/sitemap.xml"
        sitemap_markup = await get_data(session=session, proxies=proxies, headers=headers, request_delay=request_delay,
                                        url=sitemap_url)
        soup_sitemap = BeautifulSoup(sitemap_markup, "xml")
        sitemap_urls = soup_sitemap.find_all("loc", string=re.compile(f"{main_url}/sitemap_products_"))
        table_content = []
        for url in sitemap_urls:
            sitemap_prods = await get_data(session=session, url=url.text, request_delay=request_delay, headers=headers,
                                           proxies=proxies)
            soup_sitemap_prods = BeautifulSoup(sitemap_prods, "xml")
            all_tags = soup_sitemap_prods.find_all("loc", string=re.compile(f"{main_url}/products/"))
            tasks = []
            max_active_tasks = 10
            for loc in all_tags:
                if len(tasks) >= max_active_tasks:
                    results = await asyncio.gather(*tasks)
                    table_content.extend(results)
                    tasks.clear()

                task = asyncio.create_task(parser_blendsus(session, loc.text, proxylist, multiplier, request_delay))
                tasks.append(task)

            if len(tasks):
                results = await asyncio.gather(*tasks)
                table_content.extend(results)
                tasks.clear()

    table_content = [i for i in table_content if i]
    with open(f"temp/{file_name}.csv", "w", encoding="utf-8", newline='') as file:
        writer = csv.writer(file)
        writer.writerows(await formatted_content(all_records, table_content, primary_id))

    end = time.time() - start
    print(f"{main_url} | Parsing completed! Took time: {end} seconds")

    await upload_to_gs(agcm, file_name, worksheet_id)
