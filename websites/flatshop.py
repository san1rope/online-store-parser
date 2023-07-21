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


async def parser_flatshop(session: aiocfscrape.CloudflareScraper, item_url: str, proxylist: list, multiplier: float,
                          request_delay: float = None):
    try:
        proxies = await get_proxy(proxylist)
        headers = {"user-agent": user_agent.generate_user_agent()}

        markup = await get_data(session, item_url, request_delay, headers, proxies)
        soup_item = BeautifulSoup(markup, "lxml")

        title = soup_item.find(class_="item-title").text.strip().replace(',', '').upper()
        image_link = soup_item.find("div", {"id": "item-carousel"}).find("a")["href"]

        sizes = []
        selector = soup_item.find(class_="form-group").find_all("a")
        for size in selector:
            size_name = size.text.replace('*', '').strip()
            if size_name == "U":
                size_name = "ONE_SIZE"

            if size.get("disabled"):
                size_price = "SOLD_OUT"
            else:
                full_size_price = soup_item.find(class_="final-price in-offer")
                if not full_size_price:
                    full_size_price = soup_item.find(class_="final-price")

                size_price = ""
                for ch in full_size_price.text.strip():
                    if ch.isdigit() or ch == ".":
                        size_price += ch

                size_price = await fixed_price((float(size_price) * multiplier), 2)

            sizes.append(f"{size_name} = {size_price}")

        return [title, image_link, item_url, '\n'.join(sizes)]
    except Exception:
        print(f"[-] {item_url} {traceback.format_exc()}")


async def flatshop(proxylist: list, proxy_all_values: list):
    main_url, file_name = "https://www.flatshop.it", "flatshop"
    request_delay = 0.1
    headers = {"user-agent": user_agent.generate_user_agent()}

    agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

    primary_id = proxy_all_values[19][4]
    multiplier = float(proxy_all_values[2][7].replace(',', '.'))
    worksheet_id = proxy_all_values[19][3]

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
        table_content = []
        soup_sitemap_prods = BeautifulSoup(sitemap_markup, "xml")
        all_loc = []
        for loc in soup_sitemap_prods.find_all("loc"):
            if loc.text.split("/")[-1].isdigit():
                all_loc.append(loc)

        tasks = []
        max_active_tasks = 10
        for loc in all_loc:
            if len(tasks) >= max_active_tasks:
                results = await asyncio.gather(*tasks)
                table_content.extend(results)
                tasks.clear()

            task = asyncio.create_task(parser_flatshop(session, loc.text, proxylist, multiplier, request_delay))
            tasks.append(task)

        if len(tasks):
            results = await asyncio.gather(*tasks)
            table_content.extend(results)
            tasks.clear()


    with open(f"temp/{file_name}.csv", "w", encoding="utf-8", newline='') as file:
        writer = csv.writer(file)
        writer.writerows(await formatted_content(all_records, table_content, primary_id))

    end = time.time() - start
    print(f"{main_url} | Parsing completed! Took time: {end} seconds")

    await upload_to_gs(agcm, file_name, worksheet_id)
