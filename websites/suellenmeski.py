import csv
import re
import time
import gspread_asyncio
import lxml
import json

from bs4 import BeautifulSoup
from tools import *


async def parser_suellenmeski(session: aiocfscrape.CloudflareScraper, item_url: str, proxylist: list, multiplier: float,
                              request_delay: float = None, retry: int = 3):
    try:
        proxies = await get_proxy(proxylist)
        headers = {"user-agent": user_agent.generate_user_agent()}

        item_markup = await get_data(session, item_url, request_delay, headers, proxies)
        soup_item = BeautifulSoup(item_markup, "lxml")

        title = soup_item.find(class_="product_title entry-title nectar-inherit-default").text.strip().upper()
        image_link = soup_item.find(class_="wp-post-image")["src"]

        data_json = soup_item.find("form", class_="variations_form cart")["data-product_variations"]
        data_dict = json.loads(data_json)

        sizes = []
        for size in data_dict:
            size_name = ""
            for attribute in size["attributes"]:
                size_name = size["attributes"][attribute].upper()

            if size_name == "TALLA ÚNICA":
                size_name = "ONE_SIZE"

            if size["is_in_stock"]:
                size_price = float(size["display_price"]) * multiplier
            else:
                size_price = "SOLD_OUT"

            sizes.append(f"{size_name} = {size_price}")

        return [title, image_link, item_url, '\n'.join(sizes)]
    except Exception:
        print(f"[-] {item_url} {traceback.format_exc()}")
        if retry:
            return await parser_suellenmeski(session, item_url, proxylist, multiplier, request_delay, (retry - 1))


async def suellenmeski(proxylist: list, proxy_all_values: list):
    main_url, file_name = "https://suellenmeski.com", "suellenmeski"
    request_delay = 0.1
    headers = {"user-agent": user_agent.generate_user_agent()}

    agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

    primary_id = proxy_all_values[26][4]
    multiplier = float(proxy_all_values[2][7].replace(',', '.'))
    worksheet_id = proxy_all_values[26][3]

    worktable = await connect_to_gs(worksheet_id, agcm)
    worksheet = await worktable.get_worksheet(0)
    all_records = await worksheet.get_all_values()
    if len(all_records) > 0:
        all_records.pop(0)

    start = time.time()

    async with aiocfscrape.CloudflareScraper() as session:
        proxies = await get_proxy(proxylist)
        sitemap_url = f"{main_url}/sitemap_index.xml"
        sitemap_markup = await get_data(session=session, proxies=proxies, headers=headers, request_delay=request_delay,
                                        url=sitemap_url)
        soup_sitemap = BeautifulSoup(sitemap_markup, "xml")
        sitemap_urls = soup_sitemap.find_all("loc", string=re.compile(f"{main_url}/product-sitemap"))
        table_content = []
        for url in sitemap_urls:
            sitemap_prods = await get_data(session=session, url=url.text, request_delay=request_delay, headers=headers,
                                           proxies=proxies)
            soup_sitemap_prods = BeautifulSoup(sitemap_prods, "xml")
            all_tags = soup_sitemap_prods.find_all("loc", string=re.compile(f"{main_url}/producto/"))
            tasks = []
            max_active_tasks = 1
            for loc in all_tags:
                if len(tasks) >= max_active_tasks:
                    results = await asyncio.gather(*tasks)
                    table_content.extend(results)
                    tasks.clear()

                task = asyncio.create_task(parser_suellenmeski(session, loc.text, proxylist, multiplier, request_delay))
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
