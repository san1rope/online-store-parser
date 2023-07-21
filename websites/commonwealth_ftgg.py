import csv
import re
import time
import asyncio
import traceback
import user_agent
import aiocfscrape
import gspread_asyncio
import lxml

from bs4 import BeautifulSoup
from tools import get_data, connect_to_gs, get_proxy, fixed_price, formatted_content, get_creds, upload_to_gs


async def parser_commonwealth_ftgg(session: aiocfscrape.CloudflareScraper, item_url: str, proxylist: list,
                                   multiplier: float, request_delay: float = None):
    try:
        headers = {"user-agent": user_agent.generate_user_agent()}
        proxies = await get_proxy(proxylist)
        item_markup = await get_data(session, item_url, request_delay, headers, proxies)
        soup_item = BeautifulSoup(item_markup, "lxml")

        title = soup_item.find(class_="product_name").text.strip().upper()
        image_link = "https:" + soup_item.find(class_="image__container").find("img")["data-src"]
        sizes = []

        div_selector = soup_item.find("form", {"id": re.compile("product_form_")}).find(class_="swatch clearfix").find_all("div", class_=re.compile("swatch-element"))
        for size in div_selector:
            size_name = size["value"].replace(" ", "_").upper()
            if size_name == "O/S" or size_name == "OS":
                size_name = "ONE_SIZE"

            size_class = size["class"]
            if "soldout" in size_class:
                size_price = "SOLD_OUT"
            else:
                product_price = soup_item.find(
                    class_=re.compile("price__container price__container--display-price-false")).find(
                    class_="current_price").find(class_="money").text.strip()
                size_price = ""
                for ch in product_price:
                    if ch.isdigit() or ch == ".":
                        size_price += ch
                size_price = await fixed_price((float(size_price) * multiplier), 2)

            sizes.append(f"{size_name} = {size_price}")

        return [title, image_link, item_url, '\n'.join(sizes)]
    except Exception:
        print(f"[-] {item_url} {traceback.format_exc()}")


async def commonwealth_ftgg(proxylist: list, proxy_all_values: list):
    main_url, file_name = "https://commonwealth-ftgg.com", "commonwealth_ftgg"
    request_delay = 0.1
    headers = {"user-agent": user_agent.generate_user_agent()}

    agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

    primary_id = proxy_all_values[10][4]
    multiplier = float(proxy_all_values[4][7].replace(',', '.'))
    worksheet_id = proxy_all_values[10][3]

    worktable = await connect_to_gs(worksheet_id, agcm)
    worksheet = await worktable.get_worksheet(0)
    all_records = await worksheet.get_all_values()
    if len(all_records) > 0:
        all_records.pop(0)

    start = time.time()

    async with aiocfscrape.CloudflareScraper() as session:
        proxies = await get_proxy(proxylist)
        sitemap_url = f"{main_url}/sitemap.xml"
        sitemap_markup = await get_data(session, sitemap_url, request_delay, headers, proxies)
        soup_sitemap = BeautifulSoup(sitemap_markup, "xml")
        sitemap_urls = soup_sitemap.find_all("loc", string=re.compile(f"{main_url}/sitemap_products_"))
        table_content = []
        for url in sitemap_urls:
            sitemap_prods = await get_data(session, url.text, request_delay, headers, proxies)
            soup_sitemap_prods = BeautifulSoup(sitemap_prods, "xml")
            all_tags = soup_sitemap_prods.find_all("loc", string=re.compile(f"{main_url}/products/"))
            tasks = []
            max_active_tasks = 10
            for loc in all_tags:
                if len(tasks) >= max_active_tasks:
                    results = await asyncio.gather(*tasks)
                    table_content.extend(results)
                    tasks.clear()

                task = asyncio.create_task(
                    parser_commonwealth_ftgg(session, loc.text, proxylist, multiplier, request_delay))
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
