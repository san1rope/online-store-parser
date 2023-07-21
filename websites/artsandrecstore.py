import csv
import re
import time
import asyncio
import traceback
import aiocfscrape
import gspread_asyncio
import lxml
import user_agent

from bs4 import BeautifulSoup
from tools import get_data, connect_to_gs, get_proxy, fixed_price, formatted_content, get_creds, upload_to_gs


async def parser_artsandrecstore(session: aiocfscrape.CloudflareScraper, item_url: str, proxylist: list,
                                 multiplier: float, request_delay: float = None):
    try:
        headers = {"user-agent": user_agent.generate_user_agent()}
        proxies = await get_proxy(proxylist)
        item_markup = await get_data(session, item_url, request_delay, headers, proxies)
        soup_item = BeautifulSoup(item_markup, "lxml")

        title = soup_item.find(class_="title").text.strip().upper()
        try:
            image_link = "https:" + soup_item.find(class_="show-gallery")["href"]
        except TypeError:
            image_link = "https://www.freeiconspng.com/uploads/no-image-icon-6.png"

        sizes = []

        select = soup_item.find("select", class_="original-selector").find_all(
            "option", {"value": re.compile(r'\d')})
        for option in select:
            value = option["value"]
            # size_name = option.text.strip().replace(' ', '_').upper()
            size_name = option.text.strip().upper()
            if "MEN" in size_name and "WOMEN" in size_name:
                size_name_list = size_name.split("MEN")
                size_name = f"US_{size_name_list[0].strip()}"
            elif "DEFAULT_TITLE" in size_name:
                size_name = "ONE_SIZE"
            else:
                size_name = size_name.replace(' ', '_')

            if option["data-stock"] == "out":
                size_price = "SOLD_OUT"
            else:
                size_price_markup = soup_item.find(class_="variant-visibility-area").find(
                    "script", {"data-variant-id": value}).text
                soup_size_price = BeautifulSoup(size_price_markup, "lxml")
                size_price_full = soup_size_price.find(class_="current-price theme-money").text.strip()
                size_price = ""
                for ch in size_price_full:
                    if ch.isdigit() or ch == ".":
                        size_price += ch

                size_price = await fixed_price((float(size_price) * multiplier), 2)

            sizes.append(f"{size_name} = {size_price}")

        return [title, image_link, item_url, '\n'.join(sizes)]
    except Exception:
        print(f"[-] {item_url} {traceback.format_exc()}")


async def artsandrecstore(proxylist: list, proxy_all_values: list):
    main_url, file_name = "https://artsandrecstore.com", "artsandrecstore"
    request_delay = 0.1
    headers = {"user-agent": user_agent.generate_user_agent()}

    agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

    primary_id = proxy_all_values[11][4]
    multiplier = float(proxy_all_values[4][7].replace(',', '.'))
    worksheet_id = proxy_all_values[11][3]

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
                    parser_artsandrecstore(session, loc.text, proxylist, multiplier, request_delay))
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
