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


async def parser_bdgastore(session: aiocfscrape.CloudflareScraper, item_url: str, proxylist: list, multiplier: float,
                           request_delay: float = None):
    try:
        headers = {"user-agent": user_agent.generate_user_agent()}
        proxies = await get_proxy(proxylist)
        cookies = {
            'localization': 'UA',
        }

        item_markup = await get_data(session, item_url, request_delay, headers, proxies, cookies=cookies)
        soup_item = BeautifulSoup(item_markup, "lxml")

        title_brand = soup_item.find(class_="product-single__vendor").text.strip().upper()
        title_model = soup_item.find(class_="product-single__title").text.strip().upper()
        title = f"{title_brand} {title_model}"

        image_link = "https:" + soup_item.find(
            "div", {"class": "product-single__media",
                    "data-zoom": re.compile("//bdgastore.com/cdn/shop/products/")})["data-zoom"]

        sizes = []
        try:
            fieldset = soup_item.find(
                "fieldset", {"class": "single-option-radio", "id": "ProductSelect-option-1"}).find_all("label")
        except AttributeError:
            try:
                fieldset = soup_item.find(
                    "fieldset", {"class": "single-option-radio", "id": "ProductSelect-option-0"}).find_all("label")
            except AttributeError:
                size_name = "ONE_SIZE"
                full_size_price = soup_item.find("span", {"id": "ProductPrice"}).text.strip()
                size_price = ""
                for ch in full_size_price:
                    if ch.isdigit() or ch == '.':
                        size_price += ch

                print([title, image_link, item_url, f"{size_name} = {size_price}"])
                return [title, image_link, item_url, f"{size_name} = {size_price}"]

        for el in fieldset:
            size_name = el.text.strip().upper()
            if size_name == "O/S":
                size_name = "ONE_SIZE"

            if "soldout" in el["class"]:
                size_price = "SOLD_OUT"
                sizes.append(f"{size_name} = {size_price}")
            else:
                select = soup_item.find("select", {"id": "ProductSelect--product-template"}).find_all("option")
                for option in select:
                    if size_name in option.text.split(" "):
                        try:
                            full_size_price = option.text.strip().split(' ')[-2]
                            size_price = ""
                            for ch in full_size_price:
                                if ch.isdigit() or ch == ".":
                                    size_price += ch

                            size_price = await fixed_price((float(size_price) * multiplier), 2)
                        except ValueError:
                            size_price = "SOLD_OUT"

                        sizes.append(f"{size_name} = {size_price}")
                        break

        if not sizes:
            sizes.append("ONE_SIZE = SOLD_OUT")

        print([title, image_link, item_url, '\n'.join(sizes)])
        return [title, image_link, item_url, '\n'.join(sizes)]
    except Exception:
        print(f"[-] {item_url} {traceback.format_exc()}")


async def bdgastore(proxylist: list, proxy_all_values: list):
    main_url, file_name = "https://bdgastore.com", "bdgastore"
    request_delay = 1
    headers = {"user-agent": user_agent.generate_user_agent()}

    agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

    primary_id = proxy_all_values[12][4]
    multiplier = float(proxy_all_values[3][7].replace(',', '.'))
    worksheet_id = proxy_all_values[12][3]

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
                    parser_bdgastore(session, loc.text, proxylist, multiplier, request_delay))
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
