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


async def parser_nude_project(session: aiocfscrape.CloudflareScraper, item_url: str, proxylist: list, multiplier: float,
                              request_delay: float = None):
    proxies = await get_proxy(proxylist)
    headers = {"user-agent": user_agent.generate_user_agent()}

    try:
        markup = await get_data(session, item_url, request_delay, headers, proxies)
        soup_item = BeautifulSoup(markup, "lxml")

        title = soup_item.find(class_="ProductMeta__Title Heading u-h4").text.strip().upper()
        image_link = "https:" + soup_item.find("img", class_="Click-here Image--lazyLoad Image--fadeIn")["src"]

        sizes = []

        try:
            ul = soup_item.find(class_="SizeSwatchList HorizontalList HorizontalList--spacingTight").find_all("label")
            for label in ul:
                size_name = label.text.strip()
                if "inactive" in label["class"]:
                    size_price = "SOLD_OUT"
                else:
                    full_size_price = soup_item.find(class_="ProductMeta__Price Price Text--subdued u-h4").text.strip()
                    size_price = ""
                    for ch in full_size_price:
                        if ch.isdigit() or ch in [',', '.']:
                            size_price += ch

                    size_price = await fixed_price((float(size_price.replace(',', '.')) * multiplier), 2)

                sizes.append(f"{size_name} = {size_price}")
        except AttributeError:
            button = soup_item.find(class_=re.compile("ProductForm__AddToCart Button Button--secondary"))
            if button.get("disabled"):
                size_price = "SOLD_OUT"
            else:
                full_size_price = soup_item.find(class_="ProductMeta__Price Price Text--subdued u-h4").text.strip()
                size_price = ""
                for ch in full_size_price:
                    if ch.isdigit() or ch in [',', '.']:
                        size_price += ch

                size_price = await fixed_price((float(size_price.replace(',', '.')) * multiplier), 2)

            sizes.append(f"ONE_SIZE = {size_price}")

        return [title, image_link, item_url, '\n'.join(sizes)]
    except Exception:
        print(f"[-] {item_url} {traceback.format_exc()}")


async def nude_project(proxylist: list, proxy_all_values: list):
    main_url, file_name = "https://nude-project.com", "nude_project"
    request_delay = 0.1
    headers = {"user-agent": user_agent.generate_user_agent()}

    agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

    primary_id = proxy_all_values[21][4]
    multiplier = float(proxy_all_values[2][7].replace(',', '.'))
    worksheet_id = proxy_all_values[21][3]

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
        sitemap_urls = soup_sitemap.find_all("loc", string=re.compile(f"{main_url}/pt-pt/sitemap_products_"))
        table_content = []
        for url in sitemap_urls:
            sitemap_prods = await get_data(
                session, url=url.text, request_delay=request_delay, headers=headers, proxies=proxies)
            soup_sitemap_prods = BeautifulSoup(sitemap_prods, "xml")
            all_tags = soup_sitemap_prods.find_all("loc", string=re.compile(f"{main_url}/pt-pt/products/"))
            tasks = []
            max_active_tasks = 10
            for loc in all_tags:
                if len(tasks) >= max_active_tasks:
                    results = await asyncio.gather(*tasks)
                    table_content.extend(results)
                    tasks.clear()

                task = asyncio.create_task(parser_nude_project(session, loc.text, proxylist, multiplier, request_delay))
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
