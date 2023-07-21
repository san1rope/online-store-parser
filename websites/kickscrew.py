import csv
import re
import time
import json
import gspread_asyncio
import lxml

from bs4 import BeautifulSoup
from tools import *


async def parser_kickscrew(session: aiocfscrape.CloudflareScraper, item_url: str, proxylist: list, multiplier: float,
                           request_delay: float = None, retry: int = 3):
    try:
        proxies = await get_proxy(proxylist)
        headers = {"user-agent": user_agent.generate_user_agent()}
        cookies = {
            'localization': 'US',
            'cart_currency': 'USD'
        }

        item_markup = await get_data(session, item_url, request_delay, headers, proxies, cookies=cookies)
        soup_item = BeautifulSoup(item_markup, "lxml")

        title = soup_item.find(
            class_="product-area__details__title product-detail__gap-sm h2").text.strip().upper().split("  ")[0]

        try:
            image_link = "https:" + soup_item.find("img", class_="rimage__image")["src"]
        except KeyError:
            image_link = "https://www.freeiconspng.com/uploads/no-image-icon-6.png"

        sizes = []

        size_names_json = soup_item.find("script", {"id": re.compile("cc-product-json-")}).text.strip()
        size_names_dict = json.loads(size_names_json)["variants"]

        all_size_names = []
        try:
            select = soup_item.find("select", {"id": re.compile("option-size-")}).find_all("option")
            for option in select:
                all_size_names.append(option.text.split("|")[0].strip())

            for variant in size_names_dict:
                full_size_name = variant["option2"]
                size_name = ""
                for size in all_size_names:
                    if size.replace(' ', '') in full_size_name:
                        size_name = size.replace(' ', '_')

                if variant["available"]:
                    size_price_int = str(variant["price"])[:-2]
                    size_price_float = str(variant["price"])[-2:]
                    size_price = float(f"{size_price_int}.{size_price_float}")

                    size_price = await fixed_price((float(size_price) * multiplier), 2)
                else:
                    size_price = "SOLD_OUT"

                sizes.append(f"{size_name} = {size_price}")
        except AttributeError:
            if size_names_dict[0]["available"]:
                size_price_int = str(size_names_dict[0]["price"])[:-2]
                size_price_float = str(size_names_dict[0]["price"])[-2:]
                size_price = float(f"{size_price_int}.{size_price_float}")

                size_price = await fixed_price((float(size_price) * multiplier), 2)
            else:
                size_price = "SOLD_OUT"

            sizes.append(f"ONE_SIZE = {size_price}")

        return [title, image_link, item_url, '\n'.join(sizes)]
    except Exception:
        print(f"[-] {item_url} {traceback.format_exc()}")
        if retry:
            return await parser_kickscrew(session, item_url, proxylist, multiplier, request_delay, (retry - 1))


async def kickscrew(proxylist: list, proxy_all_values: list):
    main_url, file_name = "https://www.kickscrew.com", "kickscrew"
    request_delay = 1
    headers = {"user-agent": user_agent.generate_user_agent()}

    agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

    primary_id = proxy_all_values[31][4]
    multiplier = float(proxy_all_values[4][7].replace(',', '.'))
    worksheet_id = proxy_all_values[31][3]

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
            max_active_tasks = 1
            for loc in all_tags:
                if len(tasks) >= max_active_tasks:
                    results = await asyncio.gather(*tasks)
                    table_content.extend(results)
                    tasks.clear()

                task = asyncio.create_task(parser_kickscrew(session, loc.text, proxylist, multiplier, request_delay))
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
