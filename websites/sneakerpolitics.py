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


async def parser_sneakerpolitics(session: aiocfscrape.CloudflareScraper, item_url: str, proxylist: list,
                                 multiplier: float, request_delay: float = None, retry: int = 3):
    try:
        headers = {"user-agent": user_agent.generate_user_agent()}
        proxies = await get_proxy(proxylist)
        item_markup = await get_data(session, item_url, request_delay, headers, proxies)
        soup_item = BeautifulSoup(item_markup, "lxml")

        title = soup_item.find(class_="product__title").text.strip().replace("  ", " ").upper()
        image_link = "https:" + soup_item.find(class_="product-carousel w-full").find("img")["src"]

        sizes = []

        item_values = []
        select_html = soup_item.find("select", {"id": re.compile("MasterSelect-template--")}).find_all("option")
        for el in select_html:
            item_values.append(el["value"])

        for value in item_values:
            variant_url = f"{item_url}?variant={value}"
            variant_markup = await get_data(session, variant_url, request_delay, headers, proxies)
            soup_variant = BeautifulSoup(variant_markup, "lxml")
            try:
                fieldset_html = soup_variant.find("fieldset", class_="js product-form__input").find_all("input")
                for el in fieldset_html:
                    size_name, size_price = "", ""
                    if 'checked=""' in str(el):
                        size_name = el.find_next().text.strip().upper().replace(' ', '_')

                        try:
                            if el["class"]:
                                size_price = "SOLD_OUT"
                        except KeyError:
                            size_price_full = soup_variant.find(class_="price__regular").find(
                                class_="price-item price-item--regular").text.strip()
                            for ch in size_price_full:
                                if ch.isdigit() or ch == '.':
                                    size_price += ch

                            size_price = await fixed_price((float(size_price) * multiplier), 2)

                        sizes.append(f"{size_name} = {size_price}")
                        break
            except AttributeError:
                size_name = "ONE_SIZE"
                size_price = ""
                option = select_html[0]
                if "disabled" in option:
                    size_price = "SOLD_OUT"
                else:
                    size_price_full = soup_variant.find(class_="product-price__current").text.strip()
                    for ch in size_price_full:
                        if ch.isdigit() or ch == ".":
                            size_price += ch

                    size_price = await fixed_price((float(size_price) * multiplier), 2)

                sizes.append(f"{size_name} = {size_price}")

        return [title, image_link, item_url, '\n'.join(sizes)]
    except Exception:
        print(f"[-] {item_url} {traceback.format_exc()}")
        if retry:
            return await parser_sneakerpolitics(session, item_url, proxylist, multiplier, request_delay, (retry - 1))


async def sneakerpolitics(proxylist: list, proxy_all_values: list):
    main_url, file_name = "https://sneakerpolitics.com", "sneakerpolitics"
    request_delay = 0.1
    headers = {"user-agent": user_agent.generate_user_agent()}

    agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

    primary_id = proxy_all_values[6][4]
    multiplier = float(proxy_all_values[4][7].replace(',', '.'))
    worksheet_id = proxy_all_values[6][3]

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
                    parser_sneakerpolitics(session, loc.text, proxylist, multiplier, request_delay))
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
