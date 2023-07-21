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


async def parser_stashedsf(session: aiocfscrape.CloudflareScraper, item_url: str, proxylist: list, multiplier: float,
                           request_delay: float = None):
    try:
        headers = {"user-agent": user_agent.generate_user_agent()}
        proxies = await get_proxy(proxylist)

        item_markup = await get_data(session, item_url, request_delay, headers, proxies)
        soup_item = BeautifulSoup(item_markup, "lxml")

        title = soup_item.find(class_="product-details-product-title").text.strip()
        image_links = {}
        sizes = {}

        products_select = soup_item.find("select", {"data-section": re.compile("template--")}).find_all("option")
        try:
            product_colors_html = soup_item.find("div", {"class": "swatch clearfix",
                                                         "swatch-type": re.compile("Color")}).find(
                class_="swatch-items-wrapper clearfix").find_all(class_=re.compile("swatch-element"))
            product_colors = []
            for div in product_colors_html:
                data_value = div["data-value"]
                product_colors.append(data_value)
                sizes[data_value] = []

                try:
                    image_links[data_value] = "https:" + div["data-variant-image"]
                except KeyError:
                    try:
                        image_links[data_value] = \
                            soup_item.find("div", class_="wrapper-padded product-form-vue main-page-container").find(
                                "img", {"src": re.compile("//cdn.shopify.com/s/files")})['src'].replace('//',
                                                                                                        "https://")
                    except TypeError:
                        image_links[data_value] = "https://www.freeiconspng.com/uploads/no-image-icon-6.png"

            for option in products_select:
                full_size_text = option.text.strip()
                for color in product_colors:
                    if color in full_size_text:
                        size_text_list = full_size_text.split(' ')
                        if color in size_text_list:
                            size_text_list.remove(color)

                        if '/' in size_text_list:
                            size_text_list.remove("/")
                        size_name = ""
                        size_price = ""
                        flag = False
                        for el in size_text_list:
                            if (not flag) and el == "-":
                                flag = True
                            elif not flag:
                                size_name += f"{el} "
                            elif flag:
                                if "Sold out" in full_size_text:
                                    size_price = "SOLD_OUT"
                                    break
                                else:
                                    for ch in el:
                                        if ch.isdigit() or ch == ".":
                                            size_price += ch

                        size_name = size_name.strip().replace(' ', '_').upper().replace(
                            "DEFAULT_TITLE", "ONE_SIZE")
                        if "SOLD_OUT" not in size_price:
                            size_price = await fixed_price((float(size_price) * multiplier), 2)

                        sizes[color].append(f"{size_name.upper()} = {size_price}")
        except AttributeError:
            sizes["TEMP"] = []
            try:
                image_links["TEMP"] = soup_item.find(
                    "div", class_="wrapper-padded product-form-vue main-page-container").find(
                    "img", {"src": re.compile("//cdn.shopify.com/s/files")})['src'].replace('//', "https://")
            except TypeError:
                image_links["TEMP"] = "https://www.freeiconspng.com/uploads/no-image-icon-6.png"

            for option in products_select:
                full_size_text = option.text.strip()
                size_text_list = full_size_text.split(' ')
                size_name = ""
                size_price = ""
                flag = False
                for el in size_text_list:
                    if (not flag) and el == "-":
                        flag = True
                    elif not flag:
                        size_name += f"{el} "
                    elif flag:
                        if "Sold out" in full_size_text:
                            size_price = "SOLD_OUT"
                            break
                        else:
                            for ch in el:
                                if ch.isdigit() or ch == ".":
                                    size_price += ch

                size_name = size_name.strip().replace(' ', '_').upper().replace("DEFAULT_TITLE", "ONE_SIZE")
                if "SOLD_OUT" not in size_price:
                    size_price = await fixed_price((float(size_price) * multiplier), 2)

                sizes["TEMP"].append(f"{size_name} = {size_price}")

        if len(sizes) == 1:
            for size in sizes:
                return [title.upper(), image_links[size], item_url, '\n'.join(sizes[size])]
        else:
            for size in sizes:
                return [f"{title.upper()} {size.upper()}", image_links[size], item_url, '\n'.join(sizes[size])]
    except Exception:
        print(f"[-] {item_url} {traceback.format_exc()}")


async def stashedsf(proxylist: list, proxy_all_values: list):
    main_url, file_name = "https://stashedsf.com", "stashedsf_-_5_3"
    request_delay = 0.1
    headers = {"user-agent": user_agent.generate_user_agent()}

    agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

    primary_id = proxy_all_values[5][4]
    multiplier = float(proxy_all_values[4][7].replace(',', '.'))
    worksheet_id = proxy_all_values[5][3]

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

                task = asyncio.create_task(parser_stashedsf(session, loc.text, proxylist, multiplier, request_delay))
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
