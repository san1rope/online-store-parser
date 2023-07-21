import csv
import re
import time
import traceback

import gspread_asyncio

from concurrent.futures import ProcessPoolExecutor
from bs4 import BeautifulSoup
from tools import *


async def parser_homegrownskateshop(item_url: str, proxylist: list, multiplier: float, request_delay: float = None,
                                    retry: int = 3):
    proxies = await get_proxy(proxylist)
    item_markup = await get_data_selenium(item_url, proxies, request_delay)
    soup_item = BeautifulSoup(item_markup, "lxml")
    try:
        all_products = soup_item.find(class_="ec-store__content-wrapper ec-store__content-wrapper--wide")
        if all_products:
            return None

        title = soup_item.find(class_="product-details__product-title ec-header-h3").text.strip().upper()
        image_link = soup_item.find(class_="details-gallery__picture details-gallery__photoswipe-index-0")["src"]

        sizes = []
        try:
            selector = soup_item.find(
                class_="product-details-module details-product-option details-product-option--size details-product-option--Size").find(
                class_='product-details-module__content').find_all(
                class_=re.compile("form-control--checkbox-button form-control details-product-option--"))
            for size in selector:
                if "disable" in str(size["class"]):
                    size_price = "SOLD_OUT"
                else:
                    full_size_price = soup_item.find(
                        class_="details-product-price__value ec-price-item notranslate").text.strip()
                    size_price = ""
                    for ch in full_size_price:
                        if ch.isdigit() or ch == '.':
                            size_price += ch

                    size_price = await fixed_price((float(size_price) * multiplier), 2)

                size_name = size.text.strip().replace(' ', '_')

                sizes.append(f"{size_name} = {size_price}")
        except AttributeError:
            try:
                selector = soup_item.find(
                    class_="product-details-module details-product-option details-product-option--radio details-product-option--Size").find(
                    class_="product-details-module__content").find_all(
                    class_=re.compile(
                        "form-control--radio form-control form-control--flexible details-product-option--"))
                for size in selector:
                    size_price = size.find("div", class_=re.compile("form-control__radio-wrap"))
                    if "disable" in str(size_price):
                        size_price = "SOLD_OUT"
                    else:
                        full_size_price = soup_item.find(
                            class_="details-product-price__value ec-price-item notranslate").text
                        size_price = ""
                        for ch in full_size_price:
                            if ch.isdigit() or ch == '.':
                                size_price += ch

                        size_price = await fixed_price((float(size_price) * multiplier), 2)

                    size_name = size.text.strip()

                    sizes.append(f"{size_name} = {size_price}")
            except AttributeError:
                try:
                    size_name = "ONE_SIZE"

                    button = soup_item.find(
                        class_=re.compile("form-control form-control--button form-control--large form-control--primary form-control--flexible form-control--animated")).find(
                        class_="form-control__button-text").text.strip()
                    if "Add to Bag" not in button:
                        size_price = "SOLD_OUT"
                    else:
                        full_size_price = soup_item.find(
                            class_="details-product-price__value ec-price-item notranslate").text
                        size_price = ""
                        for ch in full_size_price:
                            if ch.isdigit() or ch == '.':
                                size_price += ch

                        size_price = await fixed_price((float(size_price) * multiplier), 2)

                    sizes.append(f"{size_name} = {size_price}")
                except AttributeError:
                    sizes.append("ONE_SIZE = SOLD_OUT")
        return [title, image_link, item_url, '\n'.join(sizes)]
    except Exception:
        print(f"[-] {item_url} {traceback.format_exc()} {proxies} {item_markup}")
        if retry:
            return await parser_homegrownskateshop(item_url, proxylist, multiplier, request_delay, (retry - 1))


async def flow_distribution(links: list, max_active_tasks: int, proxylist: list, multiplier: float,
                            request_delay: float):
    tasks = []
    table_content = []
    for url in links:
        if len(tasks) >= max_active_tasks:
            results = await asyncio.gather(*tasks)
            table_content.extend(results)
            tasks.clear()

        task = asyncio.create_task(parser_homegrownskateshop(url, proxylist, multiplier, request_delay))
        tasks.append(task)

    if len(tasks):
        results = await asyncio.gather(*tasks)
        table_content.extend(results)
        tasks.clear()

    return table_content


async def homegrownskateshop(proxylist: list, proxy_all_values: list):
    main_url, file_name = "https://homegrownskateshop.com", "homegrownskateshop"
    request_delay = 3

    agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

    primary_id = proxy_all_values[9][4]
    multiplier = float(proxy_all_values[4][7].replace(',', '.'))
    worksheet_id = proxy_all_values[9][3]

    worktable = await connect_to_gs(worksheet_id, agcm)
    worksheet = await worktable.get_worksheet(0)
    all_records = await worksheet.get_all_values()
    if len(all_records) > 0:
        all_records.pop(0)

    start = time.time()

    async with aiocfscrape.CloudflareScraper() as session:
        available_proxies = await check_proxy(proxylist, session, main_url)

        proxies = await get_proxy(available_proxies)
        max_active_tasks = 1
        links_for_process = 10

        sitemap_url = f"{main_url}/sitemap.xml"
        sitemap_markup = await get_data_selenium(sitemap_url, proxies, request_delay)
        soup_sitemap = BeautifulSoup(sitemap_markup, "xml")
        all_loc = soup_sitemap.find_all("loc", string=re.compile(f"{main_url}/products/"))
        links = []
        temp_links = []
        for loc in all_loc:
            if loc.text == "https://homegrownskateshop.com/products/":
                continue

            temp_links.append(loc.text)

            if len(temp_links) >= links_for_process:
                links.append(temp_links)
                temp_links = []

        if temp_links:
            links.append(temp_links)

        futures = []
        loop = asyncio.get_running_loop()
        with ProcessPoolExecutor(max_workers=1) as executor:
            for task in links:
                future = loop.run_in_executor(executor, wrapper, flow_distribution, task, max_active_tasks, proxylist,
                                              multiplier, request_delay)
                futures.append(future)

    table_content = await get_result(futures, 10)
    with open(f"temp/{file_name}.csv", "w", encoding="utf-8", newline='') as file:
        writer = csv.writer(file)
        writer.writerows(await formatted_content(all_records, table_content, primary_id))

    end = time.time() - start
    print(f"{main_url} | Parsing completed! Took time: {end} seconds")

    await upload_to_gs(agcm, file_name, worksheet_id)
