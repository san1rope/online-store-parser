import csv
import re
import time
import gspread_asyncio
import lxml
import json

from bs4 import BeautifulSoup
from tools import *


async def parser_footshop(session: aiocfscrape.CloudflareScraper, item_url: str, proxylist: list, multiplier: float,
                           request_delay: float = None, retry: int = 3):
    try:
        headers = {"user-agent": user_agent.generate_user_agent()}
        proxies = await get_proxy(proxylist)

        item_markup = await get_data(session, item_url, request_delay, headers, proxies)
        soup_item = BeautifulSoup(item_markup, "lxml")

        title = soup_item.find("h1", {"class": re.compile("Headline_"), "itemprop": "name"}).text.strip().upper()
        image_link = soup_item.find("img", {"itemprop": "image"})["src"]

        sizes = []

        start_index = item_markup.find('<!--{"data":{"is_admin_view":false')
        end_index = item_markup.find('.html"}}--></script>')
        data_json = item_markup[start_index:end_index].replace("<!--", "") + '.html"}}'
        data_dict = json.loads(data_json)

        all_sizes = data_dict["data"]["product_data"]["availability"]["data"]
        for size in all_sizes:
            size_name_list = list(size["size_values_en"])
            size_name = size["size_values_en"][size_name_list[0]].upper()

            if size_name == "UNIVERSAL":
                size_name = "ONE_SIZE"

            if size["quantity_online"] == 0:
                size_price = "SOLD_OUT"
            else:
                size_price = float(data_dict["data"]["product_data"]["price"]["value"]) * multiplier

            sizes.append(f"{size_name} = {size_price}")

        return [title, image_link, item_url, '\n'.join(sizes)]
    except Exception:
        print(f"[-] {item_url} {traceback.format_exc()}")
        if retry:
            return await parser_footshop(session, item_url, proxylist, multiplier, request_delay, (retry - 1))


async def footshop(proxylist: list, proxy_all_values: list):
    main_url, file_name = "https://www.footshop.com", "footshop"
    request_delay = 0.1
    headers = {"user-agent": user_agent.generate_user_agent()}

    agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

    primary_id = proxy_all_values[28][4]
    multiplier = float(proxy_all_values[4][7].replace(',', '.'))
    worksheet_id = proxy_all_values[28][3]

    worktable = await connect_to_gs(worksheet_id, agcm)
    worksheet = await worktable.get_worksheet(0)
    all_records = await worksheet.get_all_values()
    if len(all_records) > 0:
        all_records.pop(0)

    start = time.time()

    async with aiocfscrape.CloudflareScraper() as session:
        proxies = await get_proxy(proxylist)
        sitemap_url = f"{main_url}/en/download/sitemaps/12_en_0_sitemap.xml"
        sitemap_markup = await get_data(session=session, proxies=proxies, headers=headers, request_delay=request_delay,
                                        url=sitemap_url)
        soup_sitemap = BeautifulSoup(sitemap_markup, "xml")
        all_urls = soup_sitemap.find_all("url")
        all_products = []
        for url in all_urls:
            if url.find("priority").text == "0.9":
                all_products.append(url.find("loc"))

        tasks = []
        table_content = []
        max_active_tasks = 10
        for loc in all_products:
            if len(tasks) >= max_active_tasks:
                results = await asyncio.gather(*tasks)
                table_content.extend(results)
                tasks.clear()

            task = asyncio.create_task(parser_footshop(session, loc.text, proxylist, multiplier, request_delay))
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
