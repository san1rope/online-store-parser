import asyncio
import csv
import re
import time
import json
import gspread_asyncio
import lxml

from bs4 import BeautifulSoup
from tools import *


async def parser_dr_adams(session: aiocfscrape.CloudflareScraper, item_url: str, proxylist: list, multiplier: float,
                          request_delay: float = None, retry: int = 3):
    try:
        proxies = await get_proxy(proxylist)
        headers = {"user-agent": user_agent.generate_user_agent()}

        item_markup = await get_data(session, item_url, request_delay, headers, proxies)
        soup_item = BeautifulSoup(item_markup, "lxml")

        title_brand = soup_item.find("h2", {"id": "commodity-show-brand"}).text.strip().upper()
        title_model = soup_item.find("h1", {"id": "commodity-show-title"}).text.strip().upper()
        title = f"{title_brand} {title_model}"

        image_link = soup_item.find(class_="popup").find("img")["src"]

        sizes = []

        full_size_price = soup_item.find("span", class_="price").text.strip()
        size_price = ""
        for ch in full_size_price:
            if ch.isdigit() or ch == ",":
                size_price += ch.replace(',', '.')

        size_price = float(size_price)

        try:
            script = soup_item.find("script", string=re.compile("commodity-show-images")).text.strip()
            script_sizes = "{" + script[(script.find('"items": {')):(script.find('}, "pids"') + 1)].strip() + "}"
            all_sizes = json.loads(script_sizes)
            for size_key in all_sizes["items"]:
                size = all_sizes["items"][size_key]
                size_name = size["params"][-1]

                if int(size["availability"]):
                    sizes.append(f"{size_name.replace(' ', '_')} = {size_price}")
                else:
                    sizes.append(
                        f"{size_name.replace('Udsolgt - påmind mig', '').strip().replace(' ', '_')} = SOLD_OUT")
        except AttributeError:
            sizes.append(f"ONE_SIZE = {size_price}")

        return [title, image_link, item_url, '\n'.join(sizes)]
    except Exception:
        print(f"[-] {item_url} {traceback.format_exc()}")
        if retry:
            return await parser_dr_adams(session, item_url, proxylist, multiplier, request_delay, (retry - 1))


async def dr_adams(proxylist: list, proxy_all_values: list):
    main_url, file_name = "https://www.dr-adams.dk", "dr_adams"
    request_delay = 0.1
    headers = {"user-agent": user_agent.generate_user_agent()}

    agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

    primary_id = proxy_all_values[22][4]
    multiplier = float(proxy_all_values[5][7].replace(',', '.'))
    worksheet_id = proxy_all_values[22][3]

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
        all_products = soup_sitemap.find_all("loc", string=re.compile(f"{main_url}/vare/"))
        tasks = []
        max_active_tasks = 10
        table_content = []
        for loc in all_products:
            if len(tasks) >= max_active_tasks:
                results = await asyncio.gather(*tasks)
                table_content.extend(results)
                tasks.clear()

            task = asyncio.create_task(parser_dr_adams(session, loc.text, proxylist, multiplier, request_delay))
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
