import csv
import time
import asyncio
import aiocfscrape
import gspread_asyncio
import lxml
import user_agent
import re

from bs4 import BeautifulSoup
from tools import get_data, connect_to_gs, get_proxy, fixed_price, formatted_content, get_creds, upload_to_gs, \
    check_proxy


async def parser_lishop(session: aiocfscrape.CloudflareScraper, page_url: str, proxylist: list, multiplier: float,
                        request_delay: float = None):
    content = []
    request_counter = 0
    proxies = await get_proxy(proxylist)
    headers = {"user-agent": user_agent.generate_user_agent()}

    page_markup = await get_data(session, page_url, request_delay, headers, proxies)
    request_counter += 1
    soup_page = BeautifulSoup(page_markup, "lxml")

    all_products = soup_page.find_all(class_="image")
    for item in all_products:
        if request_counter >= 3:
            proxies = await get_proxy(proxylist)
            request_counter = 0
            headers = {"user-agent": user_agent.generate_user_agent()}

        item_url = item.find("a")["href"]
        item_markup = await get_data(session, item_url, request_delay, headers, proxies)
        request_counter += 1
        soup_item = BeautifulSoup(item_markup, "lxml")

        title = soup_item.find(class_="name-block").text.strip().upper()
        image_link = soup_item.find(class_="img0 one-img")["src"]

        sizes = []
        full_size_price = soup_item.find(class_="price").text.strip()
        size_price = ""
        for ch in full_size_price:
            if ch.isdigit() or ch == '.':
                size_price += ch

        size_price = await fixed_price((float(size_price) * multiplier), 2)

        selector = soup_item.find_all("div", {"class": re.compile("radio-block with-attr"), "data-type": "US"})
        if not selector:
            selector = soup_item.find_all(class_=re.compile("radio-block with-attr"))
            if not selector:
                content.append([title, image_link, item_url, "ONE_SIZE = SOLD_OUT"])
                continue

        for size in selector:
            size_name = size.text.strip().replace(' ', '_')
            if size_name == "OS":
                size_name = "ONE_SIZE"

            sizes.append(f"{size_name} = {size_price}")

        content.append([title, image_link, item_url, '\n'.join(sizes)])

    return content


async def lishop(proxylist: list, proxy_all_values: list):
    main_url, file_name = "https://lishop.store", "lishop"
    request_delay = 0.1
    headers = {"user-agent": user_agent.generate_user_agent()}

    agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

    primary_id = proxy_all_values[20][4]
    multiplier = float(proxy_all_values[3][7].replace(',', '.'))
    worksheet_id = proxy_all_values[20][3]

    worktable = await connect_to_gs(worksheet_id, agcm)
    worksheet = await worktable.get_worksheet(0)
    all_records = await worksheet.get_all_values()
    if len(all_records) > 0:
        all_records.pop(0)

    start = time.time()

    async with aiocfscrape.CloudflareScraper() as session:
        table_content = []

        available_proxies = await check_proxy(proxylist, session, main_url)
        section_urls = [f"{main_url}/male", f"{main_url}/female", f"{main_url}/acsessuare"]
        for sect_url in section_urls:
            proxies = await get_proxy(available_proxies)
            sect_markup = await get_data(session, sect_url, request_delay, headers, proxies)
            soup_sect = BeautifulSoup(sect_markup, "lxml")

            pagination = int(soup_sect.find(class_="pag-btn").find("a")["href"].split("=")[-1])
            tasks = []
            max_active_tasks = 10
            for page in range(1, pagination + 1):
                url = f"{sect_url}/?page={page}"
                if len(tasks) >= max_active_tasks:
                    results = await asyncio.gather(*tasks)
                    table_content.extend(results[0])
                    tasks.clear()

                task = asyncio.create_task(parser_lishop(session, url, available_proxies, multiplier, request_delay))
                tasks.append(task)

            if len(tasks):
                results = await asyncio.gather(*tasks)
                table_content.extend(results[0])
                tasks.clear()

    with open(f"temp/{file_name}.csv", "w", encoding="utf-8", newline='') as file:
        writer = csv.writer(file)
        writer.writerows(await formatted_content(all_records, table_content, primary_id))

    end = time.time() - start
    print(f"{main_url} | Parsing completed! Took time: {end} seconds")

    await upload_to_gs(agcm, file_name, worksheet_id)
