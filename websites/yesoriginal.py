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


async def parser_yesoriginal(session: aiocfscrape.CloudflareScraper, page_url: str, proxylist: list, multiplier: float,
                             request_delay: float = None):
    try:
        content = []
        request_counter = 0
        proxies = await get_proxy(proxylist)
        headers = {"user-agent": user_agent.generate_user_agent()}
        page_markup = await get_data(session, page_url, request_delay, headers, proxies)
        request_counter += 1
        soup_page = BeautifulSoup(page_markup, "lxml")

        all_products = soup_page.find("div", {"id": "content"}).find_all(class_="product-thumb")
        for item in all_products:
            if request_counter >= 5:
                request_counter = 0
                proxies = await get_proxy(proxylist)
                headers = {"user-agent": user_agent.generate_user_agent()}

            try:
                item_url = item.find(class_="image").find("a")['href']
                item_markup = await get_data(session, item_url, request_delay, headers, proxies)
                request_counter += 1
                soup_item = BeautifulSoup(item_markup, "lxml")

                title = soup_item.find(class_="pull-left info-left col-sm-7 nopad").find("h1").text.strip().upper()
                try:
                    image_link = soup_item.find("div", {"id": "image-additional"}).find("img")['src']
                except AttributeError:
                    image_link = soup_item.find(class_="item").find("img")['src']

                sizes = []

                full_size_price = soup_item.find(class_="price-cont__price").text.strip()
                size_price = ""
                for ch in full_size_price:
                    if ch.isdigit() or ch == ".":
                        size_price += ch

                size_price = await fixed_price((float(size_price) * multiplier), 2)

                all_sizes = soup_item.find("div", {"id": re.compile("input-option")}).find_all("div")
                for size in all_sizes:
                    size_name = size.text.strip().upper()
                    if size_name == "MISC":
                        size_name = "ONE_SIZE"

                    sizes.append(f"{size_name} = {size_price}")

                content.append([title, image_link, item_url, '\n'.join(sizes)])
            except Exception:
                print(f"[-] {item_url} {traceback.format_exc()}")
                proxies = await get_proxy(proxylist)
                await asyncio.sleep(15)

        return content
    except Exception:
        print(f"[-] {page_url} {traceback.format_exc()}")


async def yesoriginal(proxylist: list, proxy_all_values: list):
    main_url, file_name = "https://yesoriginal.com.ua/uk", "yesoriginal_-_8_3"
    request_delay = 0.1
    headers = {"user-agent": user_agent.generate_user_agent()}

    agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

    primary_id = proxy_all_values[8][4]
    multiplier = float(proxy_all_values[3][7].replace(',', '.'))
    worksheet_id = proxy_all_values[8][3]

    worktable = await connect_to_gs(worksheet_id, agcm)
    worksheet = await worktable.get_worksheet(0)
    all_records = await worksheet.get_all_values()
    if len(all_records) > 0:
        all_records.pop(0)

    start = time.time()

    async with aiocfscrape.CloudflareScraper() as session:
        proxies = await get_proxy(proxylist)
        categories = [f"{main_url}/obuv", f"{main_url}/odezhda", f"{main_url}/verhnyaya-odezhda",
                      f"{main_url}/aksessuary"]

        table_content = []
        max_active_tasks = 10
        for category_url in categories:
            pag_markup = await get_data(session, category_url, request_delay, headers, proxies)
            soup_pag = BeautifulSoup(pag_markup, "lxml")
            count_pages = int(
                soup_pag.find("ul", class_="pagination").find_all("li")[-1].find("a")["href"].split('-')[-1])

            tasks = []
            for page in range(1, count_pages + 1):
                url = f"{category_url}/page-{page}"
                if len(tasks) >= max_active_tasks:
                    results = await asyncio.gather(*tasks)
                    table_content.extend(results)
                    tasks.clear()

                task = asyncio.create_task(
                    parser_yesoriginal(session, url, proxylist, multiplier, request_delay))
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
