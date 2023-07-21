import csv
import re
import time
import asyncio
import aiocfscrape
import gspread_asyncio
import lxml
import user_agent

from bs4 import BeautifulSoup
from tools import get_data, connect_to_gs, get_proxy, fixed_price, formatted_content, get_creds, upload_to_gs


async def parser_graduatestore(session: aiocfscrape.CloudflareScraper, page_url: str, proxylist: list,
                               multiplier: float, request_delay: float = None):
    headers = {"user-agent": user_agent.generate_user_agent()}
    proxies = await get_proxy(proxylist)
    request_counter = 0
    content = []

    page_markup = await get_data(session, page_url, request_delay, headers, proxies)
    soup_page = BeautifulSoup(page_markup, "lxml")

    all_products = soup_page.find_all("a", class_="nq-c-ProductListItem-img")
    for item in all_products:
        try:
            if request_counter >= 5:
                request_counter = 0
                proxies = await get_proxy(proxylist)
                headers = {"user-agent": user_agent.generate_user_agent()}

            item_url = item["href"]
            item_markup = await get_data(session, item_url, request_delay, headers, proxies)
            request_counter += 1
            soup_item = BeautifulSoup(item_markup, "lxml")

            title_brand = soup_item.find(class_="nq-c-Product-brand").text.strip()
            title_model = soup_item.find(class_="nq-c-Product-title").text.strip()
            title = f"{title_brand} {title_model}".upper()

            image_link = soup_item.find(class_="nq-c-ProductImages-main").find("img")["src"]

            sizes = []
            all_li = soup_item.find_all("li", class_=re.compile("nq-c-ProductVariants-item-radio swiper-slide"))
            for li in all_li:
                size_name = li.text.strip()
                if size_name == "OS":
                    size_name = "ONE_SIZE"

                data_qty = li["data-qty"]
                if data_qty == "0":
                    size_price = "SOLD_OUT"
                else:
                    size_price_full = soup_item.find(class_="nq-c-ProductPrices-current").text.strip()
                    size_price = ""
                    for ch in size_price_full:
                        if ch.isdigit() or ch == ".":
                            size_price += ch

                    size_price = await fixed_price((float(size_price) * multiplier), 2)

                sizes.append(f"{size_name} = {size_price}")

            content.append([title, image_link, item_url, '\n'.join(sizes)])
        except Exception:
            proxies = await get_proxy(proxylist)
            headers = {"user-agent": user_agent.generate_user_agent()}
            await asyncio.sleep(30)

    return content


async def graduatestore(proxylist: list, proxy_all_values: list):
    main_url, file_name = "https://graduatestore.fr/en", "graduatestore"
    request_delay = 0.1
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36 OPR/98.0.0.0"
    }

    agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

    primary_id = proxy_all_values[13][4]
    multiplier = float(proxy_all_values[2][7].replace(',', '.'))
    worksheet_id = proxy_all_values[13][3]

    worktable = await connect_to_gs(worksheet_id, agcm)
    worksheet = await worktable.get_worksheet(0)
    all_records = await worksheet.get_all_values()
    if len(all_records) > 0:
        all_records.pop(0)

    start = time.time()

    async with aiocfscrape.CloudflareScraper() as session:
        proxies = await get_proxy(proxylist)

        page_url_temp = f"{main_url}/14-shop?page=1"
        page_markup = await get_data(session, page_url_temp, request_delay, headers, proxies)
        soup_page_temp = BeautifulSoup(page_markup, "lxml")
        count_pages = int(soup_page_temp.find(class_="nq-c-Pagination-counter").text.strip().split('/')[-1])

        tasks = []
        table_content = []
        max_active_tasks = 10
        for page_number in range(1, count_pages + 1):
            page_url = f"{page_url_temp}/?page={page_number}"
            if len(tasks) >= max_active_tasks:
                results = await asyncio.gather(*tasks)
                table_content.extend(results[0])
                tasks.clear()

            task = asyncio.create_task(parser_graduatestore(session, page_url, proxylist, multiplier, request_delay))
            tasks.append(task)

        if len(tasks):
            results = await asyncio.gather(*tasks)
            table_content.extend(results[0])
            tasks.clear()

    with open(f"temp/{file_name}.csv", "w", encoding="utf-8", newline='') as file:
        writer = csv.writer(file)
        print(table_content)
        writer.writerows(await formatted_content(all_records, table_content, primary_id))

    end = time.time() - start
    print(f"{main_url} | Parsing completed! Took time: {end} seconds")

    await upload_to_gs(agcm, file_name, worksheet_id)
