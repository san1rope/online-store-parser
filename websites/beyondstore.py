
import csv
import re
import time
import gspread_asyncio
import lxml

from bs4 import BeautifulSoup
from tools import *


async def parser_beyondstore(session: aiocfscrape.CloudflareScraper, item_url: str, proxylist: list, multiplier: float,
                             request_delay: float = None, retry: int = 3):
    try:
        proxies = await get_proxy(proxylist)
        headers = {"user-agent": user_agent.generate_user_agent()}

        item_markup = await get_data(session, item_url, request_delay, headers, proxies)
        soup_item = BeautifulSoup(item_markup, "lxml")

        title = soup_item.find(class_="ProductName ProductCardProductName").text.strip().upper()
        image_link = f'https://www.beyondstore.fi{soup_item.find(class_="ProductImage")["href"]}'

        sizes = []

        try:
            full_size_price = soup_item.find(class_="ProductDiscountPrice").text.strip()
        except AttributeError:
            full_size_price = soup_item.find(class_="ProductCurrentPrice").text.strip()

        size_price = ""
        for ch in full_size_price:
            if ch.isdigit() or ch == ',':
                size_price += ch.replace(',', '.')

        size_price = float(size_price)

        try:
            selector = soup_item.find(class_="FormItem CheckWrap BuyFormVariationRadio").find(class_="Checks").find_all(
                "label", {"for": re.compile("BuyFormVariationRadio-")})
            for size in selector:
                size_name = size.text.split('/')[0].strip().upper().replace(' ', '_').replace(',', '.')

                sizes.append(f"{size_name} = {size_price}")
        except AttributeError:
            sizes.append(f"ONE_SIZE = {size_price}")

        return [title, image_link, item_url, '\n'.join(sizes)]
    except Exception:
        print(f"[-] {item_url} {traceback.format_exc()}")
        if retry:
            return await parser_beyondstore(session, item_url, proxylist, multiplier, request_delay, (retry - 1))


async def beyondstore(proxylist: list, proxy_all_values: list):
    main_url, file_name = "https://www.beyondstore.fi", "beyondstore"
    request_delay = 0.1
    headers = {"user-agent": user_agent.generate_user_agent()}

    agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

    primary_id = proxy_all_values[24][4]
    multiplier = float(proxy_all_values[2][7].replace(',', '.'))
    worksheet_id = proxy_all_values[24][3]

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
        all_products = soup_sitemap.find_all("loc", string=re.compile(f"{main_url}/product/"))

        tasks = []
        max_active_tasks = 10
        table_content = []
        for loc in all_products:
            if len(tasks) >= max_active_tasks:
                results = await asyncio.gather(*tasks)
                table_content.extend(results)
                tasks.clear()

            task = asyncio.create_task(parser_beyondstore(session, loc.text, proxylist, multiplier, request_delay))
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
