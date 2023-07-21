import csv
import re
import json
import time
import gspread_asyncio
import lxml

from bs4 import BeautifulSoup
from tools import *


async def parser_nittygrittystore(session: aiocfscrape.CloudflareScraper, item_url: str, proxylist: list,
                                  multiplier: float, request_delay: float = None, retry: int = 3):
    try:
        proxies = await get_proxy(proxylist)
        headers = {"user-agent": user_agent.generate_user_agent()}

        item_markup = await get_data(session, item_url, request_delay, headers, proxies)
        if not item_markup:
            return

        soup_item = BeautifulSoup(item_markup, "lxml")

        title_html = soup_item.find(class_=re.compile("ProductInfo_name__"))
        title = f"{title_html.find('a').text.strip()} {title_html.find('h1').text.strip()}"

        data_json = soup_item.find("script", {"id": "__NEXT_DATA__"}).text.strip()
        data_dict = json.loads(data_json)

        image_link = data_dict["props"]["pageProps"]["data"]["products"]["products"][0]["media"]["full"][0]

        sizes = []
        all_sizes = data_dict["props"]["pageProps"]["data"]["products"]["products"][0]["items"]
        for size in all_sizes:
            size_name = size["name"].upper().replace(" ", "_")

            if size["stock"] == "no":
                size_price = "SOLD_OUT"
            else:
                size_price = float(
                    data_dict["props"]["pageProps"]["data"]["products"]["products"][0]["prices"][0]["priceAsNumber"])
                size_price *= multiplier

            sizes.append(f"{size_name} = {size_price}")

        return [title, image_link, item_url, '\n'.join(sizes)]
    except Exception:
        print(f"[-] {item_url} {traceback.format_exc()}")
        if retry:
            return await parser_nittygrittystore(session, item_url, proxylist, multiplier, request_delay, (retry - 1))


async def nittygrittystore(proxylist: list, proxy_all_values: list):
    main_url, file_name = "https://nittygrittystore.com", "nittygrittystore"
    request_delay = 0.1
    headers = {"user-agent": user_agent.generate_user_agent()}

    agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

    primary_id = proxy_all_values[25][4]
    multiplier = float(proxy_all_values[6][7].replace(',', '.'))
    worksheet_id = proxy_all_values[25][3]

    worktable = await connect_to_gs(worksheet_id, agcm)
    worksheet = await worktable.get_worksheet(0)
    all_records = await worksheet.get_all_values()
    if len(all_records) > 0:
        all_records.pop(0)

    start = time.time()

    async with aiocfscrape.CloudflareScraper() as session:
        proxies = await get_proxy(proxylist)
        sitemap_url = f"{main_url}/sitemap/sitemap.xml"
        sitemap_markup = await get_data(session=session, proxies=proxies, headers=headers, request_delay=request_delay,
                                        url=sitemap_url)
        soup_sitemap = BeautifulSoup(sitemap_markup, "xml")
        sitemap_urls = soup_sitemap.find_all("loc", string=re.compile(f"{main_url}/sitemap/products/"))
        table_content = []
        max_active_tasks = 10
        for sitemap_prods in sitemap_urls:
            sitemap_prods_markup = await get_data(session=session, proxies=proxies, headers=headers,
                                                  url=sitemap_prods.text, request_delay=request_delay)
            soup_sitemap_prods = BeautifulSoup(sitemap_prods_markup, "xml")
            all_products = soup_sitemap_prods.find_all("loc")

            tasks = []
            for loc in all_products:
                if loc.text == "https://nittygrittystore.com/undefined":
                    continue

                if len(tasks) >= max_active_tasks:
                    results = await asyncio.gather(*tasks)
                    table_content.extend(results)
                    tasks.clear()

                task = asyncio.create_task(
                    parser_nittygrittystore(session, loc.text, proxylist, multiplier, request_delay))
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
