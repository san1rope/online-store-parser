import csv
import re
import time
from concurrent.futures import ProcessPoolExecutor
import gspread_asyncio
import lxml

from bs4 import BeautifulSoup
from tools import *


async def parser_nakedcph(item_url: str, proxylist: list, multiplier: float, request_delay: float = None,
                          retry: int = 3):
    proxies = await get_proxy(proxylist)
    user_agent_ = user_agent.generate_user_agent()

    item_markup = await get_data_selenium(item_url, proxies, request_delay, user_agent_)
    soup_item = BeautifulSoup(item_markup, "lxml")

    try:
        title_brand = soup_item.find(class_="product-title mb-4").find(class_="product-property").text.strip()
        title_model = soup_item.find(class_="product-title mb-4").find(
            class_="product-property product-name").text.strip()
        title = f"{title_brand} {title_model}".upper()

        image_link = "https://www.nakedcph.com" + soup_item.find(class_="carousel__image embed-responsive-item")["src"]

        sizes = []
        try:
            select = soup_item.find("select", {"id": "product-form-select"}).find_all("option")
            for option in select:
                if 'value="-1"' in str(option):
                    continue

                size_name = option.text.strip().upper()

                if "disabled" in str(option):
                    size_price = "SOLD_OUT"
                else:
                    full_size_price = soup_item.find(class_="h4 mb-4").find(class_="price__sales").text.strip()
                    size_price = ""
                    for ch in full_size_price:
                        if ch.isdigit() or ch == ".":
                            size_price += ch

                    size_price = await fixed_price((float(size_price) * multiplier), 2)

                sizes.append(f"{size_name} = {size_price}")
        except AttributeError:
            sizes.append("ONE_SIZE = SOLD_OUT")

        return [title, image_link, item_url, '\n'.join(sizes)]
    except Exception:
        print(f"[-] {item_url} {traceback.format_exc()} {proxies}")
        if retry:
            return await parser_nakedcph(item_url, proxylist, multiplier, request_delay, (retry - 1))


async def flow_distribution(links: list, max_active_tasks: int, proxylist: list, multiplier: float,
                            request_delay: float, func):
    tasks = []
    table_content = []
    for url in links:
        if len(tasks) >= max_active_tasks:
            results = await asyncio.gather(*tasks)
            table_content.extend(results)
            tasks.clear()

        task = asyncio.create_task(parser_nakedcph(url, proxylist, multiplier, request_delay))
        tasks.append(task)

    if len(tasks):
        results = await asyncio.gather(*tasks)
        table_content.extend(results)
        tasks.clear()

    return table_content


async def nakedcph(proxylist: list, proxy_all_values: list):
    main_url, file_name = "https://www.nakedcph.com", "nakedcph"
    request_delay = 3

    agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

    primary_id = proxy_all_values[15][4]
    multiplier = float(proxy_all_values[5][7].replace(',', '.'))
    worksheet_id = proxy_all_values[15][3]

    worktable = await connect_to_gs(worksheet_id, agcm)
    worksheet = await worktable.get_worksheet(0)
    all_records = await worksheet.get_all_values()
    if len(all_records) > 0:
        all_records.pop(0)

    start = time.time()

    async with aiocfscrape.CloudflareScraper() as session:
        available_proxies = await check_proxy(proxylist, session, main_url)
        proxies = await get_proxy(available_proxies)
        max_active_tasks = 3
        links_for_process = 10
        user_agent_ = user_agent.generate_user_agent()
        sitemap_url = f"https://www.nakedcph.com/sitemap/naked_en_sitemap.xml"
        sitemap_markup = await get_data_selenium(sitemap_url, proxies, request_delay, user_agent_)
        soup_sitemap = BeautifulSoup(sitemap_markup, "xml")
        all_loc = soup_sitemap.find_all("loc", string=re.compile(f"{main_url}/en/product/"))

        links = []
        temp_links = []
        for loc in all_loc:
            temp_links.append(loc.text)
            if len(temp_links) >= links_for_process:
                links.append(temp_links)
                temp_links = []

        if temp_links:
            links.append(temp_links)

        futures = []
        loop = asyncio.get_running_loop()
        with ProcessPoolExecutor(max_workers=3) as executor:
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
