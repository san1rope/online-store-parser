import csv
import re
import time
import gspread_asyncio
import lxml

from bs4 import BeautifulSoup
from tools import *


async def parser_theroombarcelona(session: aiocfscrape.CloudflareScraper, item_url: str, proxylist: list,
                                  multiplier: float, request_delay: float = None, retry: int = 3):
    try:
        proxies = await get_proxy(proxylist)
        headers = {"user-agent": user_agent.generate_user_agent()}

        item_markup = await get_data(session, item_url, request_delay, headers, proxies)
        soup_item = BeautifulSoup(item_markup, "lxml")

        wrong_url = soup_item.find(class_=re.compile("help-text"))
        if wrong_url:
            return None

        title_brand = soup_item.find(class_="product-short").find("h2").text.strip()
        title_model = soup_item.find(class_="product-short").find("h1").text.strip()
        title = f"{title_brand} {title_model}".upper()

        try:
            image_link = soup_item.find(
                class_="woocommerce-product-gallery woocommerce-product-gallery--with-images woocommerce-product-gallery--columns-4 woocommerce-thumb-nav--bottom images").find(
                class_="woocommerce-product-gallery__image").find("a")["href"]
        except AttributeError:
            image_link = "https://www.freeiconspng.com/uploads/no-image-icon-6.png"

        sizes = []
        size_price = ""
        try:
            full_size_price = soup_item.find(class_="summary-top clearfix").find("ins")
            if not full_size_price:
                full_size_price = soup_item.find(class_="summary-top clearfix").text.strip()
            else:
                full_size_price = full_size_price.text.strip()

            for ch in full_size_price:
                if ch.isdigit() or ch == ',':
                    size_price += ch

            size_price = await fixed_price((float(size_price.replace(',', '.')) * multiplier), 2)
        except ValueError:
            size_price = "SOLD_OUT"

        try:
            selector = soup_item.find(
                "ul", class_="variable-items-wrapper button-variable-items-wrapper wvs-style-squared").find_all("li")
            for size in selector:
                size_name = size.text.strip().replace(" ", "_")

                if "disabled" in str(size["class"]):
                    size_price = "SOLD_OUT"

                sizes.append(f"{size_name} = {size_price}")
        except AttributeError:
            sizes.append(f"ONE_SIZE = {size_price}")

        return [title, image_link, item_url, '\n'.join(sizes)]
    except Exception:
        print(f"[-] {item_url} {traceback.format_exc()}")
        if retry:
            return await parser_theroombarcelona(session, item_url, proxylist, multiplier, request_delay, (retry - 1))


async def theroombarcelona(proxylist: list, proxy_all_values: list):
    main_url, file_name = "https://www.theroombarcelona.com", "theroombarcelona"
    request_delay = 0.1
    headers = {"user-agent": user_agent.generate_user_agent()}

    agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

    primary_id = proxy_all_values[14][4]
    multiplier = float(proxy_all_values[2][7].replace(',', '.'))
    worksheet_id = proxy_all_values[14][3]

    worktable = await connect_to_gs(worksheet_id, agcm)
    worksheet = await worktable.get_worksheet(0)
    all_records = await worksheet.get_all_values()
    if len(all_records) > 0:
        all_records.pop(0)

    start = time.time()

    async with aiocfscrape.CloudflareScraper() as session:
        proxies = await get_proxy(proxylist)

        sitemap_markup = await get_data(session, f"{main_url}/sitemap_index.xml", request_delay, headers, proxies)
        soup_sitemap = BeautifulSoup(sitemap_markup, "xml")

        all_loc = soup_sitemap.find_all("loc", string=re.compile("https://www.theroombarcelona.com/product-sitemap"))
        table_content = []
        max_active_tasks = 10
        no_products = ["https://www.theroombarcelona.com/all/uncategorized/poly-knit-dress-white/",
                       "https://www.theroombarcelona.com/tienda/"]
        for loc in all_loc:
            sitemap_prods_markup = await get_data(session, loc.text, request_delay, headers, proxies)
            soup_sitemap_prods = BeautifulSoup(sitemap_prods_markup, "xml")

            all_products_url = soup_sitemap_prods.find_all("loc")
            tasks = []
            for product_url in all_products_url:
                if (product_url.text in no_products) or (".jpg" in product_url.text) or (".png" in product_url.text):
                    continue

                if len(tasks) >= max_active_tasks:
                    results = await asyncio.gather(*tasks)
                    table_content.extend(results)
                    tasks.clear()

                task = asyncio.create_task(
                    parser_theroombarcelona(session, product_url.text, proxylist, multiplier, request_delay))
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
