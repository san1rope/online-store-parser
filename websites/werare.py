import csv
import time
import asyncio
import aiocfscrape
import gspread_asyncio
import lxml
import user_agent

from bs4 import BeautifulSoup
from tools import get_data, connect_to_gs, get_proxy, fixed_price, formatted_content, get_creds, upload_to_gs


async def parser_werare(session: aiocfscrape.CloudflareScraper, page_url: str, proxylist: list, multiplier: float,
                        request_delay: float = None):
    content = []
    request_counter = 0
    proxies = await get_proxy(proxylist)
    headers = {"user-agent": user_agent.generate_user_agent()}

    page_markup = await get_data(session, page_url, request_delay, headers, proxies)
    request_counter += 1
    soup_page = BeautifulSoup(page_markup, "lxml")

    all_products = soup_page.find_all(class_="product-layout product-grid col-md-4 col-sm-6 col-xs-6")
    for product in all_products:  # Прохожусь по товарам
        if request_counter >= 5:
            proxies = await get_proxy(proxylist)
            headers = {"user-agent": user_agent.generate_user_agent()}
            request_counter = 0

        discounted_product = False
        try:
            # Достаю атрибут href с разметки, что-бы получить ссылку на товар
            product_href = product.find("div", class_="image").find(
                "a", class_="wishlist oct-button").find_next_sibling()['href']
        except KeyError:  # Для товаров со скидкой, у них немного отличается код
            product_href = product.find("div", class_="image").find(
                "a", class_="wishlist oct-button").find_next_sibling().find_next_sibling()['href']
            discounted_product = True

        product_markup = await get_data(session, product_href, request_delay, headers, proxies)
        request_counter += 1
        soup_product = BeautifulSoup(product_markup, "lxml")

        try:
            title = soup_product.find("h1", class_="product-header").text.strip()
        except AttributeError:
            continue

        image_link = soup_product.find("li", class_="image thumbnails-one thumbnail").find("a")['href']
        link = product_href
        sizes = []
        try:
            # Получаю размеры товара
            temp_sizes = soup_product.find(class_="options-box").find_all(
                class_="radio oct-product-radio ravno-val-sp")
            if not temp_sizes:  # Если возвращает None, тогда у товара не указаны размеры или он один
                temp_sizes = soup_product.find(class_="options-box").find_all(class_="radio oct-product-radio")
                if len(temp_sizes) == 0:
                    temp_sizes = soup_product.find(class_="options-box").find_all(
                        class_="radio oct-product-radio not-allowed")

                if discounted_product:  # Проверка на товар со скидкой, у них прайс лежит в другом классе
                    size_price_full = soup_product.find(class_="oct-price-new h2").text.strip()
                else:
                    size_price_full = soup_product.find(class_="oct-price-normal h2").text.strip()

                size_price = ""
                for i in size_price_full:  # Сохраняю цифры с прайса, что-бы убрать валюту
                    if i.isdigit():
                        size_price += i

                # Прохожусь по размерам, у товара может быть много размеров с одинаковой ценой
                for size in temp_sizes:
                    size_name = size.find("span", class_="radio-name-sp").text.strip().replace(" ", "_")
                    sizes.append(f"{size_name} = {await fixed_price((float(size_price) * float(multiplier)), 2)}")
            else:  # Если у товара есть много размеров и разные цены на них
                for size in temp_sizes:
                    size_name = size.find("span", class_="radio-name-sp").text.strip().replace(" ", "_")
                    size_price_temp = size.find("span", class_="radio-val-sp").text.strip()
                    size_price = ""
                    for i in size_price_temp:
                        if i.isdigit():
                            size_price += i

                    size_price = await fixed_price((float(size_price) * float(multiplier)), 2)

                    sizes.append(f"{size_name} = {size_price}")
        except ConnectionError:  # Обработка превышеного таймаута
            await asyncio.sleep(30)
            proxies = await get_proxy(proxylist)
            continue
        except Exception:  # Обработка на случай, если не могу найти размер товара
            try:  # У товара могут отсутствовать размеры, в таком случае сохраняю только цену
                full_price = soup_product.find(class_="oct-price-normal h2").text.strip()
                price = ""
                for i in full_price:
                    if i.isdigit():
                        price += i

                sizes.append(f"ONE_SIZE = {await fixed_price((float(price) * float(multiplier)), 2)}")
            except Exception:  # В том случае, если товара нету в наличии
                sizes.append("ONE_SIZE = SOLD_OUT")

        content.append([title.upper(), image_link, link, '\n'.join(sizes)])

    return content


async def werare(proxylist: list, proxy_all_values: list):
    main_url, file_name = "https://werare.com.ua", "werare"
    request_delay = 0.1
    headers = {"user-agent": user_agent.generate_user_agent()}

    agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

    primary_id = proxy_all_values[4][4]
    multiplier = float(proxy_all_values[3][7].replace(',', '.'))
    worksheet_id = proxy_all_values[4][3]

    worktable = await connect_to_gs(worksheet_id, agcm)
    worksheet = await worktable.get_worksheet(0)
    all_records = await worksheet.get_all_values()
    if len(all_records) > 0:
        all_records.pop(0)

    start = time.time()

    async with aiocfscrape.CloudflareScraper() as session:
        proxies = await get_proxy(proxylist)

        # Собираю ссылки с 3 категорий товаров
        sections_markup = await get_data(session, main_url, request_delay, headers, proxies)
        soup_sections = BeautifulSoup(sections_markup, "lxml")

        nav_bar = soup_sections.find("ul", class_="nav navbar-nav").find_all("li", class_="dropdown")
        sections = []
        for li in nav_bar:
            href = li.find("a")['href'].strip()
            if href == "#":
                continue

            category_url = f"{main_url}/{href}"
            sections.append(category_url)

        table_content = []
        max_active_tasks = 10
        for section_url in sections:
            page_markup = await get_data(session, section_url, request_delay, headers, proxies)
            soup_pag = BeautifulSoup(page_markup, "lxml")
            count_pages = int(
                soup_pag.find("ul", class_="pagination").find_all("li")[-1].find("a")["href"].split('-')[-1])

            tasks = []
            for page in range(1, count_pages + 1):
                url = f"{section_url}/page-{page}"
                if len(tasks) >= max_active_tasks:
                    results = await asyncio.gather(*tasks)
                    table_content.extend(results[0])
                    tasks.clear()

                task = asyncio.create_task(parser_werare(session, url, proxylist, multiplier, request_delay))
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
