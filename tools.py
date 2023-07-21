import asyncio
import os
import traceback

import undetected_chromedriver
import aiohttp
import aiocfscrape
import user_agent

from google.oauth2.service_account import Credentials
from random import randint

from webdriver_manager.chrome import ChromeDriverManager

from config import load_config


def wrapper(async_func, *args):
    task = asyncio.run(async_func(*args))
    return task


async def get_result(futures: list, delay: float):
    table_content = []
    try:
        for future in futures:
            if future.result():
                table_content.extend(future.result())
    except asyncio.exceptions.InvalidStateError:
        await asyncio.sleep(delay)
        table_content = await get_result(futures, delay)

    return table_content


async def get_data(session: aiocfscrape.CloudflareScraper, url: str, request_delay: float, headers: dict, proxies: dict,
                   retry: int = 5, cookies: dict = None):
    config = await load_config('.env')
    if request_delay:
        await asyncio.sleep(request_delay)

    try:
        if proxies:
            proxy = f"http://{proxies['host']}:{proxies['port']}"
            proxy_auth = aiohttp.BasicAuth(proxies["login"], proxies["password"])
        else:
            proxy, proxy_auth = None, None

        async with session.get(url=url, headers=headers, proxy=proxy, proxy_auth=proxy_auth,
                               cookies=cookies, timeout=10) as response:
            if response.status in [430, 503]:
                raise ConnectionError
            elif response.status in [404, 500]:
                return None

            print(f"[+] {url} {response.status}")
    except Exception:
        await asyncio.sleep(config.operate_time.req_retry_delay)
        if retry:
            print(f"retry={retry} => {url} {proxies}")
            return await get_data(session=session, url=url, request_delay=request_delay, proxies=proxies,
                                  headers=headers, retry=(retry - 1), cookies=cookies)
        else:
            raise ConnectionError
    else:
        return await response.text()


def get_creds():
    creds = Credentials.from_service_account_file("credentials.json")
    scoped = creds.with_scopes([
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ])

    return scoped


async def connect_to_gs(table_id: str, agcm):  # Подключение к гугл таблицам
    agc = await agcm.authorize()
    ss = await agc.open_by_key(table_id)
    return ss


async def get_proxy(proxy_list: list) -> dict:  # Получаю рандомное прокси со списка
    random_proxy_num = randint(0, (len(proxy_list) - 1))
    proxy = proxy_list[random_proxy_num].split(':')
    host, port, login, password = proxy[0], proxy[1], proxy[2], proxy[3]
    return {"host": host, "port": port, "login": login, "password": password}


async def check_proxy(proxy_list: list, session: aiocfscrape.CloudflareScraper, url: str) -> list:
    available_proxies = []
    for proxy in proxy_list:
        proxy_split = proxy.split(':')
        headers = {"user-agent": user_agent.generate_user_agent()}
        host, port, login, password = proxy_split[0], proxy_split[1], proxy_split[2], proxy_split[3]
        proxy_ip, proxy_auth = f"http://{host}:{port}", aiohttp.BasicAuth(login, password)
        async with session.get(url=url, headers=headers, proxy=proxy_ip, proxy_auth=proxy_auth) as response:
            code = response.status
            if code == 200:
                available_proxies.append(proxy)

    return available_proxies


async def fixed_price(numObj, digits: float = 0) -> str:
    return f"{numObj:.{digits}f}"


async def formatted_content(table_entries: list, new_entries: list, prefix_id: str) -> list:
    unsorted_content = {}
    deleted_records = []
    for exists_record in table_entries:
        flag = False
        for new_record in new_entries:
            try:
                url = new_record[2]
                if url in exists_record[3]:
                    exists_id_with_pref = exists_record[0]
                    exists_id = exists_id_with_pref.split('-')[-1]
                    unsorted_content[int(exists_id)] = [exists_id_with_pref, new_record[0], new_record[1],
                                                        new_record[2],
                                                        new_record[3]]

                    deleted_records.append(exists_record)
                    flag = new_record
                    break
            except Exception:
                flag = new_record

        if flag:
            new_entries.remove(flag)

    for record in deleted_records:
        table_entries.remove(record)

    for exists_record in table_entries:
        exists_id_with_pref = exists_record[0]
        exists_id = exists_id_with_pref.split('-')[-1]

        sizes = exists_record[4].split('\n')
        new_sizes = []
        for size in sizes:
            size_list = size.split("=")
            size_name = size_list[0].strip()

            new_sizes.append(f"{size_name} = SOLD_OUT")

        exists_record[4] = '\n'.join(sizes)

        unsorted_content[int(exists_id)] = exists_record

    for new_record in new_entries:
        try:
            id = len(unsorted_content) + 1
            id_with_pref = prefix_id + str(id)
            new_record.insert(0, id_with_pref)
            unsorted_content[id] = new_record
        except Exception:
            continue

    all_keys = list(unsorted_content)
    all_keys.sort()
    finished_content = [["ID", "TITLE", "IMAGE_LINK", "LINK", "SIZES"]]
    for key in all_keys:
        finished_content.append(unsorted_content[key])

    return finished_content


async def upload_to_gs(agcm, filename: str, worksheet_id: str):
    path = f"temp/{filename}.csv"
    with open(path, "r", encoding="utf-8") as file:
        agc = await agcm.authorize()
        await agc.import_csv(worksheet_id, file.read())

    os.remove(path)


async def create_proxy_extension(username, password, endpoint, port, path):
    try:
        os.mkdir(path)
    except FileExistsError:
        pass

    manifest_json = """
    {
        "version": "1.0.0",
        "manifest_version": 2,
        "name": "Proxies",
        "permissions": [
            "proxy",
            "tabs",
            "unlimitedStorage",
            "storage",
            "<all_urls>",
            "webRequest",
            "webRequestBlocking"
        ],
        "background": {
            "scripts": ["background.js"]
        },
        "minimum_chrome_version":"22.0.0"
    }
    """

    background_js = """
    var config = {
            mode: "fixed_servers",
            rules: {
            singleProxy: {
                scheme: "http",
                host: "%s",
                port: parseInt(%s)
            },
            bypassList: ["localhost"]
            }
        };

    chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

    function callbackFn(details) {
        return {
            authCredentials: {
                username: "%s",
                password: "%s"
            }
        };
    }

    chrome.webRequest.onAuthRequired.addListener(
                callbackFn,
                {urls: ["<all_urls>"]},
                ['blocking']
    );
    """ % (endpoint, port, username, password)

    with open(path + "/manifest.json", 'w') as m_file:
        m_file.write(manifest_json)
    with open(path + "/background.js", 'w') as b_file:
        b_file.write(background_js)


async def get_data_selenium(url: str, proxies: dict = None, delay: float = None, user_agent_: str = None):
    """ Не готово """
    host, port = proxies["host"], proxies["port"]

    options = undetected_chromedriver.ChromeOptions()
    options.add_argument('--headless=new')
    options.add_argument('--ignore-certificate-errors')
    if user_agent_:
        options.add_argument(f"--user-agent={user_agent_}")

    if proxies:
        filename_host = host.replace('.', '_').replace('-', '_')
        filename_port = port.replace('.', '_')
        options.add_argument(
            f'--load-extension={os.getcwd()}/proxy-extensions/{filename_host}_{filename_port}/')

    driver = undetected_chromedriver.Chrome(driver_executable_path=ChromeDriverManager().install(), options=options)

    markup = None
    try:
        driver.get(url=url)
        if delay:
            await asyncio.sleep(delay)

        markup = driver.page_source
    except Exception as ex:
        print(f"[-] {url} selenium-warning {ex}")
    finally:
        print(f"[+] {url} selenium-passed")
        driver.close()
        driver.quit()

    return markup
