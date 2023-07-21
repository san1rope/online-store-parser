import shutil
import gspread_asyncio

from concurrent.futures import ProcessPoolExecutor
from websites import *
from tools import *


async def main():
    agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

    config = await load_config('.env')
    proxytable = await connect_to_gs(config.misc.proxy, agcm)
    proxysheet = await proxytable.get_worksheet(0)
    proxy_all_values = await proxysheet.get_all_values()

    tasks = [stussy_us]

    # Getting a list of proxies from google tables | aiocfscrape
    proxylist = []
    for value in proxy_all_values:
        if not value[10]:
            break

        proxylist.append(value[10])

    proxylist.pop(0)

    # Create proxy extensions for chrome | Selenium
    folder = f"{os.getcwd()}/proxy-extensions"
    for file in os.scandir(folder):
        shutil.rmtree(file.path)

    for proxy in proxylist:
        proxy_list = proxy.split(':')
        host, port, login, password = proxy_list[0], proxy_list[1], proxy_list[2], proxy_list[3]
        filename_host = host.replace('.', '_').replace('-', '_')
        filename_port = port.replace('.', '_')
        await create_proxy_extension(login, password, host, port,
                                     f"{os.getcwd()}/proxy-extensions/{filename_host}_{filename_port}")

    # Running parsers
    loop = asyncio.get_running_loop()
    while True:
        with ProcessPoolExecutor(max_workers=config.misc.processes) as executor:
            for task in tasks:
                loop.run_in_executor(executor, wrapper, task, proxylist, proxy_all_values)

        await asyncio.sleep(config.operate_time.once_per)


if __name__ == "__main__":
    asyncio.run(main())
