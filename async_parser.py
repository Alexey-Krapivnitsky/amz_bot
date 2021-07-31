import os
import json
import logging
import requests
import aiocron
import asyncio
import random
import html5lib


from logging.handlers import TimedRotatingFileHandler
from dotenv import load_dotenv
from bs4 import BeautifulSoup


def get_constants_from_env():
    dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)


log = logging.getLogger()
log.setLevel(logging.INFO)
rh = TimedRotatingFileHandler('data_files\\logs\\parser.log', when='d', interval=1, encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
rh.setFormatter(formatter)
log.addHandler(rh)


async def write_log(level, message):
    if level == 'err':
        log.error(message)
    elif level == 'inf':
        log.info(message)
    else:
        log.debug(message)


get_constants_from_env()

PROXY_KEY = os.environ.get('PROXY_KEY')
PROXY_URL = f"https://api.good-proxies.ru/get.php?type%5Bhttp%5D=on&count=&ping=5000&time=600&works=100&key={PROXY_KEY}"

with open('data_files\\user_agents', 'r') as ua:
    USER_AGENT = ua.read().split('\n')


def get_proxy(proxy_url):
    response = requests.post(proxy_url)
    proxy = response.content.decode().strip().split('\n')
    # print(proxy)
    with open('data_files\\proxy.json', 'w') as pr:
        json.dump(proxy, pr)


# get_proxy(PROXY_URL)


@aiocron.crontab('*/15 * * * *')
async def get_proxy_from_site(proxy_url=PROXY_URL):
    await write_log('inf', f'reload proxy from site')
    response = requests.post(proxy_url)
    proxy = response.content.decode().strip().split('\n')
    with open('data_files\\proxy.json', 'w') as pr:
        json.dump(proxy, pr)
    return proxy


async def write_result(url, data):
    asin = url.split('/')[-1][:10]
    with open('data_files\\statuses.json', 'r') as stat:
        result = json.load(stat)
    if asin in result.keys():
        result[asin] = data
    else:
        result.setdefault(asin, data)
    with open('data_files\\statuses.json', 'w', encoding='utf-8') as stat:
        json.dump(result, stat)


async def get_proxy_from_file(file='proxy.json'):
    with open(f'data_files\\{file}', 'r') as pr:
        proxy = json.load(pr)
    return proxy


async def get_response(url, headers, proxy):
    response = requests.get(url, headers=headers, proxies={'http': proxy}, timeout=5.0)
    return response


async def write_error_content(url, content):
    asin = url.split('/')[-1][:10]
    with open(f'pages\\errors_page\\{asin}_error.html', 'wb') as epr:
        epr.write(content)


async def parse_page(url):
    asin = url.split('/')[-1][:10]
    request_count = 0
    await write_log('inf', f'start parse {url}')
    await asyncio.sleep(.1)
    proxy = await get_proxy_from_file()
    # print(f'proxy - {proxy}')
    if proxy[0] == '<html>':
        get_proxy(PROXY_URL)
        proxy = await get_proxy_from_file()
    result_data = {}
    user_agent = random.choice(USER_AGENT)

    if 'uk' in url:
        host = 'www.amazon.co.uk'
        lang = 'en-en'
        headers = {
            'User-Agent': user_agent,
            'Host': host,
            'Accept-Language': lang
        }
    else:
        host = 'www.amazon.com'
        lang = 'en-Us'
        headers = {
            'User-Agent': user_agent,
            'Host': host,
            'Accept-Language': lang
        }
    await write_log('inf', f'{url} - {headers}')
    while True:
        if request_count == 10:
            with open('data_files\\statuses.json', 'r') as rd:
                result_data = json.load(rd)[asin]
            await write_log('err', 'Нет ответа сервера')
            break
        request_proxy = random.choice(proxy)
        try:
            response = await get_response(url, headers, request_proxy)
            if response.status_code in [500, 503, 504, 404]:
                request_count += 1
                continue
        except Exception as e:
            await write_log('err', e)
            request_count += 1
            continue

        page_code = response.content  # .decode(encoding='utf-8')
        # print(page_code)
        # print(type(page_code))
        soup = BeautifulSoup(page_code, 'html5lib')
        print(type(soup))
        is_captcha = soup.find('form', attrs={'action': '/errors/validateCaptcha'})

        if is_captcha:
            await write_log('', f'Is Captcha in {url}, current Proxy - {request_proxy}, try next Proxy.')
            request_count += 1
            continue
        review_count = soup.find('span', attrs={'id': 'acrCustomerReviewText'})
        # if review_count is None:
        #     await write_error_content(url, response.content)
        print('countinue')
        identifier_words_type = soup.find('label', attrs={'class': 'a-form-label'})
        identifier_words = soup.find('span', attrs={'class': 'selection'})

        if review_count is not None:
            review_count = int(review_count.string.strip().split(' ')[0].replace(',', ''))
        else:
            review_count = 'Элемент "Отзывы" на странице отсутствует'

        if identifier_words_type is None:
            identifier_words_type = 'Элемент "Вариации" отсутствует'
            identifier_words = []
        else:
            identifier_words_type = identifier_words_type.string.strip()
            identifier_words = identifier_words.string.strip().split(',')

        result_data = {'review_count': review_count,
                       'identifier_words_type': identifier_words_type,
                       'identifier_words': identifier_words,
                       'identifier_words_count': len(identifier_words)
                       }
        print(result_data)
        break
    await write_log('inf', f'{result_data}')
    print('LOGG')
    await asyncio.sleep(.1)
    await write_result(url, result_data)
    print('WRITE')
    await asyncio.sleep(.1)
    return {'url': url, 'result_data': result_data}


async def run_parser(urls):
    for url in urls:
        await asyncio.gather(parse_page(url))


if __name__ == '__main__':

    URLS = ['http://www.amazon.com/dp/B08LP22NK5',
            'http://www.amazon.com/dp/B08NDY3841',
            'http://www.amazon.co.uk/dp/B08N1LMJF9',
            'https://www.amazon.com/dp/B08P62FDQ8']

    while True:
        asyncio.run(run_parser(URLS))

    # executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)
    # event_loop = asyncio.get_event_loop()
    # w = asyncio.wait([run_parser(executor, URLS)])
    # event_loop.run_until_complete(w)
