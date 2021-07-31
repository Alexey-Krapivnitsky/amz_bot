from aiogram import Bot, types
from aiogram.dispatcher import Dispatcher
from aiogram.utils import executor
from datetime import datetime

from async_parser import *

get_constants_from_env()

BOT_TOKEN = os.environ.get('BOT_TOKEN')
BOT_PASSWORD = os.environ.get('BOT_PASSWORD')
BOT_WATCH_PASSWORD = os.environ.get('BOT_WATCH_PASSWORD')

with open('data_files\\users.json', 'r', encoding='utf-8') as users:
    USERS = json.load(users)
    USERS_ID = list(USERS.values())

with open('data_files\\watch_users.json', 'r', encoding='utf-8') as w_users:
    W_USERS = json.load(w_users)
    W_USERS_ID = list(W_USERS.values())

INFO_USERS = set(USERS_ID + W_USERS_ID)

HELP_MESSAGE = '''
"/run" - запустить мониторинг\n
"/add us B08NDY3841" - добавить отслеживаемый товар B08NDY3841 на площадке us\n
"/del us B08NDY3841" - удалить отслеживаемый товар B08NDY3841 на площадке us из списка\n
"/status" - получить сведения о состоянии монитора и последние данные об отслеживаемых параметрах\n
"/stop" - остановить мониторинг
'''

AMZ_MARKETS = {'us': 'https://www.amazon.com/dp/',
               'uk': 'https://www.amazon.co.uk/dp/'}

# test_urls = ['http://www.amazon.com/dp/B08LP22NK5',
#             'http://www.amazon.com/dp/B08NDY3841',
#             'http://www.amazon.co.uk/dp/B08N1LMJF9',
#             'https://www.amazon.com/dp/B08P62FDQ8']

with open('data_files\\statuses.json', 'r', encoding='utf-8') as stat:
    data = json.load(stat)
    if 'asins' in data.keys():
        AMZ_URLS = data['asins']
    else:
        AMZ_URLS = []


def get_results():
    fixed_result = {}
    with open('data_files\\statuses.json', 'r', encoding='utf-8') as stat:
        result_data = json.load(stat)
    for every_url in AMZ_URLS:
        asin = every_url.split('/')[-1][:10]
        if asin in result_data.keys():
            fixed_result.setdefault(asin, result_data[asin])
    return fixed_result

monitor_bot = Bot(token=BOT_TOKEN)
bot_dispatcher = Dispatcher(monitor_bot)
tasks = {}


@bot_dispatcher.message_handler(lambda message: message.chat.id not in USERS_ID)
async def login_required(message: types.Message):
    message_type = message.text.split(' ')[0]
    if message_type != '/password':
        await message.reply('Введите пароль доступа в формате\n'
                            '/password Значение пароля')
    else:
        await process_password_command(message)


@login_required
@bot_dispatcher.message_handler(commands=['start'])
async def process_start_command(message: types.Message):
    await message.reply('Привет!\nГотов к запуску мониторинга!\nДля просмотра команд наберите\n"/help".')


async def parse_result(old_data, last_data):
    result_message = []
    await write_log('inf', f'old data - {old_data}')
    await write_log('inf', f'new data - {last_data}')
    for key, value in last_data.items():
        if key in old_data.keys():
            if not isinstance(old_data[key], str) and not isinstance(last_data[key], str):
                old_review = old_data[key]['review_count']
                old_words = old_data[key]['identifier_words_count']
                new_review = last_data[key]['review_count']
                new_words = last_data[key]['identifier_words_count']

                if isinstance(old_review, int) and isinstance(old_review, type(new_review)):
                    if new_review < old_review:
                        result_message.append(f'Найдены изменения на странице товара {key}: '
                                              f'уменьшилось количество отзывов.')
                if isinstance(old_words, int) and isinstance(old_words, type(new_words)):
                    if new_words < old_words:
                        result_message.append(f'Найдены изменения на странице товара {key}: '
                                              f'уменьшилось количество слов-определителей')
                if isinstance(old_review, int) and isinstance(new_review, str):
                    result_message.append(f'На странице товара {key} пропали отзывы')
                if isinstance(old_words, int) and isinstance(new_words, str):
                    result_message.append(f'На странице товара {key} пропали слова-определители')
                if isinstance(old_review, str) and isinstance(new_review, int):
                    result_message.append(f'На странице товара {key} восстановлены отзывы')
                if isinstance(old_words, str) and isinstance(new_words, int):
                    result_message.append(f'На странице товара {key} восстановлены слова-определители')
                if isinstance(old_words, type(new_words)) and isinstance(old_review, type(new_review)):
                    result_message.append(f'Товар {key} - значимых изменений не обнаружено')
    return result_message


async def parse_worker(message):
    fixed_result = get_results()
    while True:
        for url in AMZ_URLS:
            await asyncio.gather(parse_page(url))
        await asyncio.sleep(2)
        last_result = get_results()
        messages = await parse_result(fixed_result, last_result)
        if messages:
            for status_message in messages:
                await write_log('inf', status_message)
                await asyncio.sleep(.1)
                if 'изменений не обнаружено' not in status_message:
                    for user_id in INFO_USERS:
                        await monitor_bot.send_message(user_id, status_message)
                        await asyncio.sleep(.1)
                        await write_log('inf', user_id)
            await asyncio.sleep(.1)

            fixed_result = last_result
            await write_log('inf', 'OK')
            await asyncio.sleep(.1)
        else:
            await write_log('debug', 'No messages')


async def start_parse(urls):
    parse_task = [asyncio.create_task(parse_page(url)) for url in urls]
    return parse_task


@login_required
@bot_dispatcher.message_handler(commands=['help'])
async def process_help_command(message: types.Message):
    await message.reply(HELP_MESSAGE)


@login_required
@bot_dispatcher.message_handler(commands=['run'])
async def task_start(message: types.Message):
    task = tasks.get('parser')
    if not task:
        task = asyncio.create_task(parse_worker(message))
        tasks['parser'] = task
        await message.reply('Запущен мониторинг')
    else:
        await message.reply('Мониторинг уже работает')


@login_required
@bot_dispatcher.message_handler(commands=['status'])
async def task_status(message: types.Message):

    task = tasks.get('parser')
    if task:
        await monitor_bot.send_message(message.chat.id, 'Монитор работает')
    else:
        await monitor_bot.send_message(message.chat.id, 'Монитор не запущен. Для запуска наберите "/run"')

    with open('data_files\\statuses.json', 'r') as stat:
        statuses = json.load(stat)

    with open('data_files\\asins.json', 'r') as stat:
        asins = json.load(stat)

    if 'asins' in statuses.keys():
        check_list = asins['asins']
        await monitor_bot.send_message(message.chat.id, f'На отслеживании товаров: {len(check_list)}')
        for num, asin in enumerate(check_list, start=1):
            if asin[-10:] in statuses.keys():
                product_status = statuses[asin[-10:]]
                if not isinstance(product_status, dict):
                    reply_message = f'{num}. {asin}\n' \
                                    f'На последнем проходе произошла ошибка:\n' \
                                    f'{product_status}\n'
                else:
                    review_count = product_status['review_count']
                    keywords_count = product_status['identifier_words_count']
                    reply_message = f'{num}. {asin}\n' \
                                    f'По результатам последнего прохода:\n' \
                                    f'Количество отзывов - {review_count}\n' \
                                    f'Количество слов-определителей - {keywords_count}'
            else:
                reply_message = f'Мониторинг для товара {asin[-10:]} еще не запускался. Состояние неизвестно'
            await monitor_bot.send_message(message.chat.id, reply_message)
    else:
        await monitor_bot.send_message(message.chat.id, 'Товаров на отслеживании нет')


@login_required
@bot_dispatcher.message_handler(commands="stop")
async def task_stop(message: types.Message):
    task = tasks.get('parser')
    if not task:
        await message.reply('Мониторинг не запущен')
    else:
        task.cancel()
        tasks['parser'] = None
        await message.reply('Мониторинг остановлен')


@login_required
@bot_dispatcher.message_handler(commands=['add'])
async def process_add_command(message: types.Message):
    params = message.text.split(' ')[1:]
    if params[0] in AMZ_MARKETS.keys() and len(params[1]) == 10:
        AMZ_URLS.append(f'{AMZ_MARKETS[params[0]]}{params[1]}')
        with open('data_files\\asins.json', 'r', encoding='utf-8') as stat:
            statuses = json.load(stat)
            if 'asins' in statuses.keys():
                check_list = set(statuses['asins'])
                check_list.add(f'{AMZ_MARKETS[params[0]]}{params[1]}')
                statuses['asins'] = list(check_list)
            else:
                statuses.setdefault('asins', [f'{AMZ_MARKETS[params[0]]}{params[1]}'])
        with open('data_files\\asins.json', 'w', encoding='utf-8') as stat:
            json.dump(statuses, stat)
        await message.reply('Принято')
    else:
        await message.reply('Переданные параметры не соответствуют формату команды')


@bot_dispatcher.message_handler(commands=['password'])
async def process_password_command(message: types.Message):
    password = message.text.split(' ')[1]
    if password == BOT_PASSWORD:
        USERS.setdefault(str(len(USERS_ID) + 1), message.chat.id)
        with open('data_files\\users.json', 'w', encoding='utf-8') as users:
            json.dump(USERS, users)
        await message.reply('Доступ открыт')
        await monitor_bot.send_message(message.chat.id, HELP_MESSAGE)
    elif password == BOT_WATCH_PASSWORD:
        W_USERS.setdefault(str(len(W_USERS_ID) + 1), message.chat.id)
        with open('data_files\\watch_users.json', 'w', encoding='utf-8') as users:
            json.dump(W_USERS, users)
        await message.reply('Открыт доступ к просмотру сообщений')
    else:
        await message.reply('Вы не являетесь разрешенным пользователем сервиса')


@login_required
@bot_dispatcher.message_handler(commands=['del'])
async def process_del_command(message: types.Message):
    params = message.text.split(' ')[1:]
    if params[0] in AMZ_MARKETS.keys() and len(params[1]) == 10:
        product = f'{AMZ_MARKETS[params[0]]}{params[1]}'
        if product in AMZ_URLS:
            AMZ_URLS.remove(product)
        with open('data_files\\asins.json', 'r', encoding='utf-8') as stat:
            statuses = json.load(stat)
        if 'asins' in statuses.keys():
            check_list = statuses['asins']
            if product in check_list:
                check_list.remove(product)
                statuses['asins'] = check_list
                with open('data_files\\asins.json', 'w', encoding='utf-8') as stat:
                    json.dump(statuses, stat)
                await message.reply(f'Товар {params[1]} удален из списка отслеживаемых товаров')
            else:
                await monitor_bot.send_message(message.chat.id, 'Такого товара на отслеживании нет')
        else:
            await monitor_bot.send_message(message.chat.id, 'Товаров на отслеживании нет')

    else:
        await message.reply('Переданные параметры не соответствуют формату команды')


@bot_dispatcher.message_handler()
async def echo_message(msg: types.Message):
    await monitor_bot.send_message(msg.from_user.id, 'Моя твоя не понимай!')


if __name__ == '__main__':
    executor.start_polling(bot_dispatcher)
