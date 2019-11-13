import asyncio
import time
import fcoin
from fcoin.WebsocketClient import WebsocketClient
from threading import Thread
from datetime import datetime, timedelta
from peregrinearb import create_weighted_multi_exchange_digraph, print_profit_opportunity_for_path_multi,\
    bellman_ford_multi, best_ask, best_bid
from ccxt import async_support as ccxt
import traceback
import sys




class HandleWebsocket(WebsocketClient):
    def handle(self,msg):
        # print('receive message')
        symbol = None
        ask_price = None
        ask_qtd = None
        bid_price = None
        bid_qtd = None
        for key, value in msg.items():
            if key == 'type' and value == 'ping':
                print(f'Received ping event.. connection is good', flush=True)
            elif key == 'type' and 'depth' not in value:
                print(f'event not identified:{value}')
            if key == 'type':
                symbol = value.replace("depth.L20.", "")
            if key == 'bids':
                best_bid[symbol] = value[0]
                bid_price = value[0]
                bid_qtd = value[1]
            if key == 'asks':
                ask_price = value[0]
                ask_qtd = value[1]
                best_ask[symbol] = value[0]
        best_bid[symbol] = {'price': bid_price, 'qtd': bid_qtd}
        best_ask[symbol] = {'price': ask_price, 'qtd': ask_qtd}


last_trades = []
back_time_limit_seconds = 60


def filter_last_trades():
    global last_trades, back_time_limit_seconds
    millis = int(round(time.time() * 1000))
    millis_past_minute = millis - back_time_limit_seconds * 1000
    last_trades = [x for x in last_trades if x['ts'] > millis_past_minute]


class HandleWebsocketTrade(WebsocketClient):
    def handle(self,msg):

        # print('receive message')
        for key, value in msg.items():
            if key == 'type' and value == 'ping':
                print(f'Received ping event.. connection is good', flush=True)
            elif key == 'type' and 'trade' not in value:
                print(f'event not identified:{value}')
            elif key == 'type' and value == 'trade.btcusdt':
                last_trades.append(msg)
                filter_last_trades()


def power_trades():
    global last_trades
    if len(last_trades) > 0:
        buy_trades = [x['amount'] for x in last_trades if x['side'] == 'buy']
        sell_trades = [x['amount'] for x in last_trades if x['side'] == 'sell']

        sum_amount_buy = sum(buy_trades)
        sum_amount_sell = sum(sell_trades)

        return sum_amount_buy/(sum_amount_buy + sum_amount_sell)
    else:
        return 0.50


def amplitude():
    global last_trades
    if len(last_trades) > 0:
        first = last_trades[0]['price']
        last = last_trades[-1]['price']

        return last/first
    else:
        return 0.0


ws = HandleWebsocket()
ws2 = HandleWebsocketTrade()

# time.sleep(10)
# ws.close()

fee_config = {
    'binance': 0.0006,
    'fcoin': {
        'fee': 0.0,
        'ZEC': 0.0003,
        'ETC': 0.0003,
        'TRX': 0.0003,
        'XLM': 0.0003,
        'PAX/USDT': 0.0003,
        'TUSD/USDT': 0.0003,
        'USDC/USDT': 0.0003
    },
    'hitbtc2': 0.0007,
    'hitbtc': 0.0007
}

exchange_names_input = 'fcoin'

key_fcoin = sys.argv[1]
secret_fcoin = sys.argv[2]
exchange_names = exchange_names_input.split(',')

print('Using exchanges:{}'.format(exchange_names))

symbols_watch = ['BTC', 'USDT']

remove_pairs = []

exchange_list = [{'object': getattr(ccxt, exchange_name)(),
                  'fee': fee_config[exchange_name]} for exchange_name in exchange_names]
loop = asyncio.get_event_loop()

for exchange_name in exchange_names:
    loop.run_until_complete(exchange_list[exchange_names.index(exchange_name)]['object'].load_markets())

api_auth = fcoin.authorize(key_fcoin, secret_fcoin)


async def create_order(symbol, side, price, amount):
    symbol_transformed = f"{symbol.replace('/', '').lower()}"

    if all_pairs_decimal[symbol] == 0:
        amount_str = str(int(amount))
    else:
        amount_str = str(amount)

    order_create_param = fcoin.order_create_param(symbol_transformed, side, 'limit', str(price), amount_str)
    result = api_auth.orders.create(order_create_param)
    print(result)
    return result


async def get_order(order_id):
    return api_auth.orders.get(order_id)


async def cancel_order(order_id):
    return api_auth.orders.submit_cancel(order_id)


async def change_price(order_detail, price, symbol_complete):

    response_cancel = await cancel_order(order_detail['data']['id'])
    total_amount = float(order_detail['data']['amount'])
    amount_filled = float(order_detail['data']['filled_amount'])
    new_amount = total_amount - amount_filled

    if new_amount < amount_btc_minimum:
        print(f"No good.. new amount:{new_amount} is less than minimum:{amount_btc_minimum} "
              f"will wait 30 seconds if order is filled", flush=True)
        await asyncio.sleep(30.0)
        order_detail = await get_order(order_detail['data']['id'])
        print(f"finished wait", flush=True)

    print(f"response_cancel:{response_cancel}")

    while order_detail['data']['state'] not in ['canceled', 'partial_canceled', 'filled']:
        await asyncio.sleep(1.0)
        order_detail = await get_order(order_detail['data']['id'])

    if order_detail['data']['state'] == 'filled':
        print(f"when trying to change price.. order is already filled")
        return None
    else:
        total_amount = float(order_detail['data']['amount'])
        amount_filled = float(order_detail['data']['filled_amount'])
        new_amount = total_amount - amount_filled
        if new_amount < amount_btc_minimum:
            print(f"No good.. new amount:{new_amount} is less than minimum:{amount_btc_minimum} "
                  f"not able to release new order", flush=True)
            return None
        else:
            print(f"Changing price with new order price:{price} and amount:{new_amount}")
            return await create_order(symbol_complete, order_detail['data']['side'],
                                  price, new_amount)
    #else:
     #   raise Exception(f"Error in cancelling order.. {response_cancel}")


async def change_best_price(order_detail, symbol_complete):
    order_book_inst = await order_book(order_detail['data']['symbol'])
    if order_detail['data']['side'] == 'sell':
        price = order_book_inst['bids'][0][0]
    else:
        price = order_book_inst['asks'][0][0]
    if price != float(order_detail['data']['price']):
        print(f"price is different from order:{price} {float(order_detail['data']['price'])}")
        new_order = await change_price(order_detail, price, symbol_complete)
    else:
        print(f"Order price is equal {price} {float(order_detail['data']['price'])}")
        new_order = None
    return new_order


def release_all_new_orders(log_orders):
    global loop
    new_orders = [create_order(x['symbol_complete'], x['side'], x['price'], x['amount']) for x in log_orders]
    result_new_orders = loop.run_until_complete(asyncio.gather(*new_orders))
    return [x['data'] for x in result_new_orders]


def get_details_orders(orders):
    global loop
    orders_detail = [get_order(x) for x in orders]
    result_orders_detail = loop.run_until_complete(asyncio.gather(*orders_detail))
    return result_orders_detail


def check_log_item_amount(log_entry, orders_details):
    only_symbol_and_side = [float(x['data']['filled_amount']) for x in orders_details
                            if x['data']['symbol'] == log_entry['symbol']
                            and x['data']['side'] == log_entry['side']]
    print(f"log_entry:{log_entry}"
          f"filtered:{only_symbol_and_side} sum:{sum(only_symbol_and_side)} log_entry['amount']:{log_entry['amount']}")

    return sum(only_symbol_and_side) == log_entry['amount']


async def check_log_entry(log_entry, orders_detail):
    only_symbol_and_side = [x for x in orders_detail
                            if x['data']['symbol'] == log_entry['symbol']
                            and x['data']['side'] == log_entry['side']]
    new_order = await change_best_price(only_symbol_and_side[-1],
                                        log_entry['symbol_complete'])
    return new_order


wait_seconds_time = 2


def submit_orders_arb(log_orders):
    global loop, wait_seconds_time
    orders_id = release_all_new_orders(log_orders)

    print(f"Release all orders:{orders_id}")
    print(f"wait_seconds_time:{wait_seconds_time}")
    time.sleep(wait_seconds_time)
    orders_details = get_details_orders(orders_id)
    print(f"Orders details:{orders_details}")

    while not all(check_log_item_amount(item, orders_details) for item in log_orders):
        not_total_filled = [x for x in log_orders if not check_log_item_amount(x, orders_details)]
        print(f"not_total_filled:{not_total_filled}")

        result_new_orders = []
        for x in not_total_filled:
            try:
                new_order = loop.run_until_complete(check_log_entry(x, orders_details))
                if new_order is not None:
                    print(f"Adding new order:{new_order}")
                    result_new_orders.append(new_order)
            except Exception as ex:
                print(f"Error:{ex} will not stop..")

        print(f"result_new_orders:{result_new_orders}")
        result_new_orders = [x['data'] for x in result_new_orders]
        orders_id.extend(result_new_orders)
        print(f"orders_id:{orders_id}")
        orders_details = get_details_orders(orders_id)
        print(f"orders_details:{orders_details}")
        print(f"will wait some time now:{wait_seconds_time} seconds")
        time.sleep(wait_seconds_time)

    currencies_balance = {}
    for a_log_order in log_orders:
        print(f"a_log_order:{a_log_order}")
        base_currency, quote_currency = a_log_order['symbol_complete'].split('/')
        orders = [x for x in orders_details
                  if x['data']['symbol'] == a_log_order['symbol'] and x['data']['side'] == a_log_order['side']]
        print(f"orders found:{orders}")
        if a_log_order['side'] == 'buy':

            start = quote_currency
            end = base_currency
            total = sum([float(x['data']['filled_amount']) * float(x['data']['price']) for x in orders])
            print(f"currencies_balance[start] = {currencies_balance.get(start, 0.0)} - {total}")
            currencies_balance[start] = currencies_balance.get(start, 0.0) - total
            print(f"currencies_balance[start]:{currencies_balance[start]}")

            end_amount = sum([float(x['data']['filled_amount']) for x in orders]) - sum([float(x['data']['fill_fees']) for x in orders])
            currencies_balance[end] = currencies_balance.get(end, 0.0) + end_amount
            print(f"currencies_balance[end]:{currencies_balance[end]}")

        else:
            end = quote_currency
            start = base_currency
            filled_amount = sum([float(x['data']['filled_amount']) for x in orders])
            currencies_balance[start] = currencies_balance.get(start, 0.0) - filled_amount
            print(f"currencies_balance[start]:{currencies_balance[start]}")

            end_amount = sum([float(x['data']['filled_amount']) * float(x['data']['price']) for x in orders]) - sum([float(x['data']['fill_fees']) * float(x['data']['price']) for x in orders])
            currencies_balance[end] = currencies_balance.get(end, 0.0) + end_amount
            print(f"currencies_balance[end]:{currencies_balance[end]}")
    print(f"Final balances:{currencies_balance}", flush=True)
    return currencies_balance
    #sys.exit()


async def pairs():
    global loop
    all_symbols = []
    for exchange_name in exchange_names:
        index = exchange_names.index(exchange_name)
        symbols = [x for x in exchange_list[index]['object'].symbols if x not in remove_pairs]
        all_symbols = list(set().union(all_symbols, symbols))
    return all_symbols


async def pairs_decimal_fcoin():
    currencies = fcoin.Api().symbols()
    result_dict = {}

    for x in currencies['data']:
        result_dict[f"{x['base_currency'].upper()}/{x['quote_currency'].upper()}"] = x['amount_decimal']
    return result_dict


async def pairs_usdt():
    # binance_ex = getattr(ccxt, 'binance')()
    hitbtc_ex = getattr(ccxt, 'hitbtc2')()
    # tickers_binance = await binance_ex.fetch_tickers()
    tickers_hitbtc = await hitbtc_ex.fetch_tickers()
    # tickers = list(tickers_binance.items()) + list(tickers_hitbtc.items())
    tickers = list(tickers_hitbtc.items())
    return [x for x in tickers if 'USDT' in x[0] and x[0] not in remove_pairs]


async def order_book(a_pair):
    # print('start:{}'.format(a_pair))
    # index = exchange_names.index(exchange_name)
    key = f"{a_pair.replace('/','').lower()}"
    ask_price = best_ask[key]['price']
    ask_qtd = best_ask[key]['qtd']

    bid_price = best_bid[key]['price']
    bid_qtd = best_bid[key]['qtd']
    return {'bids':[[bid_price, bid_qtd]], 'asks':[[ask_price, ask_qtd]]}


all_pairs = loop.run_until_complete(pairs())

all_pairs_decimal = loop.run_until_complete(pairs_decimal_fcoin())

all_pairs_pre_fetch = [x for x in all_pairs
                       if x.split('/')[0] in symbols_watch and x.split('/')[1] in symbols_watch]
all_pairs_topics = [f"depth.L20.{x.split('/')[0].lower()}{x.split('/')[1].lower()}" for
                    x in all_pairs_pre_fetch]

print(all_pairs_topics)


topics = {
    "id": "tickers",
    "cmd": "sub",
    "args": all_pairs_topics,
}

topics_trades = {
    "id": "trades",
    "cmd": "sub",
    "args": ['trade.btcusdt']
}


sub = ws.sub
sub2 = ws2.sub

Thread(target=sub,args=(topics,)).start()
Thread(target=sub2,args=(topics_trades,)).start()

all_pairs_usdt = loop.run_until_complete(pairs_usdt())
# fee = 1 - exchange.fees['trading']['taker']
# fee = 1 - 0.0006

print([exchange['fee'] for exchange in exchange_list])

profit_acc = 0.0
pair_to_remove = []
amount_btc_minimum = 0.005

# starting...
print(f"Waiting {back_time_limit_seconds} seconds to store power")
time.sleep(back_time_limit_seconds)

last_show_status = datetime.now()

while True:
    try:
        symbol_use = 'BTC/USDT'
        symbol_transformed = f"{symbol_use.replace('/', '').lower()}"
        amplitude_value = amplitude()

        # print(last_trades)
        if datetime.now() > last_show_status + timedelta(seconds=5):
            print(f"amplitude_value:{amplitude_value} {len(last_trades)} "
                         f"{datetime.fromtimestamp(last_trades[0]['ts']//1000)}"
                         f" {datetime.fromtimestamp(last_trades[-1]['ts']//1000)} "
                         f"{last_trades[0]['price']} {last_trades[-1]['price']}\n", flush=True)
            last_show_status = datetime.now()
        if amplitude_value > 1.0003:

            print(f"starting a long {amplitude_value}")
            order_book_result = loop.run_until_complete(order_book(symbol_use))
            log_order = [{'side': 'buy', 'symbol': symbol_transformed,
                                    'amount': amount_btc_minimum,
                                    'price': order_book_result['asks'][0][0],
                                    'symbol_complete': symbol_use}]

            balance_result_buy = submit_orders_arb(log_order)
            print(balance_result_buy)

            time.sleep(60)

            order_book_result = loop.run_until_complete(order_book(symbol_use))
            log_order = [{'side': 'sell', 'symbol': symbol_transformed,
                          'amount': amount_btc_minimum,
                          'price': order_book_result['bids'][0][0],
                          'symbol_complete': symbol_use}]

            balance_result_sell = submit_orders_arb(log_order)
            print(balance_result_sell)

            profit_iteration = balance_result_buy['USDT'] + balance_result_sell['USDT']

            profit_acc += profit_iteration

            print(f"Final result is:{profit_iteration} profit_acc:{profit_acc}")
            # sys.exit()

        elif amplitude_value < 0.9997:

            print(f"starting a short {amplitude_value}")
            order_book_result = loop.run_until_complete(order_book(symbol_use))
            log_order = [{'side': 'sell', 'symbol': symbol_transformed,
                          'amount': amount_btc_minimum,
                          'price': order_book_result['bids'][0][0],
                          'symbol_complete': symbol_use}]

            balance_result_sell = submit_orders_arb(log_order)
            print(balance_result_sell)

            time.sleep(60)

            order_book_result = loop.run_until_complete(order_book(symbol_use))
            log_order = [{'side': 'buy', 'symbol': symbol_transformed,
                          'amount': amount_btc_minimum,
                          'price': order_book_result['asks'][0][0],
                          'symbol_complete': symbol_use}]

            balance_result_buy = submit_orders_arb(log_order)
            print(balance_result_buy)

            profit_iteration = balance_result_buy['USDT'] + balance_result_sell['USDT']
            profit_acc += profit_iteration

            print(f"Final result is:{profit_iteration} profit_acc:{profit_acc}")
        #     # sys.exit()


    except Exception as ex:
        print(ex, flush=True)
        traceback.print_exc()
        sys.exit()

