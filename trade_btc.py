import asyncio
import time
import fcoin
from fcoin.WebsocketClient import WebsocketClient
from threading import Thread
from datetime import datetime, timedelta
from peregrinearb import create_weighted_multi_exchange_digraph, print_profit_opportunity_for_path_multi,\
    bellman_ford_multi, best_ask, best_bid
# from ccxt import async_support as ccxt
import traceback
import sys


go_long = False
go_short = False

exit_long = False
exit_short = False

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

result = fcoin.Api().market.get_candle_info('M1', 'btcusdt')['data']
result.sort(key=lambda x: x['id'])
# print(f"{result}")

last_trades = result[:-1]

back_time_limit_seconds = 60


def filter_last_trades():
    global last_trades, back_time_limit_seconds
    millis = int(round(time.time() * 1000))
    millis_past_minute = millis - back_time_limit_seconds * 1000
    last_trades = [x for x in last_trades if x['ts'] > millis_past_minute]


ma_short_freq = 7
ma_long_freq = 30
ma_very_long_freq = 180

class HandleWebsocketTrade(WebsocketClient):

    open_price = None
    close_price = None
    last_ts = None
    last_go_long = False
    last_message = None

    is_up_ma_short = None
    is_down_ma_short = None

    is_short_cross_up = None
    is_short_cross_down = None



    def handle(self,msg):
        global go_short, go_long, last_trades, exit_long, exit_short

        # print('receive message')
        for key, value in msg.items():
            #print(f'message received', flush=True)
            if key == 'type' and value == 'ping':
                print(f'Received ping event.. connection is good', flush=True)
            elif key == 'type' and 'candle' not in value:
                print(f'event not identified:{value}')
            elif key == 'type' and value == 'candle.M1.btcusdt':
                if self.last_ts is None:
                    self.last_ts = msg['id']
                if msg['id'] > self.last_ts:

                    ma_short_before = moving_average(ma_short_freq)
                    ma_long_before = moving_average(ma_long_freq)
                    ma_very_long_before = moving_average(ma_very_long_freq)

                    short_below_long_before = ma_short_before < ma_long_before
                    short_above_long_before = ma_short_before > ma_long_before

                    last_trades.append(self.last_message)
                    last_trades = last_trades[-ma_very_long_freq-20:]

                    ma_short = moving_average(ma_short_freq)
                    ma_long = moving_average(ma_long_freq)
                    ma_very_long = moving_average(ma_very_long_freq)

                    short_below_long = ma_short < ma_long
                    short_above_long = ma_short > ma_long

                    self.is_up_ma_short = ma_short >= ma_short_before
                    self.is_down_ma_short = ma_short <= ma_short_before

                    self.is_up_ma_very_long = ma_very_long >= ma_very_long_before
                    self.is_down_ma_very_long = ma_very_long <= ma_very_long_before

                    self.is_short_cross_up = short_below_long_before and short_above_long
                    self.is_short_cross_down = short_above_long_before and short_below_long

                    go_long = short_above_long and self.is_up_ma_very_long
                    go_short = short_below_long and self.is_down_ma_very_long

                    exit_long = self.is_short_cross_down
                    exit_short = self.is_short_cross_up


                    print(f"close:{self.close_price} open:{self.open_price} ts:{self.last_ts} "
                          f"last:{self.last_message} is_up_ma_short:{self.is_up_ma_short} "
                          f"is_down_ma_short:{self.is_down_ma_short} is_short_cross_up:{self.is_short_cross_up} "
                          f"is_short_cross_down:{self.is_short_cross_down} is_up_ma_very_long:{self.is_up_ma_very_long} "
                          f"is_down_ma_very_long:{self.is_down_ma_very_long} ma_short:{ma_short} "
                          f"ma_long:{ma_long} ma_very_long:{ma_very_long}", flush=True)
                    # self.last_go_long = check_long
                    # self.last_go_short = check_short
                    self.last_ts = msg['id']
                self.last_message = msg


def moving_average(steps_back):
    global last_trades
    candles = last_trades[-steps_back:]
    candles_close_price = [x['close'] for x in candles]
    return sum(candles_close_price)/len(candles_close_price)

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

loop = asyncio.get_event_loop()

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

force_stop = False


async def change_price(order_detail, price, symbol_complete, enter_order=False):
    global force_stop
    total_amount = float(order_detail['data']['amount'])
    amount_filled = float(order_detail['data']['filled_amount'])
    new_amount = total_amount - amount_filled

    if new_amount < amount_btc_minimum:
        print(f"No good.. new amount:{new_amount} is less than minimum:{amount_btc_minimum} "
              f"will wait 30 seconds if order is filled", flush=True)
        await asyncio.sleep(30.0)
        order_detail = await get_order(order_detail['data']['id'])
        print(f"finished wait will force stop after cancel", flush=True)
        total_amount = float(order_detail['data']['amount'])
        amount_filled = float(order_detail['data']['filled_amount'])
        new_amount = total_amount - amount_filled
        if new_amount < amount_btc_minimum:
            print(f"After wait 30 seconds amount not reach minimum {new_amount} {amount_btc_minimum}.. force finish")
            force_stop = True

    response_cancel = await cancel_order(order_detail['data']['id'])
    print(f"response_cancel:{response_cancel}")

    while order_detail['data']['state'] not in ['canceled', 'partial_canceled', 'filled']:
        await asyncio.sleep(1.0)
        order_detail = await get_order(order_detail['data']['id'])

    if order_detail['data']['state'] == 'filled':
        print(f"when trying to change price.. order is already filled")
        return None
    elif enter_order:
        print(f"Since this order is an entry order.. will cancel and not change price..")
        force_stop = True
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


async def change_best_price(order_detail, symbol_complete, enter_order=False):
    order_book_inst = await order_book(order_detail['data']['symbol'])
    if order_detail['data']['side'] == 'sell':
        price = order_book_inst['bids'][0][0]
    else:
        price = order_book_inst['asks'][0][0]
    if price != float(order_detail['data']['price']):
        print(f"price is different from order:{price} {float(order_detail['data']['price'])}")
        new_order = await change_price(order_detail, price, symbol_complete, enter_order=enter_order)
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


async def check_log_entry(log_entry, orders_detail, enter_order=False):
    only_symbol_and_side = [x for x in orders_detail
                            if x['data']['symbol'] == log_entry['symbol']
                            and x['data']['side'] == log_entry['side']]
    new_order = await change_best_price(only_symbol_and_side[-1],
                                        log_entry['symbol_complete'], enter_order)
    return new_order


wait_seconds_time = 2


def submit_orders_simulation(log_orders):
    global loop, wait_seconds_time, force_stop

    currencies_balance = {}
    for a_log_order in log_orders:
        print(f"a_log_order:{a_log_order}")
        base_currency, quote_currency = a_log_order['symbol_complete'].split('/')

        if a_log_order['side'] == 'buy':

            start = quote_currency
            end = base_currency
            total = a_log_order['amount'] * a_log_order['price']
            print(f"currencies_balance[start] = {currencies_balance.get(start, 0.0)} - {total}")
            currencies_balance[start] = currencies_balance.get(start, 0.0) - total
            print(f"currencies_balance[start]:{currencies_balance[start]}")

            end_amount = a_log_order['amount']
            currencies_balance[end] = currencies_balance.get(end, 0.0) + end_amount
            print(f"currencies_balance[end]:{currencies_balance[end]}")

        else:
            end = quote_currency
            start = base_currency
            filled_amount = a_log_order['amount']
            currencies_balance[start] = currencies_balance.get(start, 0.0) - filled_amount
            print(f"currencies_balance[start]:{currencies_balance[start]}")

            end_amount = a_log_order['amount'] * a_log_order['price']
            currencies_balance[end] = currencies_balance.get(end, 0.0) + end_amount
            print(f"currencies_balance[end]:{currencies_balance[end]}")
    print(f"Final balances:{currencies_balance}", flush=True)
    return currencies_balance


def submit_orders_arb(log_orders, enter_order=False):
    global loop, wait_seconds_time, force_stop
    orders_id = release_all_new_orders(log_orders)

    print(f"Release all orders:{orders_id}")
    print(f"wait_seconds_time:{wait_seconds_time}")
    time.sleep(wait_seconds_time)
    orders_details = get_details_orders(orders_id)
    print(f"Orders details:{orders_details}", flush=True)

    while not all(check_log_item_amount(item, orders_details) for item in log_orders) and not force_stop:
        not_total_filled = [x for x in log_orders if not check_log_item_amount(x, orders_details)]
        print(f"not_total_filled:{not_total_filled}")

        result_new_orders = []
        for x in not_total_filled:
            try:
                new_order = loop.run_until_complete(check_log_entry(x, orders_details, enter_order=enter_order))
                if new_order is not None:
                    print(f"Adding new order:{new_order}")
                    result_new_orders.append(new_order)
            except Exception as ex:
                print(f"Error:{ex} will not stop..")

        print(f"result_new_orders:{result_new_orders}", flush=True)
        result_new_orders = [x['data'] for x in result_new_orders]
        orders_id.extend(result_new_orders)
        print(f"orders_id:{orders_id}")
        orders_details = get_details_orders(orders_id)
        print(f"orders_details:{orders_details}")
        print(f"will wait some time now:{wait_seconds_time} seconds", flush=True)
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


async def pairs_decimal_fcoin():
    currencies = fcoin.Api().symbols()
    result_dict = {}

    for x in currencies['data']:
        result_dict[f"{x['base_currency'].upper()}/{x['quote_currency'].upper()}"] = x['amount_decimal']
    return result_dict


async def order_book(a_pair):
    # print('start:{}'.format(a_pair))
    # index = exchange_names.index(exchange_name)
    key = f"{a_pair.replace('/','').lower()}"
    ask_price = best_ask[key]['price']
    ask_qtd = best_ask[key]['qtd']

    bid_price = best_bid[key]['price']
    bid_qtd = best_bid[key]['qtd']
    return {'bids':[[bid_price, bid_qtd]], 'asks':[[ask_price, ask_qtd]]}


all_pairs_decimal = loop.run_until_complete(pairs_decimal_fcoin())

all_pairs = ['BTC/USDT']

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
    "args": ["candle.M1.btcusdt"]
}

simulation_flag = False


def simulation():
    print(f"Starting simulaton waiting 10s")
    time.sleep(10)
    last_trades.reverse()
    for msg in last_trades[200:]:
        # print(f"{msg}")
        best_bid['btcusdt'] = {'price': msg['open'], 'qtd': 0}
        best_ask['btcusdt'] = {'price': msg['open'], 'qtd': 0}
        msg['type'] = 'candle.M1.btcusdt'
        ws2.handle(msg)
        time.sleep(1)


if not simulation_flag:
    sub = ws.sub
    sub2 = ws2.sub

    Thread(target=sub,args=(topics,)).start()
    Thread(target=sub2,args=(topics_trades,)).start()

else:
    last_trades = []

    start_date = datetime(2019, 10, 1)

    result = fcoin.Api().market.get_candle_info('M1', 'btcusdt')['data']
    last_trades.extend(result)
    last_time_seconds = result[-1]['id']

    while last_time_seconds > start_date.timestamp() and len(result) > 1:
        result = fcoin.Api().market.get_candle_info_before('M1', 'btcusdt', last_time_seconds)['data']
        if len(result) > 1:
            last_time_seconds = result[-1]['id']
            result = result[1:]
            last_trades.extend(result)
            print(f"result:{result[0]['id']}")

    Thread(target=simulation,args=()).start()



# fee = 1 - exchange.fees['trading']['taker']
# fee = 1 - 0.0006

profit_acc = 0.0
pair_to_remove = []
amount_btc_minimum = 0.005

# starting...
# print(f"Waiting {back_time_limit_seconds} seconds to store power")
# time.sleep(back_time_limit_seconds)

last_show_status = datetime.now()

wait_time_until_finish_seconds = 50

while True:
    try:
        force_stop = False
        symbol_use = 'BTC/USDT'
        symbol_transformed = f"{symbol_use.replace('/', '').lower()}"
        # indicator = power_trades()
        # amplitude_value = amplitude()

        # print(last_trades)
        if datetime.now() > last_show_status + timedelta(seconds=20):
            print(f"last_trades:{[x['id'] for x in last_trades[-3:]]}")
            print(f"MA 7:{moving_average(ma_short_freq)} MA 30:{moving_average(ma_long_freq)}")
        #     print(f"indicator:{indicator} amplitude_value:{amplitude_value} {len(last_trades)} "
        #                  f"{datetime.fromtimestamp(last_trades[0]['ts']//1000)}"
        #                  f" {datetime.fromtimestamp(last_trades[-1]['ts']//1000)} "
        #                  f"{last_trades[0]['price']} {last_trades[-1]['price']}\n", flush=True)
            last_show_status = datetime.now()
        if go_long:
            go_long = False
            print(f"starting a long", flush=True)
            order_book_result = loop.run_until_complete(order_book(symbol_use))
            log_order = [{'side': 'buy', 'symbol': symbol_transformed,
                                    'amount': amount_btc_minimum,
                                    'price': order_book_result['asks'][0][0],
                                    'symbol_complete': symbol_use}]
            if not simulation_flag:
                balance_result_buy = submit_orders_arb(log_order)
            else:
                balance_result_buy = submit_orders_simulation(log_order)
            print(balance_result_buy)

            if force_stop:
                print(f"Restarting loop since force stop is true")
                continue

            while not exit_long:
                pass

            order_book_result = loop.run_until_complete(order_book(symbol_use))
            log_order = [{'side': 'sell', 'symbol': symbol_transformed,
                          'amount': amount_btc_minimum,
                          'price': order_book_result['bids'][0][0],
                          'symbol_complete': symbol_use}]

            if not simulation_flag:
                balance_result_sell = submit_orders_arb(log_order)
            else:
                balance_result_sell = submit_orders_simulation(log_order)
            print(balance_result_sell)

            profit_iteration = balance_result_buy['USDT'] + balance_result_sell['USDT']

            profit_acc += profit_iteration

            print(f"Final result is:{profit_iteration} profit_acc:{profit_acc}")
            # sys.exit()

        elif go_short:
            go_short = False
            print(f"starting a short", flush=True)
            order_book_result = loop.run_until_complete(order_book(symbol_use))
            log_order = [{'side': 'sell', 'symbol': symbol_transformed,
                          'amount': amount_btc_minimum,
                          'price': order_book_result['bids'][0][0],
                          'symbol_complete': symbol_use}]

            if not simulation_flag:
                balance_result_sell = submit_orders_arb(log_order)
            else:
                balance_result_sell = submit_orders_simulation(log_order)
            print(balance_result_sell)

            if force_stop:
                print(f"Restarting loop since force stop is true")
                continue

            while not exit_short:
                pass

            order_book_result = loop.run_until_complete(order_book(symbol_use))
            log_order = [{'side': 'buy', 'symbol': symbol_transformed,
                          'amount': amount_btc_minimum,
                          'price': order_book_result['asks'][0][0],
                          'symbol_complete': symbol_use}]

            if not simulation_flag:
                balance_result_buy = submit_orders_arb(log_order)
            else:
                balance_result_buy = submit_orders_simulation(log_order)
            print(balance_result_buy)

            profit_iteration = balance_result_buy['USDT'] + balance_result_sell['USDT']
            profit_acc += profit_iteration

            print(f"Final result is:{profit_iteration} profit_acc:{profit_acc}")
        #     # sys.exit()

    except Exception as ex:
        print(ex, flush=True)
        traceback.print_exc()
        sys.exit()

