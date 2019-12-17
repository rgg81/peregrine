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
import random
import ccxt
import pandas as pd
from collections import defaultdict
import os
import pickle
import json

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


ma_short_freq = 2 # 5, 21, 173
ma_long_freq = 10
ma_very_long_freq = 340
stop_loss_percent = 0.3

if os.path.exists('best.pickle'):
    with open('best.pickle','rb') as file:
        best_mas = pickle.load(file)
        ma_short_freq, ma_long_freq, ma_very_long_freq, stop_loss_percent = best_mas[-1][0]
        print(f"recovered best configs.. using best:{best_mas[-1]}")


assert len(last_trades) >= ma_very_long_freq, f"{len(last_trades)} NOT >= {ma_very_long_freq}"
stop_gain = True
stop_gain_rates = [x * 0.5 for x in range(1, 10)]


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

    stop_loss_price_up = None
    stop_loss_price_down = None
    enter_ref_price_up = None
    enter_ref_price_down = None



    def handle(self,msg):
        global go_short, go_long, last_trades, exit_long, exit_short, simulation_flag

        # print('receive message')
        for key, value in msg.items():
            #print(f'message received', flush=True)
            if key == 'type' and value == 'ping':
                print(f'Received ping event.. connection is good', flush=True)
            elif key == 'type' and 'candle' not in value:
                print(f'event not identified:{value}')
            elif key == 'type' and value == 'candle.M1.btcusdt':

                if simulation_flag:
                    stop_loss_up_bool = self.stop_loss_price_up is not None and self.last_message[
                        'low'] <= self.stop_loss_price_up

                    if stop_loss_up_bool:
                        best_bid['btcusdt'] = {'price': round(self.stop_loss_price_up, 1), 'qtd': 0}
                        best_ask['btcusdt'] = {'price': round(self.stop_loss_price_up, 1), 'qtd': 0}

                    stop_loss_down_bool = self.stop_loss_price_down is not None and self.last_message[
                        'high'] >= self.stop_loss_price_down
                    if stop_loss_down_bool:
                        best_bid['btcusdt'] = {'price': round(self.stop_loss_price_down, 1), 'qtd': 0}
                        best_ask['btcusdt'] = {'price': round(self.stop_loss_price_down, 1), 'qtd': 0}
                else:
                    order_book_result = order_book(symbol_use)
                    stop_loss_up_bool = self.stop_loss_price_up is not None and order_book_result['bids'][0][
                        0] <= self.stop_loss_price_up
                    stop_loss_down_bool = self.stop_loss_price_down is not None and order_book_result['asks'][0][
                        0] >= self.stop_loss_price_down

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

                    stop_gain_up_bool = stop_gain and any([self.enter_ref_price_up is not None and
                                         self.last_message['open'] > (1 + x/100) * self.enter_ref_price_up > self.last_message['close']
                                         for x in stop_gain_rates])

                    stop_gain_down_bool = stop_gain and any([self.enter_ref_price_down is not None and
                                                            self.last_message['open'] < (1 - x/100) * self.enter_ref_price_down < self.last_message['close']
                                                            for x in stop_gain_rates])

                    ma_short = moving_average(ma_short_freq)
                    ma_long = moving_average(ma_long_freq)
                    ma_very_long = moving_average(ma_very_long_freq)

                    short_below_long = ma_short < ma_long
                    short_above_long = ma_short > ma_long

                    self.is_up_ma_short = ma_short >= ma_short_before
                    self.is_down_ma_short = ma_short <= ma_short_before

                    self.is_up_ma_very_long = ma_very_long < ma_long and ma_very_long < ma_short and ma_very_long >= ma_very_long_freq
                    self.is_down_ma_very_long = ma_very_long > ma_long and ma_very_long > ma_short and ma_very_long <= ma_very_long_before

                    self.is_short_cross_up = short_below_long_before and short_above_long
                    self.is_short_cross_down = short_above_long_before and short_below_long

                    # stop_loss_up_bool = False
                    # stop_loss_down_bool = False

                    if stop_loss_up_bool:
                        print(f"Found stop loss up:{self.stop_loss_price_up} "
                              f"{self.last_message} {self.last_message['close']}")

                    if stop_loss_down_bool:
                        print(f"Found stop loss down:{self.stop_loss_price_down} "
                              f"{self.last_message} {self.last_message['close']}")

                    if stop_gain_up_bool:
                        print(f"Found stop gain up:{self.last_message['open']} "
                              f"{self.last_message['close']}")

                    if stop_gain_down_bool:
                        print(f"Found stop gain down:{self.last_message['open']} "
                              f"{self.last_message['close']}")

                    exit_long = self.is_short_cross_down or stop_loss_up_bool or stop_gain_up_bool
                    if exit_long:
                        self.stop_loss_price_up = None
                        self.enter_ref_price_up = None

                    exit_short = self.is_short_cross_up or stop_loss_down_bool or stop_gain_down_bool
                    if exit_short:
                        self.stop_loss_price_down = None
                        self.enter_ref_price_down = None

                    go_long = self.is_short_cross_up and short_above_long and self.is_up_ma_very_long
                    go_short = self.is_short_cross_down and short_below_long and self.is_down_ma_very_long

                    if go_long:
                        self.stop_loss_price_up = self.last_message['close'] * (1 - stop_loss_percent/100)
                        self.enter_ref_price_up = self.last_message['close']

                    if go_short:
                        self.stop_loss_price_down = self.last_message['close'] * (1 + stop_loss_percent/100)
                        self.enter_ref_price_down = self.last_message['close']

                    if not simulation_flag:
                        print(f"ts:{self.last_ts} "
                          f"last:{self.last_message} is_up_ma_short:{self.is_up_ma_short} "
                          f"is_down_ma_short:{self.is_down_ma_short} is_short_cross_up:{self.is_short_cross_up} "
                          f"is_short_cross_down:{self.is_short_cross_down} is_up_ma_very_long:{self.is_up_ma_very_long} "
                          f"is_down_ma_very_long:{self.is_down_ma_very_long} ma_short:{ma_short} "
                          f"ma_long:{ma_long} ma_very_long:{ma_very_long} \n\n", flush=True)
                    self.last_ts = msg['id']
                self.last_message = msg


def moving_average(steps_back):
    global last_trades, cache_moving_average, simulation_flag

    if simulation_flag:
        return cache_moving_average[f"{steps_back}"][f"{last_trades[-1]['id']}"]
    else:
        candles = last_trades[-steps_back:]
        candles_close_price = [x['close'] for x in candles]
        ma = sum(candles_close_price) / len(candles_close_price)
        return ma


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

    if side == 'buy':
        amount_str = str(round(amount * price, 1))

    order_create_param = fcoin.order_create_param(symbol_transformed, side, 'market', amount_str)
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

    if order_detail['data']['state'] != 'filled' and 0.0 < new_amount < amount_btc_minimum:
        print(f"No good.. new amount:{new_amount} is less than minimum:{amount_btc_minimum} "
              f"will wait 30 seconds if order is filled", flush=True)
        await asyncio.sleep(30.0)
        order_detail = await get_order(order_detail['data']['id'])
        print(f"finished wait will force stop after cancel", flush=True)
        total_amount = float(order_detail['data']['amount'])
        amount_filled = float(order_detail['data']['filled_amount'])
        new_amount = total_amount - amount_filled
        if order_detail['data']['state'] != 'filled' and 0.0 < new_amount < amount_btc_minimum:
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
    order_book_inst = order_book(order_detail['data']['symbol'])
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

    return round(sum(only_symbol_and_side),4) == log_entry['amount']


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

            total = round(a_log_order['amount'] * a_log_order['price'], 1)
            print(f"currencies_balance[start] = {currencies_balance.get(start, 0.0)} - {total}", flush=True)
            currencies_balance[start] = currencies_balance.get(start, 0.0) - total
            print(f"currencies_balance[start]:{currencies_balance[start]}", flush=True)

            end_amount = total / a_log_order['price']
            currencies_balance[end] = currencies_balance.get(end, 0.0) + end_amount
            print(f"currencies_balance[end]:{currencies_balance[end]}", flush=True)

        else:
            end = quote_currency
            start = base_currency
            filled_amount = a_log_order['amount']
            currencies_balance[start] = currencies_balance.get(start, 0.0) - filled_amount
            print(f"currencies_balance[start]:{currencies_balance[start]}", flush=True)

            end_amount = a_log_order['amount'] * a_log_order['price']
            currencies_balance[end] = currencies_balance.get(end, 0.0) + end_amount
            print(f"currencies_balance[end]:{currencies_balance[end]}", flush=True)
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

        def get_price(order_detail):
            if float(order_detail['data']['price']) <= 0.0:
                return float(order_detail['data']['executed_value']) / float(order_detail['data']['filled_amount'])
            else:
                return float(order_detail['data']['price'])

        if a_log_order['side'] == 'buy':

            start = quote_currency
            end = base_currency
            total = sum([float(x['data']['filled_amount']) * get_price(x) for x in orders])

            rate_price_close = sum([last_trades[-1]['close'] / get_price(x) for x in orders])/len(orders)
            print(f"rate enter price:{rate_price_close} close:{last_trades[-1]['close']} mean order price:{sum([get_price(x) for x in orders])/len(orders)}", flush=True)

            print(f"currencies_balance[start] = {currencies_balance.get(start, 0.0)} - {total}")
            currencies_balance[start] = currencies_balance.get(start, 0.0) - total
            print(f"currencies_balance[start]:{currencies_balance[start]}")

            end_amount = sum([float(x['data']['filled_amount']) for x in orders]) - sum([float(x['data']['fill_fees']) for x in orders])
            currencies_balance[end] = currencies_balance.get(end, 0.0) + end_amount
            print(f"currencies_balance[end]:{currencies_balance[end]}", flush=True)

        else:
            end = quote_currency
            start = base_currency
            filled_amount = sum([float(x['data']['filled_amount']) for x in orders])

            rate_price_close = sum([last_trades[-1]['close'] / get_price(x) for x in orders])/len(orders)
            print(f"rate enter price:{rate_price_close} close:{last_trades[-1]['close']} mean order price:{sum([get_price(x) for x in orders])/len(orders)}", flush=True)

            currencies_balance[start] = currencies_balance.get(start, 0.0) - filled_amount
            print(f"currencies_balance[start]:{currencies_balance[start]}")

            end_amount = sum([float(x['data']['filled_amount']) * get_price(x) for x in orders]) - sum([float(x['data']['fill_fees']) * get_price(x) for x in orders])
            currencies_balance[end] = currencies_balance.get(end, 0.0) + end_amount
            print(f"currencies_balance[end]:{currencies_balance[end]}", flush=True)
    print(f"Final balances:{currencies_balance}", flush=True)
    return currencies_balance
    #sys.exit()


async def pairs_decimal_fcoin():
    currencies = fcoin.Api().symbols()
    result_dict = {}

    for x in currencies['data']:
        result_dict[f"{x['base_currency'].upper()}/{x['quote_currency'].upper()}"] = x['amount_decimal']
    return result_dict


def order_book(a_pair):
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

simulation_flag = sys.argv[3] == 'True'
finish_trade = False
open_trade = False
cache_moving_average = defaultdict(dict)


def simulation():
    total_iterations = 50
    total_traders = 2
    global stop_gain, open_trade, cache_moving_average, stop_loss_percent, historical_trades, total_trades, finish_trade, last_trades, ma_short_freq, ma_long_freq, ma_very_long_freq, profit_acc, ws2, go_short, go_long, exit_long, exit_short

    # total_samples_opt = 216000
    # total_samples_test = 43200

    total_samples_opt = 244000
    total_samples_test = 43200

    start_row = 0
    profit_test = 0.0

    if os.path.exists("state.json"):
        with open("state.json", "r") as read_file:
            state = json.load(read_file)
            print(f"loading state..:{state}")
            start_row = state['start_row']
            profit_test = state['profit_test']

    while total_samples_opt + start_row < len(historical_trades):
        state = {
            'start_row': start_row,
            'profit_test': profit_test
        }

        with open("state.json", "w") as write_file:
            print(f"saving state..:{state}")
            json.dump(state, write_file)

        max_profit = -1000.0
        max_total_trades = None
        max_config = []

        selected_trades_opt = historical_trades[start_row:start_row + total_samples_opt]
        # selected_trades_opt = historical_trades

        for iteration_index in range(total_iterations):
            ws2 = HandleWebsocketTrade()

            while open_trade:
                exit_long, exit_short = True, True
            go_short, go_long,  exit_long, exit_short = False, False, False, False
            ma_short_freq = random.randint(2, 4)
            # ma_short_freq = 2 #3
            # ma_long_freq = random.randrange(6, 18, 1)
            ma_long_freq = random.randrange(8, 30, 1)
            # ma_long_freq = 22
            ma_very_long_freq = random.randrange(180, 580, 20)
            # ma_very_long_freq = 360
            stop_loss_percent = random.choice([0.3, 0.5])
            stop_gain = random.choice([True, False])
            # stop_loss_percent = 0.5

            cache_moving_average = defaultdict(dict)

            df = pd.DataFrame(historical_trades)

            ma_short_mean = df['close'].rolling(window=ma_short_freq).mean().values
            ma_long_mean = df['close'].rolling(window=ma_long_freq).mean().values
            ma_very_long_mean = df['close'].rolling(window=ma_very_long_freq).mean().values

            for index in range(len(historical_trades)):
                cache_moving_average[f"{ma_short_freq}"][f"{historical_trades[index]['id']}"] = ma_short_mean[index]
                cache_moving_average[f"{ma_long_freq}"][f"{historical_trades[index]['id']}"] = ma_long_mean[index]
                cache_moving_average[f"{ma_very_long_freq}"][f"{historical_trades[index]['id']}"] = ma_very_long_mean[index]

            profit_acc = 0.0
            last_trades = selected_trades_opt[:ma_very_long_freq]
            total_trades = 0
            trades = selected_trades_opt[ma_very_long_freq:]
            with open("log.txt", "a") as f:
                print(f"Starting simulaton waiting 5s:cold start {datetime.utcfromtimestamp(last_trades[0]['id'])} "
                  f"{datetime.utcfromtimestamp(last_trades[-1]['id'])} "
                  f"trade start:{datetime.utcfromtimestamp(trades[0]['id'])} "
                  f"trade end:{datetime.utcfromtimestamp(trades[-1]['id'])}", flush=True, file=f)
            trade(simulation_data={'ws2': ws2, 'trades': trades, 'max_index': len(trades),  'mode': 'train'})

            if max_total_trades is None or profit_acc > max_profit:# profit_acc/total_trades > max_profit/max_total_trades:
                max_profit = profit_acc
                max_total_trades = total_trades

            max_config.append(((ma_short_freq, ma_long_freq, ma_very_long_freq, stop_loss_percent), profit_acc))
            max_config.sort(key=lambda x: x[1])
            max_config = max_config[-total_traders:]
            with open("log.txt", "a") as f:
                print(f"End simulation: finished profit:{profit_acc} {profit_acc_btc} total trades:{total_trades} rate:{round(profit_acc/total_trades,5)} ma_short_freq:{ma_short_freq} "
                  f"ma_long_freq:{ma_long_freq} ma_very_long_freq:{ma_very_long_freq} stop_loss_percent:{stop_loss_percent} "
                      f"stop_gain:{stop_gain} iteration:{iteration_index} "
                  f"max_profit:{max_profit} max_total_trades:{max_total_trades} max rate:{round(max_profit/max_total_trades,5)} max_config:{max_config}\n\n\n", flush=True, file=f)

        with open('best.pickle','wb') as file:
            print(f"Pickling best configs")
            pickle.dump(max_config, file)

        for a_config,a_profit in max_config:

            ws2 = HandleWebsocketTrade()

            while open_trade:
                exit_long, exit_short = True, True

            go_short, go_long,  exit_long, exit_short = False, False, False, False

            print(f"Starting test with max config:{a_config}")

            ma_short_freq, ma_long_freq, ma_very_long_freq, stop_loss_percent = a_config

            cache_moving_average = defaultdict(dict)

            df = pd.DataFrame(historical_trades)

            ma_short_mean = df['close'].rolling(window=ma_short_freq).mean().values
            ma_long_mean = df['close'].rolling(window=ma_long_freq).mean().values
            ma_very_long_mean = df['close'].rolling(window=ma_very_long_freq).mean().values

            for index in range(len(historical_trades)):
                cache_moving_average[f"{ma_short_freq}"][f"{historical_trades[index]['id']}"] = ma_short_mean[index]
                cache_moving_average[f"{ma_long_freq}"][f"{historical_trades[index]['id']}"] = ma_long_mean[index]
                cache_moving_average[f"{ma_very_long_freq}"][f"{historical_trades[index]['id']}"] = ma_very_long_mean[index]

            selected_trades_test = historical_trades[total_samples_opt + start_row - ma_very_long_freq:]
            last_trades = selected_trades_test[:ma_very_long_freq]

            profit_acc = 0.0

            total_trades = 0

            time.sleep(5)

            trades = selected_trades_test[ma_very_long_freq:]
            if len(trades) > 0:
                max_index = total_samples_test if total_samples_test < len(trades) else len(trades)
                with open("log_test.txt", "a") as f:
                    print(f"Starting test waiting 5s:last_trades cold start{datetime.utcfromtimestamp(last_trades[0]['id'])} "
                          f"{datetime.utcfromtimestamp(last_trades[-1]['id'])} "
                          f"start sample test:{datetime.utcfromtimestamp(trades[0]['id'])} "
                          f"end sample test:{datetime.utcfromtimestamp(trades[max_index - 1]['id'])}", flush=True, file=f)
                trade(simulation_data={'ws2': ws2, 'trades': trades, 'max_index': max_index, 'mode': 'test'})

                profit_test += profit_acc

                with open("log_test.txt", "a") as f:
                    print(f"End test: finished profit:{profit_acc} profit test acc:{profit_test} "
                          f"total trades:{total_trades} ma_short_freq:{ma_short_freq} "
                          f"ma_long_freq:{ma_long_freq} ma_very_long_freq:{ma_very_long_freq} "
                          f"stop_loss_percent:{stop_loss_percent} stop_gain:{stop_gain} last candle:{last_trades[-1]}\n\n\n", flush=True, file=f)

        start_row += total_samples_test


profit_acc = 0.0
profit_acc_btc = 0.0
total_trades = 0
pair_to_remove = []
amount_btc_minimum = 0.0060

# starting...
# print(f"Waiting {back_time_limit_seconds} seconds to store power")
# time.sleep(back_time_limit_seconds)

last_show_status = datetime.now()

wait_time_until_finish_seconds = 50
symbol_use = 'BTC/USDT'

wait_cpu_time_seconds = 0.01

def trade(simulation_data=None):
    global force_stop, go_short, go_long, open_trade, finish_trade, profit_acc, profit_acc_btc, total_trades, exit_long, exit_short

    index = 0

    def next_msg():
        nonlocal index
        global exit_long, exit_short

        if (index >= simulation_data['max_index'] and not open_trade) or index >= len(simulation_data['trades']):
            if simulation_data['mode'] == 'test':
                with open("log_test.txt", "a") as f:
                    print(f"Finished index:{index} {simulation_data['max_index']} open_trade:{open_trade} "
                          f"len(simulation_data['trades']):{len(simulation_data['trades'])}", flush=True, file=f)
            exit_long, exit_short = True, True
            return False

        msg = simulation_data['trades'][index]
        ws2 = simulation_data['ws2']

        best_bid['btcusdt'] = {'price': msg['open'], 'qtd': 0}
        best_ask['btcusdt'] = {'price': msg['open'], 'qtd': 0}
        msg['type'] = 'candle.M1.btcusdt'
        ws2.handle(msg)

        index += 1
        return True

    while simulation_data is None or index < simulation_data['max_index']:
        try:
            force_stop = False

            symbol_transformed = f"{symbol_use.replace('/', '').lower()}"

            if simulation_flag:
                next_msg()
            if go_long:
                go_long = False
                print(f"starting a long", flush=True)
                order_book_result = order_book(symbol_use)
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
                    return

                open_trade = True

                while not exit_long:
                    if simulation_flag:
                        valid = next_msg()
                        if not valid:
                            continue
                    finish_trade = True
                    if not simulation_flag:
                        time.sleep(wait_cpu_time_seconds)
                    pass

                order_book_result = order_book(symbol_use)
                log_order = [{'side': 'sell', 'symbol': symbol_transformed,
                              'amount': abs(round(balance_result_buy['BTC'], 4)),
                              'price': order_book_result['bids'][0][0],
                              'symbol_complete': symbol_use}]

                if not simulation_flag:
                    balance_result_sell = submit_orders_arb(log_order)
                else:
                    balance_result_sell = submit_orders_simulation(log_order)
                print(balance_result_sell)

                profit_iteration = balance_result_buy['USDT'] + balance_result_sell['USDT']
                profit_iteration_btc = balance_result_buy['BTC'] + balance_result_sell['BTC']

                profit_acc += profit_iteration
                profit_acc_btc += profit_iteration_btc

                profit_acc += profit_iteration_btc * last_trades[-1]['close']

                total_trades += 1

                open_trade = False

                print(f"Final result is:{profit_iteration} profit_acc:{profit_acc} add btc to usdt:{profit_iteration_btc * last_trades[-1]['close']} profit_acc_btc:{profit_acc_btc} profit_iteration_btc:{profit_iteration_btc} total_trades:{total_trades}\n\n",
                      flush=True)
                # sys.exit()

            elif go_short:
                go_short = False
                print(f"starting a short", flush=True)
                order_book_result = order_book(symbol_use)
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
                    return

                open_trade = True

                while not exit_short:
                    if simulation_flag:
                        valid = next_msg()
                        if not valid:
                            continue
                    finish_trade = True
                    if not simulation_flag:
                        time.sleep(wait_cpu_time_seconds)
                    pass

                order_book_result = order_book(symbol_use)
                log_order = [{'side': 'buy', 'symbol': symbol_transformed,
                              'amount': abs(round(balance_result_sell['BTC'], 4)),
                              'price': order_book_result['asks'][0][0],
                              'symbol_complete': symbol_use}]

                if not simulation_flag:
                    balance_result_buy = submit_orders_arb(log_order)
                else:
                    balance_result_buy = submit_orders_simulation(log_order)
                print(balance_result_buy)

                profit_iteration = balance_result_buy['USDT'] + balance_result_sell['USDT']
                profit_iteration_btc = balance_result_buy['BTC'] + balance_result_sell['BTC']

                profit_acc += profit_iteration
                profit_acc_btc += profit_iteration_btc

                profit_acc += profit_iteration_btc * last_trades[-1]['close']

                total_trades += 1

                open_trade = False

                print(f"Final result is:{profit_iteration} profit_acc:{profit_acc} add btc to usdt:{profit_iteration_btc * last_trades[-1]['close']} profit_acc_btc:{profit_acc_btc} profit_iteration_btc:{profit_iteration_btc} total_trades:{total_trades}\n\n",
                      flush=True)
            #     # sys.exit()
            finish_trade = True
            if not simulation_flag:
                time.sleep(wait_cpu_time_seconds)

        except Exception as ex:
            print(ex, flush=True)
            traceback.print_exc()
            sys.exit()


if not simulation_flag:
    sub = ws.sub
    sub2 = ws2.sub

    Thread(target=sub,args=(topics,)).start()
    Thread(target=sub2,args=(topics_trades,)).start()
    trade()

else:

    historical_trades = []
    start_date = datetime(2018, 1, 1)
    # start_date = datetime(2019, 5, 1)
    result = fcoin.Api().market.get_candle_info('M1', 'btcusdt')['data']
    historical_trades.extend(result)
    last_time_seconds = result[-1]['id']
    binance = ccxt.binance({
        'timeout': 30000
    })
    file_cache_name = f".cache/candles-binance.cache"
    cache_candles = None
    if os.path.exists(f"{file_cache_name}"):
        cache_candles = pd.read_pickle(file_cache_name)
        cache_candles.index = cache_candles['id']
        cache_candles.reindex()

        print(cache_candles[cache_candles['id'] == 1576179480])


    while last_time_seconds > start_date.timestamp() and len(result) > 1:
        result = fcoin.Api().market.get_candle_info_before('M1', 'btcusdt', last_time_seconds)['data']
        if len(result) > 1:
            last_time_seconds = result[-1]['id']
            result = result[1:]
            historical_trades.extend(result)
            print(f"result:{datetime.utcfromtimestamp(result[0]['id'])}")
        else:
            symbol = 'BTC/USDT'
            timeframe = '1m'

            if cache_candles is not None and last_time_seconds in cache_candles.index:
                result = cache_candles.loc[:last_time_seconds]
                result = result.to_dict('records')
                print(f"last cache result:{datetime.utcfromtimestamp(result[-1]['id'])} "
                      f"first:{datetime.utcfromtimestamp(result[0]['id'])} "
                      f"last_time_seconds:{datetime.utcfromtimestamp(last_time_seconds)}")
                result.reverse()
                result = result[1:]

            if len(result) < 1000:
                result = binance.fetch_ohlcv(symbol, timeframe, params={'endTime': last_time_seconds*1000, 'limit': 1000})
                # result = binance.convert_ohlcv_to_trading_view(result)
                result.reverse()
                result = result[1:]
                result = [{'open': x[1], 'high': x[2], 'low': x[3], 'close': x[4], 'base_vol': x[5], 'id': x[0] // 1000} for x in result]
            last_time_seconds = result[-1]['id']
            historical_trades.extend(result)
            print(f"result:{datetime.utcfromtimestamp(result[0]['id'])}")

    historical_trades.reverse()
    print(f"Saving cache")
    pd.to_pickle(pd.DataFrame(historical_trades), file_cache_name)
    simulation()
    # Thread(target=simulation,args=()).start()



# fee = 1 - exchange.fees['trading']['taker']
# fee = 1 - 0.0006



