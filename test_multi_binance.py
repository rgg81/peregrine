import asyncio
import time
import fcoin
from binance.WebsocketClient import WebsocketClient
from threading import Thread
from datetime import datetime, timedelta
from peregrinearb import create_weighted_multi_exchange_digraph, print_profit_opportunity_for_path_multi,\
    bellman_ford_multi, best_ask, best_bid
from ccxt import async_support as ccxt
import traceback
import sys


class HandleWebsocket(WebsocketClient):
    def handle(self,msg):
        symbol = msg['data']['s'].lower()
        ask_price = float(msg['data']['a'])
        ask_qtd = float(msg['data']['A'])
        bid_price = float(msg['data']['b'])
        bid_qtd = float(msg['data']['B'])

        best_bid[symbol] = {'price': bid_price, 'qtd': bid_qtd}
        best_ask[symbol] = {'price': ask_price, 'qtd': ask_qtd}

        # print(f'Symbol:{symbol} {ask_price} {ask_qtd} {bid_price} {bid_qtd}')


symbols_base = ['BTC', 'ETH', 'USDT']

symbols_watch = ['VET', 'IOST', 'TRX', 'HOT', 'MFT', 'STORM', 'KEY', 'XVG', 'ZIL', 'DATA',
                 'LEND', 'ADA', 'CMT', 'VIB', 'XLM', 'TNT', 'FUN',
                 'IOTX', 'QKC', 'DOCK', 'NEO', 'LINK', 'QTUM', 'BAT', 'IOTA',
                 'ONT', 'XRP', 'BQX', 'ADA', 'XRP', 'EOS', 'ENG']


symbols_watch_usdt = ['ARPA', 'VET', 'ERD', 'DOGE', 'MATIC', 'FET', 'NKN', 'BAT', 'XTZ', 'TRX', 'IOST']

# symbols_watch = ['VET', 'IOST', 'TRX', 'ADA', 'VIB']


# symbols_watch_usdt = ['VET', 'ADA', 'HOT', 'TRX', 'NCASH', 'ENG']

all_symbols = symbols_base + symbols_watch + symbols_watch_usdt

paths = [['BTC', x, 'ETH', 'BTC'] for x in symbols_watch] + [['BTC', x, 'USDT', 'BTC'] for x in symbols_watch_usdt]

# paths = [['ETH', x, 'BTC', 'ETH'] for x in symbols_watch_usdt]


remove_pairs = []

exchange_names = ['binance']
binance_ex = getattr(ccxt, 'binance')({'timeout': 30000})

loop = asyncio.get_event_loop()

loop.run_until_complete(binance_ex.load_markets())

async def pairs():
    global loop

    tickers = await binance_ex.fetch_tickers()

    symbols = [market_name for market_name, ticker in tickers.items() if market_name.split('/')[0] in all_symbols and
               market_name.split('/')[1] in all_symbols]

    return symbols


async def pairs_decimal_fcoin():
    result = await binance_ex.fetch_markets()
    result_dict = {}

    for x in result:
        result_dict[f"{x['symbol']}"] = x['precision']['amount']
    return result_dict


async def pairs_usdt():
    # binance_ex = getattr(ccxt, 'binance')()

    # tickers_binance = await binance_ex.fetch_tickers()
    tickers_binance = await binance_ex.fetch_tickers()
    # tickers = list(tickers_binance.items()) + list(tickers_hitbtc.items())
    tickers = list(tickers_binance.items())
    return [x for x in tickers if 'USDT' in x[0] and x[0] not in remove_pairs]


async def load_order_book_cold_start():
    # binance_ex = getattr(ccxt, 'binance')()

    # tickers_binance = await binance_ex.fetch_tickers()
    tickers_binance = await binance_ex.fetch_tickers()
    # tickers = list(tickers_binance.items()) + list(tickers_hitbtc.items())
    tickers = list(tickers_binance.items())

    def set_value(item):
        symbol = item[1]['symbol'].replace("/","").lower()
        ask_price = item[1]['ask']
        ask_qtd = item[1]['askVolume']
        bid_price = item[1]['bid']
        bid_qtd = item[1]['bidVolume']

        best_bid[symbol] = {'price': bid_price, 'qtd': bid_qtd}
        best_ask[symbol] = {'price': ask_price, 'qtd': ask_qtd}

    return [set_value(x) for x in tickers]


async def order_book(a_pair, exchange_name):
    # print('start:{}'.format(a_pair))
    # index = exchange_names.index(exchange_name)
    key = f"{a_pair.replace('/','').lower()}"
    ask_price = float(best_ask[key]['price'])
    ask_qtd = float(best_ask[key]['qtd'])

    bid_price = float(best_bid[key]['price'])
    bid_qtd = float(best_bid[key]['qtd'])
    return {'bids':[[bid_price, bid_qtd]], 'asks':[[ask_price, ask_qtd]]}


def order_book_sync(a_pair):
    # print('start:{}'.format(a_pair))
    # index = exchange_names.index(exchange_name)
    key = f"{a_pair.replace('/','').lower()}"
    ask_price = float(best_ask[key]['price'])
    ask_qtd = float(best_ask[key]['qtd'])

    bid_price = float(best_bid[key]['price'])
    bid_qtd = float(best_bid[key]['qtd'])
    return {'bids':[[bid_price, bid_qtd]], 'asks':[[ask_price, ask_qtd]]}


all_pairs = loop.run_until_complete(pairs())

# all_pairs.append('BTC/USDT')

loop.run_until_complete(load_order_book_cold_start())

all_pairs_decimal = loop.run_until_complete(pairs_decimal_fcoin())

all_pairs_pre_fetch = [x for x in all_pairs
                       if x.split('/')[0] in all_symbols and x.split('/')[1] in all_symbols]
all_pairs_topics = [f"{x.split('/')[0].lower()}{x.split('/')[1].lower()}" for
                    x in all_pairs_pre_fetch]

print(all_pairs_topics)
length_topic = len(all_pairs_topics)//3

print(all_pairs_topics[0:length_topic])
print(all_pairs_topics[length_topic:2*length_topic])
print(all_pairs_topics[length_topic*2:])


ws = HandleWebsocket(all_pairs_topics[0:length_topic])
ws2 = HandleWebsocket(all_pairs_topics[length_topic:2*length_topic])
ws3 = HandleWebsocket(all_pairs_topics[length_topic*2:])

# ws = HandleWebsocket(['cndbtc'])

# time.sleep(10)
# ws.close()

fee_config = {
    'binance': 0.0006
    #'binance': 0.0000
}



sub = ws.sub
sub2 = ws2.sub
sub3 = ws3.sub
Thread(target=sub,args=()).start()
Thread(target=sub2,args=()).start()
Thread(target=sub3,args=()).start()


profit_acc = 0.0
pair_to_remove = []


def convert_btc_to_symbol(a_symbol, input_amount):
    global all_pairs
    if a_symbol != 'BTC':
        pair_filter = [x for x in all_pairs if x == f'{a_symbol}/BTC' or x == f'BTC/{a_symbol}'][0]
        order_book_btc = order_book_sync(pair_filter)
        if a_symbol == pair_filter.split('/')[0]:
            result_amount = input_amount / order_book_btc['asks'][0][0]
        else:
            result_amount = input_amount * order_book_btc['bids'][0][0]
    else:
        result_amount = input_amount

    return result_amount


def convert_symbol_to_btc(a_symbol, input_amount):
    global all_pairs
    if a_symbol != 'BTC':
        pair_filter = [x for x in all_pairs if x == f'{a_symbol}/BTC' or x == f'BTC/{a_symbol}'][0]
        order_book_btc = order_book_sync(pair_filter)
        if a_symbol == pair_filter.split('/')[0]:
            result_amount = input_amount * order_book_btc['bids'][0][0]
        else:
            result_amount = input_amount / order_book_btc['asks'][0][0]
    else:
        result_amount = input_amount

    return result_amount


while True:
    try:

        # start_time = datetime.now()
        log_orders_exec = []
        profits_per_path = []
        for path in paths:

            tasks = []
            selected_pairs = []

            for i in range(len(path)):
                if i + 1 < len(path):
                    start = path[i]
                    end = path[i + 1]
                    # print(start, end)
                    pair = [x for x in all_pairs if x == f'{start}/{end}' or x == f'{end}/{start}'][0]
                    selected_pairs.append(pair)

            # result = loop.run_until_complete(asyncio.gather(*tasks))

            # print(result)
            start_amounts = [0.100]
            start_amount = None
            # amount_available = None
            max_profit = None
            max_amount = None
            pair_precision = None
            index_pair_precision = None

            amount_available_to_trade = None

            valid = False

            def amount_path(start_amount, path_input, start_index=0, inverted=False, precision=False):
                global valid, amount_available_to_trade
                min_pair = None
                min_cur_index = None
                precision_balance = 18
                currencies_balance = {}
                max_value_usdt = float('-inf')

                range_to_use = range(start_index, len(path_input)) if not inverted else range(start_index, -1, -1)

                def add_or_subtract(i):
                    return i + 1 if not inverted else i - 1

                for i in range_to_use:
                    if add_or_subtract(i) < len(path_input) and add_or_subtract(i) >= 0:
                        start = path_input[i]
                        end = path_input[add_or_subtract(i)]

                        fee = 1 - fee_config['binance']

                        if inverted:
                            index_to_use = next(i for i,v in enumerate(selected_pairs) if start in v and end in v)
                            selected_pair = selected_pairs[index_to_use]
                            order_book_result = order_book_sync(selected_pair)
                            print(f"Check index:{i} to use:{index_to_use} "
                                  f"selected_pairs:{selected_pairs} start:{start} end:{end}")
                        else:
                            selected_pair = selected_pairs[i]
                            order_book_result = order_book_sync(selected_pair)

                        base_currency, quote_currency = selected_pair.split('/')

                        last_amount = start_amount

                        if start == base_currency and end == quote_currency:

                            if i == 0:
                                aggression = 'asks'
                            else:
                                aggression = 'bids'

                            amount_available = order_book_result[aggression][0][1]
                            amount_available_btc = convert_symbol_to_btc(start, amount_available)
                            # print(f"Available:{amount_available} converted btc:{amount_available_btc}")

                            if amount_available_to_trade is None or amount_available_btc < amount_available_to_trade:
                                amount_available_to_trade = amount_available_btc

                            if precision:
                                start_amount = round(start_amount, all_pairs_decimal[selected_pair])
                                log_message_start = (f"BALANCE START SELL:{start} previous:{currencies_balance.get(start, 0.0)} - {start_amount}"
                                                     f" now:{round(currencies_balance.get(start, 0.0) - start_amount, precision_balance)}")
                                start_amount_str = f"{fee} * {start_amount} * {order_book_result[aggression][0][0]}"

                                symbol_transformed = f"{selected_pair.replace('/', '').lower()}"
                                log_orders_exec.append({'side': 'sell', 'symbol': symbol_transformed, 'amount': start_amount,
                                                        'price': order_book_result[aggression][0][0],
                                                        'symbol_complete': selected_pair})

                                # print(f"Using precision amount is:{start_amount} start:{start} end:{end}")
                            currencies_balance[start] = round(currencies_balance.get(start, 0.0) - start_amount,
                                                              precision_balance)

                            if start_amount > amount_available:
                                # print('not all amount available : {} > {}'.format(start_amount, amount_available))
                                valid = False
                                if a_amount == start_amounts[0]:
                                    # print('Pair not met the minimum.. deleting')
                                    pair_to_remove.append((selected_pair, datetime.now()))

                            start_amount = fee * start_amount * order_book_result[aggression][0][0]

                        elif start == quote_currency and end == base_currency:

                            if i == 0:
                                aggression = 'bids'
                            else:
                                aggression = 'asks'

                            amount_available = order_book_result[aggression][0][1]
                            amount_available_btc = convert_symbol_to_btc(end, amount_available)
                            # print(f"Available:{amount_available} converted btc:{amount_available_btc}")

                            if amount_available_to_trade is None or amount_available_btc < amount_available_to_trade:
                                amount_available_to_trade = amount_available_btc

                            if start_amount / order_book_result[aggression][0][0] > amount_available:
                                valid = False
                                if a_amount == start_amounts[0]:
                                    # print('Pair not met the minimum.. deleting')
                                    pair_to_remove.append((selected_pairs[i], datetime.now()))

                            if precision:

                                previous_balance_start = currencies_balance.get(start, 0.0)
                                amount_less_fee = round(start_amount / order_book_result[aggression][0][0],
                                                        all_pairs_decimal[selected_pair])
                                total = amount_less_fee * order_book_result[aggression][0][0]
                                currencies_balance[start] = round(currencies_balance.get(start, 0.0) - total
                                                                  ,
                                                                  precision_balance)

                                log_message_start = (f"BALANCE START BUY:{start} previous:{previous_balance_start} - {total}"
                                                     f" now:{currencies_balance[start]} --> calc total:"
                                                     f"{amount_less_fee} "
                                                     f"* {order_book_result[aggression][0][0]}")

                                start_amount_str = f"{fee} * {amount_less_fee} = {fee} * round({start_amount} / {order_book_result[aggression][0][0]},{all_pairs_decimal[selected_pair]})"

                                symbol_transformed = f"{selected_pair.replace('/', '').lower()}"
                                log_orders_exec.append({'side': 'buy', 'symbol': symbol_transformed,
                                                        'amount': amount_less_fee,
                                                        'price': order_book_result[aggression][0][0],
                                                        'symbol_complete': selected_pair})

                                start_amount = fee * amount_less_fee

                            else:
                                currencies_balance[start] = round(currencies_balance.get(start, 0.0) - start_amount,
                                                                  precision_balance)
                                start_amount = fee * start_amount / order_book_result[aggression][0][0]
                        else:
                            raise Exception(f'error in...{start} == {base_currency} and {end} == {quote_currency}')

                        if precision:
                            log_message_end = (f"BALANCE END:{end} previous:{currencies_balance.get(end, 0.0)}"
                                               f" now:{round(currencies_balance.get(end, 0.0) + start_amount, precision_balance)} "
                                               f"+ {start_amount} = {start_amount_str} fee:{fee} ")
                            # print(f"{start}-->{end} {log_message_start} --> {log_message_end} ")
                        currencies_balance[end] = round(currencies_balance.get(end, 0.0) + start_amount, precision_balance)
                        if inverted:
                            print(f"Start amount:{last_amount} converted:{start_amount} start:{start} end:{end}")


                return currencies_balance, min_pair, min_cur_index


            balance_adjusted = None
            for a_amount in start_amounts:
                valid = True

                start_amount = convert_btc_to_symbol(path[0], a_amount)

                balances, pair_precision, index_pair_precision = amount_path(start_amount, path, precision=True)

                start_amount = convert_btc_to_symbol(path[0], amount_available_to_trade)
                # print(f"Now using amount_available_to_trade:{amount_available_to_trade} btc using start_amount:{start_amount} {path[0]}")
                # if not valid:
                balances, pair_precision, index_pair_precision = amount_path(start_amount, path, precision=True)
                balance_adjusted = balances
                # print(f"\n\n")

                # balance = start_amount - first
                if valid:
                    max_profit = balances
                    max_amount = a_amount
                    # print(balances)

            profit_iteration = 0.0
            for key, value in balance_adjusted.items():
                value_btc = convert_symbol_to_btc(key, value)
                order_book_usdt = order_book_sync(f"BTC/USDT")
                value_usdt = value_btc * order_book_usdt['bids'][0][0]

                profit_iteration += value_usdt

            # print(f"profit_iteration:{profit_iteration} {balance_adjusted}\n\n")
            profits_per_path.append((profit_iteration, amount_available_to_trade, path))

        # profits_per_path = [x for x in profits_per_path if x[0] > 0.0]

        profits_per_path.sort(key=lambda x: x[0])

        # profits_per_path = [x for x in profits_per_path if 'VET' in x[2] or 'XGV' in x[2]
        #                     or 'ZIL' in x[2] or 'IOST' in x[2]]
        print_str = [f"{round(x[0], 3)} {','.join(x[2][0:2])}" for x in profits_per_path[-10:]]
        sys.stdout.write(f"{' | '.join(print_str)}  \r")
        sys.stdout.flush()
        res_list = [i for i in range(len(profits_per_path)) if profits_per_path[i][0] > 0.0]
        # if len(res_list) > 0:
        #     with open('found-binance.txt', 'a') as file:
        #     print(f"found profit!!! {res_list} {[(profits_per_path[i], paths[i]) for i in res_list]} \n\n", flush=True)
        #         file.write(f"found profit!!! {res_list} {[(profits_per_path[i], paths[i]) for i in res_list]} \n\n")
        #         file.flush()

        # end_time = datetime.now()
        # delta = end_time - start_time
        # print(f"took:{delta.microseconds}")



    except Exception as ex:
        print(ex)
        traceback.print_exc()
        sys.exit()

