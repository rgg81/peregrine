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
            # print(key,value)
            # print(key)
        best_bid[symbol] = {'price': bid_price, 'qtd': bid_qtd}
        best_ask[symbol] = {'price': ask_price, 'qtd': ask_qtd}
        # print(best_bid)
        # print(best_ask)


ws = HandleWebsocket()
ws2 = HandleWebsocket()
ws3 = HandleWebsocket()

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

symbols_watch = ['BTC', 'USDT', 'ETH', 'XRP', 'LTC', 'EOS', 'BCH', 'PAX', 'TUSD', 'USDC']

remove_pairs = ['PAX/ETH', 'USDT/PAX', 'PAX/BTC', 'TUSD/BTC', 'ZEC/BTC', 'ETC/BTC', 'TRX/BTC', 'XLM/BTC', 'ZEC/ETH',
                'ETC/ETH', 'TRX/ETH', 'XLM/ETH']

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

    total_amount = float(order_detail['data']['amount'])
    amount_filled = float(order_detail['data']['filled_amount'])
    new_amount = total_amount - amount_filled
    response_cancel = await cancel_order(order_detail['data']['id'])
    print(f"response_cancel:{response_cancel}")
    #if response_cancel['data']:
    while order_detail['data']['state'] not in ['canceled', 'partial_canceled']:
        await asyncio.sleep(1.0)
        order_detail = await get_order(order_detail['data']['id'])
    return await create_order(symbol_complete, order_detail['data']['side'],
                                  price, new_amount)
    #else:
     #   raise Exception(f"Error in cancelling order.. {response_cancel}")


async def change_best_price(order_detail, symbol_complete):
    order_book_inst = await order_book(order_detail['data']['symbol'], 'fcoin')
    if order_detail['data']['side'] == 'sell':
        price = order_book_inst['asks'][0][0]
    else:
        price = order_book_inst['bids'][0][0]
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


wait_seconds_time = 15


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


async def order_book(a_pair, exchange_name):
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
length_topic = len(all_pairs_topics)//3

print(all_pairs_topics[0:length_topic])
print(all_pairs_topics[length_topic:2*length_topic])
print(all_pairs_topics[length_topic*2:])

topics = {
    "id": "tickers",
    "cmd": "sub",
    "args": all_pairs_topics[0:length_topic],
}

topics2 = {
    "id": "tickers",
    "cmd": "sub",
    "args": all_pairs_topics[length_topic:length_topic*2],
}

topics3 = {
    "id": "tickers",
    "cmd": "sub",
    "args": all_pairs_topics[length_topic*2:],
}
sub = ws.sub
sub2 = ws2.sub
sub3 = ws3.sub
Thread(target=sub,args=(topics,)).start()
Thread(target=sub2,args=(topics2,)).start()
Thread(target=sub3,args=(topics3,)).start()

all_pairs_usdt = loop.run_until_complete(pairs_usdt())
# fee = 1 - exchange.fees['trading']['taker']
# fee = 1 - 0.0006

print([exchange['fee'] for exchange in exchange_list])

profit_acc = 0.0
pair_to_remove = []

while True:
    try:

        pair_to_remove = [x for x in pair_to_remove if datetime.now() < x[1] + timedelta(seconds=30)]
        filter_pairs = remove_pairs + [x[0] for x in pair_to_remove]

        graph = create_weighted_multi_exchange_digraph(exchange_names, log=True, fees=True,
                                                       only_symbols=symbols_watch,
                                                       remove_pairs=filter_pairs,
                                                       fee_map=fee_config,
                                                       symbols_pre_fetch=all_pairs_pre_fetch)

        graph, paths = bellman_ford_multi(graph, 'ETH', loop_from_source=False, unique_paths=True)

        log_orders_exec = []
        for path in paths:
            threshold = 0.05
            _, is_profitable = print_profit_opportunity_for_path_multi(graph, path,
                                                                       threshold=threshold,
                                                                       print_output=False)

            tasks = []
            selected_pairs = []

            for i in range(len(path)):
                if i + 1 < len(path):
                    start = path[i]
                    end = path[i + 1]

                    pair = [x for x in all_pairs if x == f'{start}/{end}' or x == f'{end}/{start}'][0]
                    exchange_name_to_test = graph[start][end]['exchange_name']
                    tasks.append(order_book(pair, exchange_name_to_test))
                    selected_pairs.append(pair)
                    # print(loop.run_until_complete(order_book(pair)))

            result = loop.run_until_complete(asyncio.gather(*tasks))

            # print(result)
            #start_amounts = [20, 30, 100, 200, 400, 800]
            start_amounts = [0.0050]
            start_amount = None
            # amount_available = None
            max_profit = None
            max_amount = None
            pair_precision = None
            index_pair_precision = None

            def amount_path(start_amount, path_input, start_index=0, inverted=False, precision=False):
                global valid
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

                        try:
                            fee = 1 - fee_config[graph[start][end]['exchange_name']]
                        except:
                            # print(exchange['object'].id)
                            # print(fee_map[exchange['object'].id])
                            fee = 1 - fee_config[graph[start][end]['exchange_name']]['fee']

                        try:
                            fee_for_cur = fee_config[graph[start][end]['exchange_name']][start]
                            fee = 1 - fee_for_cur
                        except:
                            pass

                        try:
                            fee_for_cur = fee_config[graph[start][end]['exchange_name']][end]
                            fee = 1 - fee_for_cur
                        except:
                            pass

                        try:
                            pair = [x for x in all_pairs if x == f'{start}/{end}' or x == f'{end}/{start}'][0]
                            fee_for_cur = fee_config[graph[start][end]['exchange_name']][f"{pair}"]
                            fee = 1 - fee_for_cur
                        except:
                            pass

                        if inverted:
                            index_to_use = next(i for i,v in enumerate(selected_pairs) if start in v and end in v)
                            selected_pair = selected_pairs[index_to_use]
                            order_book_result = result[index_to_use]
                            print(f"Check index:{i} to use:{index_to_use} "
                                  f"selected_pairs:{selected_pairs} start:{start} end:{end}")
                        else:
                            selected_pair = selected_pairs[i]
                            order_book_result = result[i]

                        base_currency, quote_currency = selected_pair.split('/')

                        last_amount = start_amount


                        if start == base_currency and end == quote_currency:
                            amount_available = order_book_result['bids'][0][1]
                            if precision:
                                start_amount = round(start_amount, all_pairs_decimal[selected_pair])
                                log_message_start = (f"BALANCE START SELL:{start} previous:{currencies_balance.get(start, 0.0)} - {start_amount}"
                                      f" now:{round(currencies_balance.get(start, 0.0) - start_amount, precision_balance)}")
                                start_amount_str = f"{fee} * {start_amount} * {order_book_result['bids'][0][0]}"
                                symbol_transformed = f"{selected_pair.replace('/', '').lower()}"
                                log_orders_exec.append({'side': 'sell', 'symbol': symbol_transformed, 'amount': start_amount,
                                                        'price': order_book_result['bids'][0][0],
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

                            start_amount = fee * start_amount * order_book_result['bids'][0][0]

                        elif start == quote_currency and end == base_currency:

                            amount_available = order_book_result['asks'][0][1]
                            if start_amount / order_book_result['asks'][0][0] > amount_available:
                                valid = False
                                if a_amount == start_amounts[0]:
                                    # print('Pair not met the minimum.. deleting')
                                    pair_to_remove.append((selected_pairs[i], datetime.now()))

                            if precision:

                                previous_balance_start = currencies_balance.get(start, 0.0)
                                amount_less_fee = round(start_amount / order_book_result['asks'][0][0],
                                                        all_pairs_decimal[selected_pair])
                                total = amount_less_fee * order_book_result['asks'][0][0]
                                currencies_balance[start] = round(currencies_balance.get(start, 0.0) - total
                                                                  ,
                                                                  precision_balance)

                                log_message_start = (f"BALANCE START BUY:{start} previous:{previous_balance_start} - {total}"
                                                     f" now:{currencies_balance[start]} --> calc total:"
                                                     f"{amount_less_fee} "
                                                     f"* {order_book_result['asks'][0][0]}")

                                start_amount_str = f"{fee} * {amount_less_fee} = {fee} * round({start_amount} / {order_book_result['asks'][0][0]},{all_pairs_decimal[selected_pair]})"
                                symbol_transformed = f"{selected_pair.replace('/', '').lower()}"
                                log_orders_exec.append({'side': 'buy', 'symbol': symbol_transformed,
                                                        'amount': amount_less_fee,
                                                        'price': order_book_result['asks'][0][0],
                                                        'symbol_complete': selected_pair})

                                start_amount = fee * amount_less_fee

                            else:
                                currencies_balance[start] = round(currencies_balance.get(start, 0.0) - start_amount,
                                                                  precision_balance)
                                start_amount = fee * start_amount / order_book_result['asks'][0][0]
                        else:
                            raise Exception(f'error in...{start} == {base_currency} and {end} == {quote_currency}')

                        if precision:
                            log_message_end = (f"BALANCE END:{end} previous:{currencies_balance.get(end, 0.0)}"
                                  f" now:{round(currencies_balance.get(end, 0.0) + start_amount, precision_balance)} "
                                  f"+ {start_amount} = {start_amount_str} fee:{fee} ")
                            print(f"{start}-->{end} {log_message_start} --> {log_message_end} ")
                        currencies_balance[end] = round(currencies_balance.get(end, 0.0) + start_amount, precision_balance)
                        if inverted:
                            print(f"Start amount:{last_amount} converted:{start_amount} start:{start} end:{end}")

                        if start == 'USDT':
                            value_currency_usdt = 1.0
                        else:
                            order_book_usdt = loop.run_until_complete(order_book(f"{start}/USDT", 'fcoin'))
                            value_currency_usdt = order_book_usdt['bids'][0][0]

                        if value_currency_usdt > max_value_usdt:
                            min_pair = selected_pairs[i]
                            min_cur_index = i
                            max_value_usdt = value_currency_usdt
                return currencies_balance, min_pair, min_cur_index


            for a_amount in start_amounts:
                valid = True
                if path[0] != 'BTC':
                    pair = [x for x in all_pairs if x == f'{path[0]}/BTC' or x == f'BTC/{path[0]}'][0]
                    order_book_btc = loop.run_until_complete(order_book(pair, 'fcoin'))
                    if path[0] == pair.split('/')[0]:
                        start_amount = a_amount / order_book_btc['asks'][0][0]
                    else:
                        start_amount = a_amount * order_book_btc['bids'][0][0]
                else:
                    start_amount = a_amount

                balances, pair_precision, index_pair_precision = amount_path(start_amount, path)

                # balance = start_amount - first
                if valid:
                    max_profit = balances
                    max_amount = a_amount
            if is_profitable and max_profit is not None:

                print(f"\n\n")
                print(f"currency precision:{path[index_pair_precision]} index:{index_pair_precision}")
                if path[index_pair_precision] != 'BTC':
                    pair = [x for x in all_pairs if x == f'{path[index_pair_precision]}/BTC'
                            or x == f'BTC/{path[index_pair_precision]}'][0]
                    order_book_btc = loop.run_until_complete(order_book(pair, 'fcoin'))
                    if path[index_pair_precision] == pair.split('/')[0]:
                        amount_cur_precision = round(max_amount / order_book_btc['asks'][0][0],
                                                     all_pairs_decimal[f"{path[index_pair_precision]}/BTC"])
                    else:
                        amount_cur_precision = round(max_amount, all_pairs_decimal[f"BTC/{path[index_pair_precision]}"])\
                                               * order_book_btc['bids'][0][0]

                    print(f"amount_cur_precision:{amount_cur_precision} "
                          f"max_amount:{max_amount} price ask:{order_book_btc['asks'][0][0]} price bid:{order_book_btc['bids'][0][0]} "
                          f"all_pairs_decimal[pair_precision]:{all_pairs_decimal[pair_precision]}")
                else:
                    amount_cur_precision = max_amount

                balance_start_rounded = amount_cur_precision

                if index_pair_precision > 0:
                    print(f"Checking precision amount optimum")
                    balance_inverted, _, _ = amount_path(amount_cur_precision, path, start_index=index_pair_precision,
                                                    inverted=True, precision=False)
                    balance_start_rounded = balance_inverted[path[0]]

                print(f"Using optimum amount:{balance_start_rounded}")
                balance_adjusted_2, _, _ = amount_path(balance_start_rounded, path, precision=True)

                print(f"balance_start_rounded:{balance_start_rounded}--{balance_adjusted_2}")

                balance_adjusted = max_profit

                print(balance_adjusted)

                profit_iteration = 0.0
                for key, value in balance_adjusted.items():
                        if key != 'USDT':
                            order_book_usdt = loop.run_until_complete(order_book(f"{key}/USDT", 'fcoin'))
                            profit_iteration += value * order_book_usdt['bids'][0][0]
                        else:
                            profit_iteration += value

                print('Is profitable!!')
                print(f"Actions:{log_orders_exec}")
                balance_adjusted_3 = submit_orders_arb(log_orders_exec)

                profit_iteration_rounded = 0.0
                for key, value in balance_adjusted_2.items():
                    if key != 'USDT':
                        order_book_usdt = loop.run_until_complete(order_book(f"{key}/USDT", 'fcoin'))
                        profit_iteration_rounded += value * order_book_usdt['bids'][0][0]
                    else:
                        profit_iteration_rounded += value

                profit_iteration_rounded_live = 0.0
                for key, value in balance_adjusted_3.items():
                    if key != 'USDT':
                        order_book_usdt = loop.run_until_complete(order_book(f"{key}/USDT", 'fcoin'))
                        profit_iteration_rounded_live += value * order_book_usdt['bids'][0][0]
                    else:
                        profit_iteration_rounded_live += value

                profit_acc += profit_iteration_rounded_live
                print(f'max profit/amount {profit_iteration} {max_amount} {profit_iteration_rounded} '
                      f'profit_iteration_rounded_live:{profit_iteration_rounded_live}', flush=True)
                with open('good-{}.txt'.format('-'.join(exchange_names)), 'a') as file:
                    file.write('{}-{}-{}-{}\n'.format(profit_acc, profit_iteration_rounded_live, max_amount, '-->'.join(path)))
                    file.flush()

    except Exception as ex:
        print(ex, flush=True)
        traceback.print_exc()
        sys.exit()

