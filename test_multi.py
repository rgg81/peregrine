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
                print(f'Received ping event.. connection is good')
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

exchange_names_input = sys.argv[1]

exchange_names = exchange_names_input.split(',')

print('Using exchanges:{}'.format(exchange_names))

symbols_watch = ['BTC', 'USDT', 'ETH', 'XRP', 'LTC', 'EOS', 'BCH', 'PAX', 'TUSD', 'USDC', 'ZEC', 'ETC', 'TRX', 'XLM']

remove_pairs = ['PAX/ETH', 'USDT/PAX', 'PAX/BTC', 'TUSD/BTC']

exchange_list = [{'object': getattr(ccxt, exchange_name)(),
                  'fee': fee_config[exchange_name]} for exchange_name in exchange_names]
loop = asyncio.get_event_loop()

for exchange_name in exchange_names:
    loop.run_until_complete(exchange_list[exchange_names.index(exchange_name)]['object'].load_markets())

api_auth = fcoin.authorize('', '')


async def create_order(symbol, side, price, amount):
    symbol_transformed = f"{symbol.replace('/', '').lower()}"
    order_create_param = fcoin.order_create_param(symbol_transformed, side, 'limit', price, amount)
    return api_auth.orders.create(order_create_param)


async def get_order(order_id):
    return api_auth.orders.get(order_id)


async def cancel_order(order_id):
    return api_auth.orders.submit_cancel(order_id)


async def change_price(order_id, price):
    order_detail = await get_order(order_id)
    total_amount = float(order_detail['data']['amount'])
    amount_filled = float(order_detail['data']['filled_amount'])
    new_amount = total_amount - amount_filled
    response_cancel = await cancel_order(order_id)
    if response_cancel['data']:
        while order_detail['data']['state'] not in ['canceled', 'partial_canceled']:
            await asyncio.sleep(0.3)
            order_detail = await get_order(order_id)
        return await create_order(order_detail['data']['symbol'], order_detail['data']['side'],
                                  price, new_amount)
    else:
        raise Exception(f"Error in cancelling order.. {response_cancel}")


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
            start_amounts = [20, 30, 100, 200, 400, 800]
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
                                log_orders_exec.append({'action': 'sell', 'pair': selected_pair,
                                                        'amount': start_amount,
                                                        'price': order_book_result['bids'][0][0]})
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
                                log_orders_exec.append({'action': 'buy', 'pair': selected_pair,
                                                        'amount': amount_less_fee,
                                                        'price': order_book_result['asks'][0][0]})
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
                if path[0] != 'USDT':

                    order_book_usdt = loop.run_until_complete(order_book(f"{path[0]}/USDT", 'fcoin'))
                    start_amount = a_amount / order_book_usdt['asks'][0][0]
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
                if path[index_pair_precision] != 'USDT':

                    order_book_usdt = loop.run_until_complete(order_book(f"{path[index_pair_precision]}/USDT", 'fcoin'))
                    amount_cur_precision = round(max_amount / order_book_usdt['asks'][0][0],
                                                 all_pairs_decimal[f"{path[index_pair_precision]}/USDT"])
                    print(f"amount_cur_precision:{amount_cur_precision} "
                          f"max_amount:{max_amount} price:{order_book_usdt['asks'][0][0]} "
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

                profit_iteration_rounded = 0.0
                for key, value in balance_adjusted_2.items():
                    if key != 'USDT':
                        order_book_usdt = loop.run_until_complete(order_book(f"{key}/USDT", 'fcoin'))
                        profit_iteration_rounded += value * order_book_usdt['bids'][0][0]
                    else:
                        profit_iteration_rounded += value

                profit_acc += profit_iteration
                print('Is profitable!!')
                print(f"Actions:{log_orders_exec}")
                print(f'max profit/amount {profit_iteration} {max_amount} {profit_iteration_rounded}')
                with open('good-{}.txt'.format('-'.join(exchange_names)), 'a') as file:
                    file.write('{}-{}-{}-{}\n'.format(profit_acc, max_profit, max_amount, '-->'.join(path)))
                    file.flush()

    except Exception as ex:
        print(ex)
        traceback.print_exc()

