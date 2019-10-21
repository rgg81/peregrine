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


async def pairs():
    global loop
    all_symbols = []
    for exchange_name in exchange_names:
        index = exchange_names.index(exchange_name)
        symbols = [x for x in exchange_list[index]['object'].symbols if x not in remove_pairs]
        all_symbols = list(set().union(all_symbols, symbols))
    return all_symbols


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
        # time.sleep(0.5)

        # res = loop.run_until_complete(order_book('ETH/USDT', 'fcoin'))
        # print(res['bids'][0][1])
        # print(res['bids'][0][0])

        pair_to_remove = [x for x in pair_to_remove if datetime.now() < x[1] + timedelta(seconds=30)]
        filter_pairs = remove_pairs + [x[0] for x in pair_to_remove]

        graph = create_weighted_multi_exchange_digraph(exchange_names, log=True, fees=True,
                                                       only_symbols=symbols_watch,
                                                       remove_pairs=filter_pairs,
                                                       fee_map=fee_config,
                                                       symbols_pre_fetch=all_pairs_pre_fetch)

        graph, paths = bellman_ford_multi(graph, 'ETH', loop_from_source=False, unique_paths=True)

        # graph, paths = bellman_ford_multi(graph, 'ETH', loop_from_source=False, unique_paths=True)

        # exchange.close()
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
            start_amounts = [5, 10, 50, 100, 200, 400, 800]
            start_amount = None
            amount_available = None
            max_profit = None
            max_amount = None

            for a_amount in start_amounts:
                valid = True
                for i in range(len(path)):
                    if i + 1 < len(path):
                        start = path[i]
                        if i == 0:
                            if start != 'USDT':
                                market, ticker = [(market, ticker) for market, ticker in all_pairs_usdt if start in market][0]
                                start_amount = a_amount / ticker['ask']
                                price_usdt = ticker['ask']
                            else:
                                start_amount = a_amount
                            # print('Using amount:{}'.format(start_amount))

                            first = start_amount

                        end = path[i + 1]


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

                        base_currency, quote_currency = selected_pairs[i].split('/')
                        if start == base_currency:
                            amount_available = result[i]['bids'][0][1]

                            if start_amount > amount_available:
                                # print('not all amount available : {} > {}'.format(start_amount, amount_available))
                                valid = False
                                if a_amount == start_amounts[0]:
                                    # print('Pair not met the minimum.. deleting')
                                    pair_to_remove.append((selected_pairs[i], datetime.now()))

                            start_amount = fee * start_amount * result[i]['bids'][0][0]
                        else:

                            amount_available = result[i]['asks'][0][1]
                            if start_amount / result[i]['asks'][0][0] > amount_available:
                                # print('not all amount available : {} > {}'.format(start_amount / result[i]['asks'][0][0], amount_available))
                                valid = False
                                if a_amount == start_amounts[0]:
                                    # print('Pair not met the minimum.. deleting')
                                    pair_to_remove.append((selected_pairs[i], datetime.now()))

                            start_amount = fee * start_amount / result[i]['asks'][0][0]

                        # print('{} --> {} :Amount:{} available:{} fee:{}'.format(start, end, start_amount,
                        #                                                         amount_available, fee))
                # print('End amount:{}\n\n'.format(start_amount))
                balance = start_amount - first
                if valid:
                    max_profit = balance
                    max_amount = a_amount
            if is_profitable and max_profit is not None:
                print('Is profitable!!')
                print(f'max profit/amount {max_profit} {max_amount}')
                if path[0] != 'USDT':
                    max_profit = max_profit * price_usdt
                    profit_acc += max_profit
                else:
                    profit_acc += max_profit
                with open('good-{}.txt'.format('-'.join(exchange_names)), 'a') as file:
                    file.write('{}-{}-{}-{}\n'.format(profit_acc, max_profit, max_amount, '-->'.join(path)))
                    file.flush()

    except Exception as ex:
        print(ex)
        traceback.print_exc()

