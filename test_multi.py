import asyncio
import time
from datetime import datetime, timedelta
from peregrinearb import create_weighted_multi_exchange_digraph, print_profit_opportunity_for_path_multi,\
    bellman_ford_multi
from ccxt import async_support as ccxt
import traceback

fee_config = {
    'binance': 0.0006,
    'fcoin': 0.0
}

exchange_names = ['binance','fcoin']

symbols_watch = ['BTC', 'USDT', 'ETH', 'XRP', 'LTC', 'EOS', 'BCH', 'PAX', 'TUSD', 'USDC']

exchange_list = [{'object': getattr(ccxt, exchange_name)(),
                  'fee': fee_config[exchange_name]} for exchange_name in exchange_names]
loop = asyncio.get_event_loop()

loop.run_until_complete(exchange_list[exchange_names.index('fcoin')]['object'].load_markets())

fee = 1 - fee_config['fcoin']


async def pairs():
    global loop
    index = exchange_names.index('fcoin')
    symbols = exchange_list[index]['object'].symbols
    return symbols


async def pairs_usdt():
    index = exchange_names.index('binance')
    tickers = await exchange_list[index]['object'].fetch_tickers()
    return [x for x in tickers.items() if 'USDT' in x[0]]


async def order_book(a_pair, exchange_name):
    # print('start:{}'.format(a_pair))
    index = exchange_names.index(exchange_name)
    return await exchange_list[index]['object'].fetch_order_book(a_pair, limit=20)


all_pairs = loop.run_until_complete(pairs())
all_pairs_usdt = loop.run_until_complete(pairs_usdt())
# fee = 1 - exchange.fees['trading']['taker']
# fee = 1 - 0.0006

print([exchange['fee'] for exchange in exchange_list])

profit_acc = 0.0
pair_to_remove = []

while True:
    try:
        time.sleep(1)

        # res = loop.run_until_complete(order_book('ETH/USDT', 'fcoin'))
        # print(res['bids'][0][1])
        # print(res['bids'][0][0])

        graph = create_weighted_multi_exchange_digraph(['fcoin'], log=True, fees=False, only_symbols=symbols_watch)

        graph, paths = bellman_ford_multi(graph, 'ETH', loop_from_source=False, unique_paths=True)

        # graph, paths = bellman_ford_multi(graph, 'ETH', loop_from_source=False, unique_paths=True)

        # exchange.close()
        for path in paths:
            print_profit_opportunity_for_path_multi(graph, path)

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
            start_amounts = [5, 10, 50, 100, 200, 400]
            start_amount = None
            amount_available = None
            max_profit = -1000
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
                            print('Using amount:{}'.format(start_amount))

                            first = start_amount

                        end = path[i + 1]
                        base_currency, quote_currency = selected_pairs[i].split('/')
                        if start == base_currency:
                            amount_available = result[i]['bids'][0][1]

                            if start_amount > amount_available:
                                print('not all amount available : {} > {}'.format(start_amount, amount_available))
                                valid = False
                                if a_amount == start_amounts[0]:
                                    print('Pair not met the minimum.. deleting')
                                    pair_to_remove.append((selected_pairs[i], datetime.now()))

                            start_amount = fee * start_amount * result[i]['bids'][0][0]
                        else:

                            amount_available = result[i]['asks'][0][1]
                            if start_amount / result[i]['asks'][0][0] > amount_available:
                                print('not all amount available : {} > {}'.format(start_amount / result[i]['asks'][0][0], amount_available))
                                valid = False
                                if a_amount == start_amounts[0]:
                                    print('Pair not met the minimum.. deleting')
                                    pair_to_remove.append((selected_pairs[i], datetime.now()))

                            start_amount = fee * start_amount / result[i]['asks'][0][0]

                        print('{} --> {} :Amount:{} available:{}'.format(start, end, start_amount, amount_available))
                print('End amount:{}\n\n'.format(start_amount))
                balance = start_amount - first
                if balance > max_profit and valid:
                    max_profit = balance
                    max_amount = a_amount
            print(f'max profit/amount {max_profit} {max_amount}')
            if max_profit > 0:
                if path[0] != 'USDT':
                    max_profit = max_profit * price_usdt
                    profit_acc += max_profit
                else:
                    profit_acc += max_profit
                with open(f'good-fcoin.txt', 'a') as file:
                    file.write('{}-{}-{}-{}\n'.format(profit_acc, max_profit, max_amount, '-->'.join(path)))
                    file.flush()

    except Exception as ex:
        print(ex)
        traceback.print_exc()

