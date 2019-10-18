import asyncio
import time
from datetime import datetime, timedelta
from peregrinearb import load_exchange_graph_v2, print_profit_opportunity_for_path, bellman_ford
from ccxt import async_support as ccxt
import traceback
import sys

exchange_name = sys.argv[1]

exchange = getattr(ccxt, exchange_name)()
loop = asyncio.get_event_loop()

fee_config = {
    'binance': 0.0006,
    'hitbtc2': 0.0007,
    'fcoin': 0.0,
    'coinex': 0.0005
}


async def pairs():
    tickers = await exchange.fetch_tickers()
    return [market_name for market_name, ticker in tickers.items()]


async def pairs_usdt():
    tickers = await exchange.fetch_tickers()
    return [x for x in tickers.items() if 'USDT' in x[0]]


async def order_book(a_pair):
    # print('start:{}'.format(a_pair))
    return await exchange.fetch_order_book(a_pair, limit=5)


all_pairs = loop.run_until_complete(pairs())
all_pairs_usdt = loop.run_until_complete(pairs_usdt())
# fee = 1 - exchange.fees['trading']['taker']
fee = 1 - fee_config[exchange_name]

print('fee taker:{} fee:{}'.format(exchange.fees['trading']['taker'], fee))

pair_to_remove = []
profit_acc = 0.0

while True:
    try:
        time.sleep(3)
        pair_to_remove = [x for x in pair_to_remove if datetime.now() < x[1] + timedelta(minutes=5)]
        filter_pairs = [x[0] for x in pair_to_remove]
        graph = loop.run_until_complete(load_exchange_graph_v2(exchange, fees=True, filter_pairs=filter_pairs,
                                                               force_fee=fee_config[exchange_name]))

        paths = bellman_ford(graph, 'BTC', unique_paths=True)

        # exchange.close()
        for path in paths:
            print_profit_opportunity_for_path(graph, path)

            tasks = []
            selected_pairs = []

            for i in range(len(path)):
                if i + 1 < len(path):
                    start = path[i]
                    end = path[i + 1]

                    pair = [x for x in all_pairs if x == f'{start}/{end}' or x == f'{end}/{start}'][0]
                    tasks.append(order_book(pair))
                    selected_pairs.append(pair)
                    # print(loop.run_until_complete(order_book(pair)))

            result = loop.run_until_complete(asyncio.gather(*tasks))

            print(result)
            start_amounts = [5, 10, 50, 100, 200, 400, 800]
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
                with open(f'good-{exchange_name}.txt', 'a') as file:
                    file.write('{}-{}-{}-{}\n'.format(profit_acc, max_profit, max_amount, '-->'.join(path)))
                    file.flush()

    except Exception as ex:
        print(ex)
        traceback.print_exc()

