import json
import time
import pandas as pd
import statistics as stats
from datetime import datetime
from itertools import groupby
from binance.client import Client
from dydx3 import Client as DYDXClient
from stellar_sdk import Server, Asset


def select_venue():
    venue = input('\nSelect a venue:\n\nA) Binance\nB) SDEX\nC) DYDX\n\n')
    if venue.lower() == 'a':
        venue = 'binance'
    elif venue.lower() == 'b':
        venue = 'sdex'
    elif venue.lower() == 'c':
        venue = 'dydx'
    else:
        raise ValueError('You must select venue A, B or C')
    return venue


def select_market():
    market = input('\nSelect a market:\n\nA) XLM-USD\nB) BTC-USD\nC) ETH-USD\n\n')
    if market.lower() == 'a':
        market = 'XLM-USD'
    elif market.lower() == 'b':
        market = 'BTC-USD'
    elif market.lower() == 'c':
        market = 'ETH-USD'
    else:
        raise ValueError('You must select market A, B or C')
    return market


def set_period():
    start_date = input('Enter period start date (YYYY-MM-DD): ')
    end_date = input('Enter period end date (YYYY-MM-DD) [leave blank if today]: ')
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    return start_date, end_date


def pull_sdex_data(market: str, start_date: str, end_date: str):
    base_assets = {
        'XLM': None,
        'BTC': 'GDPJALI4AZKUU2W426U5WKMAT6CN3AJRPIIRYR2YM54TL2GDWO5O2MZM',
        'ETH': 'GBFXOHVAS43OIWNIO7XLRJAHT3BICFEIKOJLZVXNT572MISM4CMGSOCC',
    }
    counter_assets = {
        'USDC': 'GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN',
    }
    server = Server(horizon_url='https://horizon.stellar.org')
    base, counter = market.split('-')
    base = Asset(base.upper(), base_assets[base.upper()])
    counter = Asset(counter.upper(), counter_assets[counter.upper()])

    call_builder = server.trades().for_asset_pair(base, counter).order(desc=True).limit(200)
    results = call_builder.call()['_embedded']['records']
    paging_token = results[-1]['paging_token']
    ledger_close_time = results[-1]['ledger_close_time']
    while ledger_close_time > start_date:
        results += call_builder.cursor(paging_token).call()['_embedded']['records']
        paging_token = results[-1]['paging_token']
        ledger_close_time = results[-1]['ledger_close_time']
        time.sleep(.25)
        print(ledger_close_time)
        return results[::-1]


def pull_binance_data(market: str, start_date: str):
    symbol = market.replace('-', '')
    start_time = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp()) * 1000
    client = Client()
    results = client.get_aggregate_trades(symbol=symbol, startTime=start_time, endTime=start_time + 3600000, limit=1000)
    aggregate_id = results[-1]['a']
    timestamp_last = 0
    timestamp_now = datetime.now().timestamp() * 1000
    while timestamp_last < timestamp_now:
        r = client.get_aggregate_trades(symbol='XLMUSDT', fromId=aggregate_id, limit=1000)
        time.sleep(1)
        results.pop()
        print('latest time', datetime.fromtimestamp(int(r[-1]['T']) / 1000))
        print('last id', results[-1]['a'], 'next id', r[0]['a'])
        results += r
        timestamp_last = int(results[-1]['T'])
        aggregate_id = results[-1]['a']
    return results


def pull_dydx_trades(market: str, start_date: str):
    client = DYDXClient('https://api.dydx.exchange')
    response = client.public.get_trades(market=market)
    trades = response.data['trades']
    while trades[-1]['createdAt'] > start_date:
        response = client.public.get_trades(                      
            market=market,
            starting_before_or_at=trades[-1]['createdAt'],
        )
        time.sleep(0.06)  # Rate limit: 175 requests / 10 seconds
        trades += response.data['trades']
        print('latest time', trades[-1]['createdAt'])
    return trades[::-1]


def transform_sdex_data(data: list):
    trades = [
        {
            'timestamp': int(datetime.fromisoformat(row['ledger_close_time'].replace('Z', '')).timestamp()),
            'quote_volume': float(row['counter_amount']),
            'price': float(row['price']['n']) / float(row['price']['d']),
        } for row in data
    ]
    trades = [(key, [d for d in group]) for key, group in groupby(trades, key=lambda d: d['timestamp'])]
    return trades


def transform_binance_data(data: list):
    trades = [
        {
            'timestamp': int(row['T'] / 1000),
            'quote_volume': float(row['q']) * float(row['p']),
            'price': float(row['p']),
        } for row in data
    ]
    trades = [(key, [d for d in group]) for key, group in groupby(trades, key=lambda d: d['timestamp'])]
    return trades


def transform_dydx_trades(trades: list):
    trades = [
        {
            'timestamp': int(datetime.fromisoformat(row['createdAt'].replace('Z', '')).timestamp()),
            'quote_volume': float(row['size']) * float(row['price']),
            'price': float(row['price']),
        } for row in trades 
    ]
    trades = [(key, [d for d in group]) for key, group in groupby(trades, key=lambda d: d['timestamp'])]
    return trades


def save_data(data: list, venue: str, market: str):
    with open(f'data/{venue}-{market}-{datetime.now().isoformat()}.json', 'w') as f:
        json.dump(data, f)


def create_df(source_path_1: str, source_path_2: str):
    df_1 = pd.read_json(source_path_1)
    df_2 = pd.read_json(source_path_2)
    df_1.rename(columns={1: 'source_1'}, inplace=True)
    df_2.rename(columns={1: 'source_2'}, inplace=True)
    df_1.set_index(0, inplace=True)
    df_2.set_index(0, inplace=True)
    df = df_1.join(df_2)
    return df


def execute_maker_buy(order, events, bid_price_2, balance_base, balance_quote, balance_perp, short_perp):
    for event in events:
        if event['price'] < order[0]:
            amount_quote = min(balance_quote, event['quote_volume'], order[1])
            amount_base = amount_quote / event['price']
            balance_base += amount_base
            balance_quote -= amount_quote
            balance_quote = max(balance_quote, 0)
            balance_perp, short_perp = execute_sell_to_cover(amount_base, bid_price_2, balance_perp, short_perp)
            order = (order[0], max(0, order[1] - event['quote_volume']))
            if not order[1]:
                order = None
                break
    return order, balance_base, balance_quote, balance_perp, short_perp


def execute_sell_to_cover(exposure_delta, bid_price_2, balance_perp, short_perp):
    short_perp -= exposure_delta
    balance_perp += exposure_delta * bid_price_2 * .9995  # Assume that market order gets filled at top bid price
    return balance_perp, short_perp


def execute_maker_sell(order, events, ask_price_2, balance_base, balance_quote, balance_perp, short_perp):
    for event in events:
        if event['price'] < order[0]:
            amount_quote = min(balance_base * event['price'], event['quote_volume'], order[1])
            amount_base = amount_quote / event['price']
            balance_quote += amount_quote
            balance_base -= amount_base 
            balance_base = max(balance_base, 0)
            balance_perp, short_perp = execute_buy_to_cover(amount_base, ask_price_2, balance_perp, short_perp)
            order = (order[0], max(0, order[1] - event['quote_volume']))
            if not order[1]:
                order = None
                break
    return order, balance_base, balance_quote, balance_perp, short_perp


def execute_buy_to_cover(exposure_delta, ask_price_2, balance_perp, short_perp):
    short_perp += exposure_delta
    balance_perp -= exposure_delta * ask_price_2 * 1.0005  # Assume that market order gets filled at top bid price
    short_perp = min(short_perp, 0)
    balance_perp = max(balance_perp, 0)
    return balance_perp, short_perp


def backtest(df):
    balance_quote = int(input('Set the quote asset starting balance (default: 1000): ') or 1000)
    balance_base = 0
    balance_perp = balance_quote
    short_perp = 0
    buy_limit_order = None
    sell_limit_order = None
    price_diff_buy = float(input('Set the price difference at which to buy (default: -0.0025) [< 0]: ') or -.0025)
    price_diff_sell = float(input('Set the price difference at which to sell (default: 0.0025) [>= 0]: ') or .0025)
    last_price_1 = last_price_2 = bid_price_1 = ask_price_1 = None
    price_diff = price_diff_pos = price_diff_neg = 0
    buy_order_created_at = sell_order_created_at = 0
    count = 0
    for idx, row in df.iterrows():
        if isinstance(row['source_1'], list):
            last_price_1 = stats.mean([d['price'] for d in row['source_1']])
            bid_price_1 = last_price_1 * .9995
            ask_price_1 = last_price_1 * 1.0005
            quote_volume = sum([d['quote_volume'] for d in row['source_1']])
            
        if isinstance(row['source_2'], list):
            last_price_2 = stats.mean([d['price'] for d in row['source_2']])
            bid_price_2 = last_price_2 * .999
            ask_price_2 = last_price_2 * 1.001
        if not last_price_1 or not last_price_2:
            continue
        price_diff_neg = last_price_1 / bid_price_2 - 1  # Hedge market order needs to sell into bids
        price_diff_pos = last_price_1 / ask_price_2 - 1  # Hedge market order needs to buy into asks

        if price_diff_neg and balance_quote and price_diff_neg < price_diff_buy: 
            if not buy_limit_order:
                buy_limit_order = (bid_price_1, min((balance_quote + balance_base * last_price_1) * .1, balance_quote)) 
                buy_order_created_at = idx
            if isinstance(row['source_1'], list):
                buy_limit_order, balance_base, balance_quote, balance_perp, short_perp = execute_maker_buy(buy_limit_order, row['source_1'], bid_price_2, balance_base, balance_quote, balance_perp, short_perp)
                if buy_limit_order and not buy_limit_order[1]:
                    buy_limit_order = None
        else:
            buy_limit_order = None    

        if price_diff_pos and balance_base and price_diff_pos > price_diff_sell:
            if not sell_limit_order:
                sell_limit_order = (ask_price_1, min((balance_quote + balance_base * last_price_1) * .1, balance_base * last_price_1)) 
                sell_order_created_at = idx
            if isinstance(row['source_1'], list):
                sell_limit_order, balance_base, balance_quote, balance_perp, short_perp = execute_maker_sell(sell_limit_order, row['source_1'], ask_price_2, balance_base, balance_quote, balance_perp, short_perp)
                if sell_limit_order and not sell_limit_order[1]:
                    sell_limit_order = None
        else:
            sell_limit_order = None

        if buy_limit_order and idx - buy_order_created_at > 60:
            buy_limit_order = (bid_price_1, buy_limit_order[1])
            buy_order_created_at = idx

        if sell_limit_order and idx - sell_order_created_at > 60:
            sell_limit_order = (ask_price_1, sell_limit_order[1])
            sell_order_created_at = idx

        if count % 1000 == 0:
            print('TOTAL', balance_quote + balance_base * last_price_1 + balance_perp + short_perp * last_price_2)
        count += 1
    print('TOTAL', balance_quote + balance_base * last_price_1 + balance_perp + short_perp * last_price_2)


def launch_backtesting_tool():
    operation = input('(F)etch trades history or (B)acktest? ')
    if operation.lower() == 'b':
        source_path_1 = 'data/sdex-XLM-USDC-2022-11-13T17:42:44.293342.json'  # input('What is the path of source #1? ')
        source_path_2 = 'data/dydx-XLM-USD-2022-11-15T17:02:03.426323.json'  # input('What is the path of source #2? ')
        df = create_df(source_path_1, source_path_2)
        backtest(df)
    elif operation.lower() == 'f':
        venue = select_venue()
        market = select_market()
        start_date, end_date = set_period()
        print(venue, market, start_date, end_date)
        if venue == 'sdex':
            market += 'C'
            data = pull_sdex_data(market, start_date, end_date)
            data = transform_sdex_data(data)
        elif venue == 'binance':
            market += 'T'
            data = pull_binance_data(market, start_date)
            data = transform_binance_data(data)
        elif venue == 'dydx':
            data = pull_dydx_trades(market, start_date)
            data = transform_dydx_trades(data)
        save_data(data, venue, market)

