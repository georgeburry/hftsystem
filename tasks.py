import time
from timeloop import Timeloop
from datetime import datetime, timedelta
from integrations import DydxIntegration, SdexIntegration

trading_tasks = Timeloop()

dydx_integration = DydxIntegration()
sdex_integration = SdexIntegration()


def _post_buy_order_if_opportunity():
    buy_offers = sdex_integration.get_buy_offers()
    bid_sdex = sdex_integration.get_first_bid()
    bid_dydx = dydx_integration.get_first_bid()
    spread = bid_sdex['price'] / bid_dydx['price'] - 1
    print(time.ctime(), 'Minus - Spread', bid_sdex['price'] / bid_dydx['price'] - 1) 

    if spread < -sdex_integration.price_differential:
        print(spread)
        balances = sdex_integration.get_balances()
        quote_as_base = balances['USDC'] / bid_sdex['price']
        total_base = balances[sdex_integration.base_asset.code] + quote_as_base 
        amount = min(quote_as_base * .99, total_base / 10)
        offer_id = int(buy_offers[0]['id']) if buy_offers else 0
        if amount >= 100:
            sdex_integration.post_buy_order(bid_sdex['price'], amount, offer_id)
            print('SDEX buy order')
    elif buy_offers:  # Cancel any outstanding offers by setting amount to 0
        for offer in buy_offers:
            sdex_integration.post_buy_order(bid_sdex['price'], 0, int(offer['id']))


def _post_sell_order_if_opportunity():
    sell_offers = sdex_integration.get_sell_offers()
    ask_sdex = sdex_integration.get_first_ask()
    ask_dydx = dydx_integration.get_first_ask()
    spread = ask_sdex['price'] / ask_dydx['price'] - 1
    print(time.ctime(), 'Positive - Spread', ask_sdex['price'] / ask_dydx['price'] - 1) 

    if spread > sdex_integration.price_differential:
        print(spread)
        balances = sdex_integration.get_balances()
        quote_as_base = balances['USDC'] / ask_sdex['price']
        total_base = balances[sdex_integration.base_asset.code] + quote_as_base 
        amount = min(balances[sdex_integration.base_asset.code] * .99, total_base / 10)
        offer_id = int(sell_offers[0]['id']) if sell_offers else 0
        if amount >= 100:
            sdex_integration.post_sell_order(ask_sdex['price'], amount, offer_id)
            print('SDEX sell order')
    elif sell_offers:  # Cancel any outstanding offers by setting amount to 0
        for offer in sell_offers:
            sdex_integration.post_sell_order(ask_sdex['price'], 0, int(offer['id']))


@trading_tasks.job(interval=timedelta(seconds=5))
def run_arbitrage_strategy():
    _post_buy_order_if_opportunity()
    _post_sell_order_if_opportunity()


@trading_tasks.job(interval=timedelta(seconds=1))
def update_hedge():
    dydx_position = dydx_integration.get_open_positions()
    dydx_position = float(dydx_position['size']) if dydx_position else 0
    sdex_balances = sdex_integration.get_balances()
    discrepancy = sdex_balances[sdex_integration.base_asset.code] + dydx_position
    discrepancy = discrepancy // 10 * 10
    if discrepancy > 100:
        print('DYDX rebalance', discrepancy)
        bid_dydx = dydx_integration.get_first_bid()
        price = bid_dydx['price'] * .95
        response = dydx_integration.create_market_sell_order(price, discrepancy)
        time.sleep(10)

    elif discrepancy < -100:
        print('DYDX rebalance', discrepancy)
        ask_dydx = dydx_integration.get_first_ask()
        price = ask_dydx['price'] * 1.05
        response = dydx_integration.create_market_buy_order(price, -discrepancy)
        time.sleep(10)


def run_trading_tasks():
    trading_tasks.start(block=True)

