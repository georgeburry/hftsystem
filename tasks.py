import json
import logging
import time
from timeloop import Timeloop
from datetime import datetime, timedelta
from integrations import DydxIntegration, SdexIntegration

logger = logging.getLogger(__name__)

trading_tasks = Timeloop()

dydx_integration = DydxIntegration()
sdex_integration = SdexIntegration()


def _post_buy_order_if_opportunity():
    buy_offers = sdex_integration.get_buy_offers()
    bid_dydx = dydx_integration.get_first_bid()
    if sdex_integration.order_type == 'taker':
        price = sdex_integration.get_first_ask()['price']
    else:
        price = sdex_integration.get_first_bid()['price']
    spread = price / bid_dydx['price'] - 1

    if spread < -sdex_integration.price_differential:
        logger.info(f'{time.ctime()} Buying opportunity: the spread is {round(spread * 100, 4)}%')
        balances = sdex_integration.get_balances()
        quote_as_base = balances['USDC'] / bid_sdex['price']
        total_base = balances[sdex_integration.base_asset.code] + quote_as_base 
        amount = min(quote_as_base * .99, total_base / 10)
        offer_id = int(buy_offers[0]['id']) if buy_offers else 0
        if amount >= dydx_integration.min_order_amount:
            logging.warning(f'{time.ctime()} The quote balance is sufficient: Posting a buy order')
            sdex_integration.post_buy_order(price, amount, offer_id)
    elif buy_offers:  # Cancel any outstanding offers by setting amount to 0
        for offer in buy_offers:
            sdex_integration.post_buy_order(price, 0, int(offer['id']))


def _post_sell_order_if_opportunity():
    sell_offers = sdex_integration.get_sell_offers()
    ask_dydx = dydx_integration.get_first_ask()
    if sdex_integration.order_type == 'taker':
        price = sdex_integration.get_first_bid()['price']
    else:
        price = sdex_integration.get_first_ask()['price']
    spread = price / ask_dydx['price'] - 1

    if spread > sdex_integration.price_differential:
        logger.info(f'{time.ctime()} Selling opportunity: the spread is {round(spread * 100, 4)}%')
        balances = sdex_integration.get_balances()
        quote_as_base = balances['USDC'] / ask_sdex['price']
        total_base = balances[sdex_integration.base_asset.code] + quote_as_base 
        amount = min(balances[sdex_integration.base_asset.code] * .99, total_base / 10)
        offer_id = int(sell_offers[0]['id']) if sell_offers else 0
        if amount >= dydx_integration.min_order_amount:
            logging.warning(f'{time.ctime()} The base balance is sufficient: Posting a sell order')
            sdex_integration.post_sell_order(price, amount, offer_id)
    elif sell_offers:  # Cancel any outstanding offers by setting amount to 0
        for offer in sell_offers:
            sdex_integration.post_sell_order(price, 0, int(offer['id']))


def _increase_hedge_position(discrepancy, sdex_balances):
    logger.info(f'{time.ctime()} The hedge needs to be increased by {discrepancy} units')
    bid_dydx = dydx_integration.get_first_bid()
    order_price = bid_dydx['price'] * (1 - dydx_integration.max_slippage)
    logger.warning(f'{time.ctime()} Submitting a market sell order to increase hedge')
    response = dydx_integration.create_market_sell_order(order_price, discrepancy)
    logger.info(response.data)
    total_equity = _calculate_total_equity(sdex_balances, bid_dydx['price'])
    logger.info(f'{time.ctime()} Total equity: {total_equity}')
    _save_equity_pnl(total_equity)
    time.sleep(10)


def _decrease_hedge_position(discrepancy, sdex_balances):
    logger.info(f'{time.ctime()} The hedge needs to be decreased by {-discrepancy} units')
    ask_dydx = dydx_integration.get_first_ask()
    order_price = ask_dydx['price'] * (1 + dydx_integration.max_slippage)
    logger.warning(f'{time.ctime()} Submitting a market buy order to decrease hedge')
    response = dydx_integration.create_market_buy_order(order_price, -discrepancy)
    logger.info(response.data)
    total_equity = _calculate_total_equity(sdex_balances, ask_dydx['price'])
    logger.info(f'{time.ctime()} Total equity: {total_equity}')
    _save_equity_pnl(total_equity)
    time.sleep(10)


def _calculate_total_equity(sdex_balances, execution_price):
    sdex_equity = sdex_balances[sdex_integration.base_asset.code] * execution_price + \
    sdex_balances[sdex_integration.counter_asset.code]
    dydx_equity = dydx_integration.get_equity()
    return sdex_equity + dydx_equity


def _save_equity_pnl(total_equity, file_name='results.json'):
    try:
        with open(file_name, 'r') as f:
            results = json.load(f)
    except FileNotFoundError:
        results = []
    results.append({
        'timestamp': int(time.time()),
        'total_equity': total_equity,
        'pnl_this_trade': total_equity / results[-1]['total_equity'] - 1 if results else None,
        'pnl_overall': total_equity / results[0]['total_equity'] - 1 if results else None,
    })
    with open(file_name, 'w') as f:
        json.dump(results, f)


@trading_tasks.job(interval=timedelta(seconds=5))
def run_arbitrage_strategy():
    try:
        _post_buy_order_if_opportunity()
        _post_sell_order_if_opportunity()
    except Exception as e:
        logging.error(f'{time.ctime()} {e}')


@trading_tasks.job(interval=timedelta(seconds=1))
def update_hedge():
    try:
        dydx_position = dydx_integration.get_open_positions()
        dydx_position = float(dydx_position['size']) if dydx_position else 0
        sdex_balances = sdex_integration.get_balances()
        discrepancy = sdex_balances[sdex_integration.base_asset.code] + dydx_position
        discrepancy = discrepancy // dydx_integration.step_size * dydx_integration.step_size
        if discrepancy > dydx_integration.min_order_amount:
            _increase_hedge_position(discrepancy, sdex_balances)
        elif discrepancy < -dydx_integration.min_order_amount:
            _decrease_hedge_position(discrepancy, sdex_balances) 
    except Exception as e:
        logging.error(f'{time.ctime()} {e}')


def run_trading_tasks():
    trading_tasks.start(block=True)

