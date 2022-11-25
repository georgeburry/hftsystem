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

liquidity = 0


def _calculate_spread(price_1: float, price_2: float):
    return price_1 / price_2 - 1


def _get_order_levels(side: str, order_type: str):
    orderbook = sdex_integration.get_orderbook()
    if (side == 'buy' and order_type == 'taker') or (side == 'sell' and order_type == 'maker'):
        return [
            {
                'price': float(ask['price']),
                'amount': float(ask['amount']),
            } for ask in orderbook['asks']
        ]
    elif (side == 'buy' and order_type == 'maker') or (side == 'sell' and order_type == 'taker'):
        return [
            {
                'price': float(bid['price']),
                'amount': float(bid['amount']),
            } for bid in orderbook['bids']
        ]


def _get_best_price_amount(side: str, price_2: float):
    price, amount = 0, 0
    order_levels = _get_order_levels(side, sdex_integration.order_type)
    for level in order_levels:
        spread = _calculate_spread(level['price'], price_2)
        if (
            side == 'buy' and spread < -sdex_integration.price_differential
            or side == 'sell' and spread > sdex_integration.price_differential
        ):
            price = level['price']
            if sdex_integration.order_type == 'maker':
                return level['price'], 1e9
            amount += level['amount']
        else:
            break
    return price, amount


def _post_buy_order_if_opportunity():
    global liquidity
    buy_offers = sdex_integration.get_buy_offers()
    bid_dydx = dydx_integration.get_first_bid()
    price, amount = _get_best_price_amount('buy', bid_dydx['price'])
    if price and amount:
        spread = _calculate_spread(price, bid_dydx['price'])
        logger.info(f'{time.ctime()} Buying opportunity: the spread is {round(spread * 100, 4)}%')
        balances = sdex_integration.get_balances()
        quote_as_base = balances['USDC'] / price
        total_base = balances[sdex_integration.base_asset.code] + quote_as_base 
        liquidity = min(bid_dydx['amount'], amount)  # Update liquidity for use in record keeping
        amount = min(amount, quote_as_base * .99, total_base / 10)
        offer_id = int(buy_offers[0]['id']) if buy_offers else 0
        if amount >= dydx_integration.min_order_amount:
            logging.warning(f'{time.ctime()} The quote balance is sufficient: Posting a buy order')
            sdex_integration.post_buy_order(price, amount, offer_id)
    elif buy_offers:  # Cancel any outstanding offers by setting amount to 0
        for offer in buy_offers:
            sdex_integration.post_buy_order(1, 0, int(offer['id']))


def _post_sell_order_if_opportunity():
    global liquidity
    sell_offers = sdex_integration.get_sell_offers()
    ask_dydx = dydx_integration.get_first_ask()
    price, amount = _get_best_price_amount('sell', ask_dydx['price'])
    if price and amount:
        spread = _calculate_spread(price, ask_dydx['price'])
        logger.info(f'{time.ctime()} Selling opportunity: the spread is {round(spread * 100, 4)}%')
        balances = sdex_integration.get_balances()
        quote_as_base = balances['USDC'] / price
        total_base = balances[sdex_integration.base_asset.code] + quote_as_base 
        liquidity = min(ask_dydx['amount'], amount)  # Update liquidity for use in record keeping
        amount = min(amount, balances[sdex_integration.base_asset.code] * .99, total_base / 10)
        offer_id = int(sell_offers[0]['id']) if sell_offers else 0
        if amount >= dydx_integration.min_order_amount:
            logging.warning(f'{time.ctime()} The base balance is sufficient: Posting a sell order')
            sdex_integration.post_sell_order(price, amount, offer_id)
    elif sell_offers:  # Cancel any outstanding offers by setting amount to 0
        for offer in sell_offers:
            sdex_integration.post_sell_order(1, 0, int(offer['id']))


def _increase_hedge_position(discrepancy, sdex_balances):
    logger.info(f'{time.ctime()} The hedge needs to be increased by {discrepancy} units')
    bid_dydx = dydx_integration.get_first_bid()
    order_price = bid_dydx['price'] * (1 - dydx_integration.max_slippage)
    logger.warning(f'{time.ctime()} Submitting a market sell order to increase hedge')
    response = dydx_integration.create_market_sell_order(order_price, discrepancy)
    logger.info(response.data)
    total_equity = _calculate_total_equity(sdex_balances)
    logger.info(f'{time.ctime()} Total equity: {total_equity}')
    _save_equity_pnl(total_equity)
    time.sleep(5)


def _decrease_hedge_position(discrepancy, sdex_balances):
    logger.info(f'{time.ctime()} The hedge needs to be decreased by {-discrepancy} units')
    ask_dydx = dydx_integration.get_first_ask()
    order_price = ask_dydx['price'] * (1 + dydx_integration.max_slippage)
    logger.warning(f'{time.ctime()} Submitting a market buy order to decrease hedge')
    response = dydx_integration.create_market_buy_order(order_price, -discrepancy)
    logger.info(response.data)
    total_equity = _calculate_total_equity(sdex_balances)
    logger.info(f'{time.ctime()} Total equity: {total_equity}')
    _save_equity_pnl(total_equity)
    time.sleep(5)


def _calculate_total_equity(sdex_balances):
    sdex_price = sdex_integration.get_midmarket_price()
    sdex_equity = sdex_balances[sdex_integration.base_asset.code] * sdex_price + \
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
        'liquidity': liquidity,
        'sdex_last_trade': sdex_integration.get_last_trade(),
        'dydx_last_trade': dydx_integration.get_last_trade(),
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

