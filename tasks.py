import json
import logging
import os
import time
from timeloop import Timeloop
from datetime import datetime, timedelta
from integrations import BinanceIntegration, DydxIntegration, SdexIntegration

logger = logging.getLogger(__name__)

trading_tasks = Timeloop()

integration = None
dydx_integration = None
instance = None

venue = None
liquidity = 0
user = None
dydx_trailing_volume = 0


def _calculate_spread(price_1: float, price_2: float):
    return price_1 / price_2 - 1


def _get_order_levels(side: str, order_type: str):
    orderbook = integration.get_orderbook()
    if (side == 'buy' and order_type == 'taker') or (side == 'sell' and order_type == 'maker'):
        return [
            {
                'price': float(ask[0] if venue == 'binance' else ask['price']),
                'amount': float(ask[1] if venue == 'binance' else ask['amount']),
            } for ask in orderbook['asks']
        ]
    elif (side == 'buy' and order_type == 'maker') or (side == 'sell' and order_type == 'taker'):
        return [
            {
                'price': float(bid[0] if venue == 'binance' else bid['price']),
                'amount': float(bid[1] if venue == 'binance' else bid['amount']),
            } for bid in orderbook['bids']
        ]


def _get_best_price_amount(side: str, price_2: float):
    price, amount = 0, 0
    order_levels = _get_order_levels(side, integration.order_type)
    for level in order_levels:
        spread = _calculate_spread(level['price'], price_2)
        if (
            side == 'buy' and spread - integration.spread < integration.buy_spread
            or side == 'sell' and spread + integration.spread > integration.sell_spread
        ):
            price = level['price']
            amount += level['amount']
            if (
                integration.order_type == 'maker'
                and abs(price - order_levels[0]['price']) > integration.spread
            ):
                return price, 1e9
        else:
            break
    return price, amount


def _binance_buy_if_opportunity():
    global liquidity
    if venue != 'binance':
        raise ValueError('Venue must be "binance" to run _binance_buy_if_opportunity function')
    buy_orders = integration.get_open_orders(side='BUY')
    bid_dydx = dydx_integration.get_first_bid()
    price, amount = _get_best_price_amount('buy', bid_dydx['price'])
    if price and amount:
        balances = integration.get_free_balances()
        quote_as_base = balances['quote'] / price
        total_base = balances['base'] + quote_as_base
        min_notional = float(integration.filters['min_notional']['minNotional']) / price * 1.01
        amount = min(amount, quote_as_base, max(min_notional, total_base * integration.account_ratio)) * .99
        liquidity = min(bid_dydx['amount'], amount)  # Update liquidity for use in record keeping
        spread = round(_calculate_spread(price, bid_dydx['price']) * 100, 4)
        logger.info(f'{time.ctime()} Binance - Spread: {spread}% ({amount} units @ price: {price})')
        if amount > dydx_integration.min_order_amount and amount > min_notional:
            logger.warning(f'{time.ctime()} Binance - Buying {amount} units @ price: {price}')
            response = integration.create_limit_buy_order(price, amount)
            logger.info(response)
    elif buy_orders:
        for order in buy_orders:
            integration.cancel_order(order['orderId'])

def _binance_sell_if_opportunity():
    global liquidity
    if venue != 'binance':
        raise ValueError('Venue must be "binance" to run _binance_sell_if_opportunity function')
    sell_orders = integration.get_open_orders(side='SELL')
    ask_dydx = dydx_integration.get_first_ask()
    price, amount = _get_best_price_amount('sell', ask_dydx['price'])
    if price and amount:
        balances = integration.get_free_balances()
        quote_as_base = balances['quote'] / price
        total_base = balances['base'] + quote_as_base
        min_notional = float(integration.filters['min_notional']['minNotional']) / price * 1.01
        amount = min(amount, balances['base'], max(min_notional, total_base * integration.account_ratio)) * .99
        liquidity = min(ask_dydx['amount'], amount)  # Update liquidity for use in record keeping
        spread = round(_calculate_spread(price, ask_dydx['price']) * 100, 4)
        logger.info(f'{time.ctime()} Binance - Spread: {spread}% ({amount} units @ price: {price})')
        if amount > dydx_integration.min_order_amount and amount > min_notional:
            logger.warning(f'{time.ctime()} Binance - Selling {amount} units @ price: {price}')
            response = integration.create_limit_sell_order(price, amount)
            logger.info(response)
    elif sell_orders:
        for order in sell_orders:
            integration.cancel_order(order['orderId'])

def _sdex_buy_if_opportunity():
    global liquidity
    if venue != 'sdex':
        raise ValueError('Venue must be "sdex" to run _sdex_buy_if_opportunity function')
    buy_offers = integration.get_buy_offers()
    bid_dydx = dydx_integration.get_first_bid()
    price, amount = _get_best_price_amount('buy', bid_dydx['price'])
    if price and amount:
        spread = round(_calculate_spread(price, bid_dydx['price']) * 100, 4)
        logger.info(f'{time.ctime()} SDEX - Spread: {spread}% ({amount} units @ price: {price})')
        balances = integration.get_balances()
        quote_as_base = balances['USDC'] / price
        total_base = balances[integration.base_asset.code] + quote_as_base
        liquidity = min(bid_dydx['amount'], amount)  # Update liquidity for use in record keeping
        amount = min(amount, quote_as_base, total_base * integration.account_ratio) * .99
        if amount > dydx_integration.min_order_amount:
            logger.warning(f'{time.ctime()} SDEX - Buying {amount} units @ price: {price}')
            offer_id = int(buy_offers[0]['id']) if buy_offers else 0
            integration.post_buy_order(price, amount, offer_id)
    elif buy_offers:  # Cancel any outstanding offers by setting amount to 0
        for offer in buy_offers:
            integration.post_buy_order(1, 0, int(offer['id']))

def _sdex_sell_if_opportunity():
    global liquidity
    if venue != 'sdex':
        raise ValueError('Venue must be "sdex" to run _sdex_sell_if_opportunity function')
    sell_offers = integration.get_sell_offers()
    ask_dydx = dydx_integration.get_first_ask()
    price, amount = _get_best_price_amount('sell', ask_dydx['price'])
    if price and amount:
        spread = round(_calculate_spread(price, ask_dydx['price']) * 100, 4)
        logger.info(f'{time.ctime()} SDEX - Spread: {spread}% ({amount} units @ price: {price})')
        balances = integration.get_balances()
        quote_as_base = balances['USDC'] / price
        total_base = balances[integration.base_asset.code] + quote_as_base
        liquidity = min(ask_dydx['amount'], amount)  # Update liquidity for use in record keeping
        amount = min(amount, balances[integration.base_asset.code], total_base * integration.account_ratio) * .99
        if amount > dydx_integration.min_order_amount:
            logger.warning(f'{time.ctime()} SDEX - Selling {amount} units @ price: {price}')
            offer_id = int(sell_offers[0]['id']) if sell_offers else 0
            integration.post_sell_order(price, amount, offer_id)
    elif sell_offers:  # Cancel any outstanding offers by setting amount to 0
        for offer in sell_offers:
            integration.post_sell_order(1, 0, int(offer['id']))


def _increase_hedge_position(discrepancy, balances):
    logger.info(f'{time.ctime()} The hedge needs to be increased by {discrepancy} units')
    bid_dydx = dydx_integration.get_first_bid()
    order_price = bid_dydx['price'] * (1 - dydx_integration.max_slippage)
    logger.warning(f'{time.ctime()} Submitting a market sell order to increase hedge')
    response = dydx_integration.create_market_sell_order(order_price, discrepancy)
    logger.info(response.data)
    total_equity = _calculate_total_equity(balances)
    logger.info(f'{time.ctime()} Total equity: {total_equity}')
    _save_equity_pnl(total_equity)
    time.sleep(5)


def _decrease_hedge_position(discrepancy, balances):
    logger.info(f'{time.ctime()} The hedge needs to be decreased by {-discrepancy} units')
    ask_dydx = dydx_integration.get_first_ask()
    order_price = ask_dydx['price'] * (1 + dydx_integration.max_slippage)
    logger.warning(f'{time.ctime()} Submitting a market buy order to decrease hedge')
    response = dydx_integration.create_market_buy_order(order_price, -discrepancy)
    logger.info(response.data)
    total_equity = _calculate_total_equity(balances)
    logger.info(f'{time.ctime()} Total equity: {total_equity}')
    _save_equity_pnl(total_equity)
    time.sleep(5)


def _calculate_total_equity(balances):
    venue_a_price = integration.get_midmarket_price()
    venue_a_equity = balances['base'] * venue_a_price + \
    balances['quote']
    dydx_equity = dydx_integration.get_equity()
    return venue_a_equity + dydx_equity


def _save_equity_pnl(total_equity, file_name='results'):
    try:
        with open(f'{file_name}_{instance}', 'r') as f:
            results = json.load(f)
    except FileNotFoundError:
        results = []
    venue_a_last_trade = integration.get_last_trade()
    results.append({
        'timestamp': int(time.time()),
        'total_equity': total_equity,
        'liquidity': liquidity,
        'venue_a_last_trade': venue_a_last_trade,
        'dydx_last_trade': dydx_integration.get_last_trade(),
        'pnl_this_trade': total_equity / results[-1]['total_equity'] - 1 if results else None,
        'pnl_overall': total_equity / results[0]['total_equity'] - 1 if results else None,
    })
    with open(f'{file_name}_{instance}_{integration.asset}.json', 'w') as f:
        json.dump(results, f)


@trading_tasks.job(interval=timedelta(seconds=int(os.getenv('REFRESH_PERIOD')) or 5))
def run_arbitrage_strategy():
    try:
        if dydx_trailing_volume > 100000:
            logger.warning('Trading halted! User: {user["publicId"]} DYDX 30D trailing volume has exceeded 100,000 USD')
        elif venue == 'binance':
            _binance_buy_if_opportunity()
            _binance_sell_if_opportunity()
        elif venue == 'sdex':
            _sdex_buy_if_opportunity()
            _sdex_sell_if_opportunity()
    except Exception as e:
        logger.error(f'{time.ctime()} {e}')


@trading_tasks.job(interval=timedelta(seconds=1))
def update_hedge():
    try:
        global user, dydx_trailing_volume
        dydx_position = dydx_integration.get_open_positions()
        dydx_position = float(dydx_position['size']) if dydx_position else 0
        if venue == 'binance':
            balances = integration.get_total_balances()
        elif venue == 'sdex':
            balances = integration.get_balances()
            balances = {
                'base': balances[integration.base_asset.code],
                'quote': balances[integration.counter_asset.code],
            }
        discrepancy = balances['base'] + dydx_position
        discrepancy = discrepancy // dydx_integration.step_size * dydx_integration.step_size
        if discrepancy > dydx_integration.min_order_amount:
            _increase_hedge_position(discrepancy, balances)
        elif discrepancy < -dydx_integration.min_order_amount:
            _decrease_hedge_position(discrepancy, balances)
        user = dydx_integration.get_user()
        dydx_trailing_volume = float(user['makerVolume30D']) + float(user['takerVolume30D'])
    except Exception as e:
        logger.error(f'{time.ctime()} {e}')


def run_trading_tasks():
    global venue, integration, dydx_integration, instance
    venue_input = input('Select the venue:\n\nA) Binance\nB) SDEX\n\n')
    venue = {'A': 'binance', 'B': 'sdex'}.get(venue_input.upper())
    asset = input('Enter the asset code (e.g. BTC): ') or None
    if venue == 'binance':
        integration = BinanceIntegration(asset=asset)
    elif venue == 'sdex':
        integration = SdexIntegration()
    instance = int(input('Enter the venue instance for hedging [integer]: '))
    dydx_integration = DydxIntegration(instance, integration.asset)
    trading_tasks.start(block=True)

