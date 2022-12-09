import os
import time
from datetime import datetime
from stellar_sdk import Asset, ManageBuyOffer, ManageSellOffer, Network, TransactionBuilder
from stellar_sdk import exceptions
from connectors import create_binance_connector, create_dydx_connector, create_sdex_connector
from dydx3.constants import (
    ORDER_SIDE_BUY,
    ORDER_SIDE_SELL,
    ORDER_TYPE_MARKET,
    TIME_IN_FORCE_FOK,
)


class BinanceIntegration:
    def __init__(self, asset: str = None, quote_asset: str = 'BUSD'):
        self.client = create_binance_connector()
        self.account = self.get_account()
        self.asset = asset or os.getenv('BINANCE_ASSET')
        self.quote_asset = quote_asset
        self.buy_spread = float(os.getenv('BINANCE_BUY_SPREAD'))
        self.sell_spread = float(os.getenv('BINANCE_SELL_SPREAD'))
        self.order_type = os.getenv('BINANCE_ORDER_TYPE')
        self.filters = self.get_symbol_filters(asset)
        self.account_ratio = float(os.getenv('BINANCE_ACCOUNT_RATIO'))
        self.spread = float(os.getenv('BINANCE_SPREAD')) or .01

    def get_exchange_info(self):
        response = self.client.get_exchange_info()
        time.sleep(1)
        return response

    def get_symbol_filters(self, asset: str):
        exchange_info = self.get_exchange_info()
        time.sleep(1)
        symbol = asset + self.quote_asset
        filters = [s for s in exchange_info['symbols'] if s['symbol'] == symbol][0]['filters']
        return {
            'price_filters': [f for f in filters if f['filterType'] == 'PRICE_FILTER'][0],
            'min_notional': [f for f in filters if f['filterType'] == 'MIN_NOTIONAL'][0],
            'lot_size': [f for f in filters if f['filterType'] == 'LOT_SIZE'][0],
        }

    def get_all_tickers(self):
        response = self.client.get_all_tickers()
        time.sleep(1)
        return response

    def get_orderbook(self):
        response = self.client.get_order_book(symbol=self.asset + self.quote_asset)
        time.sleep(1)
        return response

    def get_midmarket_price(self):
        orderbook = self.get_orderbook()
        return (float(orderbook['bids'][0][0]) + float(orderbook['asks'][0][0])) / 2

    def get_account(self):
        response = self.client.get_account()
        time.sleep(1)
        return response

    def get_account_balance(self):
        balances = self.get_account()['balances']
        tickers = self.get_all_tickers()
        total_balance = 0
        for balance in balances:
            if balance['asset'] != self.quote_asset:
                tickers_filtered = [
                    ticker for ticker in tickers
                    if ticker['symbol'] == balance['asset'] + self.quote_asset
                ]
                if not tickers_filtered:
                    continue
                price = float(tickers_filtered[0]['price'])
            else:
                price = 1
            total_balance += float(balance['free']) * price + float(balance['locked']) * price
        return total_balance

    def get_asset_balance(self, asset: str):
        return self.client.get_asset_balance(asset=asset)

    def get_total_base_balance(self):
        response = self.get_asset_balance(self.asset)
        return float(response['free']) + float(response['locked'])

    def get_total_quote_balance(self):
        response = self.get_asset_balance(self.quote_asset)
        return float(response['free']) + float(response['locked'])

    def get_free_base_balance(self):
        return float(self.get_asset_balance(self.asset)['free'])

    def get_free_quote_balance(self):
        return float(self.get_asset_balance(self.quote_asset)['free'])

    def get_free_balances(self):
        base_asset = self.client.get_asset_balance(asset=self.asset)
        time.sleep(1)
        quote_asset = self.client.get_asset_balance(asset=self.quote_asset)
        time.sleep(1)
        return {
            'base': float(base_asset['free']),
            'quote': float(quote_asset['free']),
        }

    def get_total_balances(self):
        base_asset = self.client.get_asset_balance(asset=self.asset)
        time.sleep(1)
        quote_asset = self.client.get_asset_balance(asset=self.quote_asset)
        time.sleep(1)
        return {
            'base': float(base_asset['free']) + float(base_asset['locked']),
            'quote': float(quote_asset['free']) + float(quote_asset['locked']),
        }

    def get_open_orders(self, side=None):
        orders = self.client.get_open_orders(symbol=self.asset + self.quote_asset)
        time.sleep(1)
        if side:
            orders = [order for order in orders if order['side'] == side.upper()]
        return orders

    def get_last_trade(self):
        trades = self.client.get_my_trades(symbol=self.asset + self.quote_asset)
        time.sleep(1)
        if not trades:
            return {}
        return {
            'time': datetime.fromtimestamp(trades[-1]['time'] / 1000).isoformat(),
            'side': 'BUY' if trades[-1]['isBuyer'] else 'SELL',
            'price': float(trades[-1]['price']),
            'amount': float(trades[-1]['qty']),
        }

    def create_limit_buy_order(self, price, quantity):
        step_size = float(self.filters['lot_size']['stepSize'])
        tick_size = float(self.filters['price_filters']['tickSize'])
        quantity = round(quantity // step_size * step_size, 8)
        price = round(price // tick_size * tick_size, 8)
        response = self.client.order_limit_buy(
            symbol=self.asset + self.quote_asset,
            quantity=quantity,
            price=price,
        )
        time.sleep(1)
        return response

    def create_limit_sell_order(self, price, quantity):
        step_size = float(self.filters['lot_size']['stepSize'])
        tick_size = float(self.filters['price_filters']['tickSize'])
        quantity = round(quantity // step_size * step_size, 8)
        price = round(price // tick_size * tick_size, 8)
        response = self.client.order_limit_sell(
            symbol=self.asset + self.quote_asset,
            quantity=quantity,
            price=price,
        )
        time.sleep(1)
        return response

    def create_market_buy_order(self, quantity):
        step_size = float(self.filters['lot_size']['stepSize'])
        quantity = round(quantity // step_size * step_size, 8)
        response = self.client.order_market_buy(
            symbol=self.asset + self.quote_asset,
            quantity=quantity,
        )
        time.sleep(1)
        return response

    def create_market_sell_order(self, quantity):
        step_size = float(self.filters['lot_size']['stepSize'])
        quantity = round(quantity // step_size * step_size, 8)
        response = self.client.order_market_sell(
            symbol=self.asset + self.quote_asset,
            quantity=quantity,
        )
        time.sleep(1)
        return response

    def cancel_order(self, order_id):
        response = self.client.cancel_order(
            symbol=self.asset + self.quote_asset,
            orderId=order_id,
        )
        time.sleep(1)
        return response


class DydxIntegration:
    def __init__(self, instance: int, asset: str, leverage: float = 1.0):
        self.instance = instance
        self.client = create_dydx_connector(instance)
        self.account = self.get_account() 
        self.asset = asset
        self.market = self.asset + '-USD'
        self.market_info = self.get_market_info()
        self.min_order_amount = float(self.market_info['minOrderSize'])
        self.step_size = float(self.market_info['stepSize'])
        self.tick_size = float(self.market_info['tickSize'])
        self.max_slippage = float(os.getenv('MAX_SLIPPAGE'))
        self.leverage = leverage

    def get_market_info(self):
        response = self.client.public.get_markets(self.market).data
        return response['markets'][self.market]

    def get_orderbook(self):
        return self.client.public.get_orderbook(self.market).data

    def get_first_bid(self):
        orderbook = self.get_orderbook()
        return {
            'amount': float(orderbook['bids'][0]['size']),
            'price': float(orderbook['bids'][0]['price'])
        }

    def get_first_ask(self):
        orderbook = self.get_orderbook()
        return {
            'amount': float(orderbook['asks'][0]['size']),
            'price': float(orderbook['asks'][0]['price'])
        }

    def get_user(self):
        return self.client.private.get_user().data['user']

    def get_account(self):
        return self.client.private.get_account(
            ethereum_address=os.getenv(f'DYDX_ETH_ADDRESS_{self.instance}')
        ).data['account']

    def get_equity(self):
        return float(self.get_account()['equity'])

    def get_open_positions(self):
        open_positions = self.get_account()['openPositions']
        return open_positions.get(self.market) or {}

    def get_last_trade(self):
        trades = self.client.private.get_fills(market=self.market, limit=1).data['fills']
        if not trades:
            return {}
        return {
            'time': trades[0]['createdAt'],
            'side': trades[0]['side'],
            'price': float(trades[0]['price']),
            'amount': float(trades[0]['size']),
        }

    def create_market_buy_order(self, price: float, amount: float):
        price = round(price // self.tick_size * self.tick_size, 4)
        amount = round(amount // self.step_size * self.step_size, 10)
        placed_order = self.client.private.create_order(
            position_id=self.account['positionId'],
            market=self.market,
            side=ORDER_SIDE_BUY,
            order_type=ORDER_TYPE_MARKET,
            post_only=False,
            size=str(amount),
            price=str(price),
            limit_fee='0.0005',
            expiration_epoch_seconds=int(time.time() + 3600),
            time_in_force=TIME_IN_FORCE_FOK,
        ) 
        return placed_order

    def create_market_sell_order(self, price: float, amount: float):
        price = round(price // self.tick_size * self.tick_size, 4)
        amount = round(amount // self.step_size * self.step_size, 10)
        placed_order = self.client.private.create_order(
            position_id=self.account['positionId'],
            market=self.market,
            side=ORDER_SIDE_SELL,
            order_type=ORDER_TYPE_MARKET,
            post_only=False,
            size=str(amount),
            price=str(price),
            limit_fee='0.0005',
            expiration_epoch_seconds=int(time.time() + 3600),
            time_in_force=TIME_IN_FORCE_FOK,
        )
        return placed_order


class SdexIntegration:
    def __init__(self):
        issuers = {
            'XLM': None,
            'BTC': 'GDPJALI4AZKUU2W426U5WKMAT6CN3AJRPIIRYR2YM54TL2GDWO5O2MZM',
            'ETH': 'GBFXOHVAS43OIWNIO7XLRJAHT3BICFEIKOJLZVXNT572MISM4CMGSOCC',
            'USDC': 'GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN',
        }
        self.client, self.keypair = create_sdex_connector()
        self.asset = os.getenv('SDEX_ASSET')
        self.base_asset = Asset(self.asset, issuer=issuers[self.asset])
        self.counter_asset = Asset('USDC', issuer=issuers['USDC'])
        self.buy_spread = float(os.getenv('SDEX_BUY_SPREAD'))
        self.sell_spread = float(os.getenv('SDEX_SELL_SPREAD'))
        self.order_type = os.getenv('SDEX_ORDER_TYPE')
        self.account_ratio = float(os.getenv('SDEX_ACCOUNT_RATIO'))
        self.spread = float(os.getenv('SDEX_SPREAD')) or .01

    def get_orderbook(self):
        return self.client.orderbook(self.base_asset, self.counter_asset).call()

    def get_midmarket_price(self):
        orderbook = self.get_orderbook()
        return (float(orderbook['bids'][0]['price']) + float(orderbook['asks'][0]['price'])) / 2

    def get_first_bid(self):
        orderbook = self.get_orderbook()
        return {
            'amount': float(orderbook['bids'][0]['amount']),
            'price': float(orderbook['bids'][0]['price'])
        }

    def get_first_ask(self):
        orderbook = self.get_orderbook()
        return {
            'amount': float(orderbook['asks'][0]['amount']),
            'price': float(orderbook['asks'][0]['price'])
        }

    def get_account(self):
        return self.client.load_account(account_id=self.keypair.public_key)

    def get_balances(self):
        balances = self.get_account().raw_data['balances']
        reserve = 1.5 + (len(balances) - 1) * .5 + .5  # .5 x 1 order
        native_balance = {
            'XLM': max(float(b['balance']) - reserve, 0) for b in balances
            if b['asset_type'] == 'native'
        }
        other_balances = {
            b['asset_code']: float(b['balance'])
            for b in balances if 'credit_alphanum' in b['asset_type']
        }
        return {**native_balance, **other_balances}

    def get_buy_offers(self):
        offers = self.client.offers().for_seller(self.keypair.public_key)
        buy_offers = offers.for_buying(self.base_asset).call()
        buy_offers = buy_offers['_embedded']['records']
        return buy_offers

    def get_sell_offers(self):
        offers = self.client.offers().for_seller(self.keypair.public_key)
        sell_offers = offers.for_buying(self.counter_asset).call()
        sell_offers = sell_offers['_embedded']['records']
        return sell_offers

    def get_last_trade(self):
        trades = self.client.trades()\
            .for_account(self.keypair.public_key)\
            .for_asset_pair(self.base_asset, self.counter_asset)\
            .order(desc=True)\
            .call()['_embedded']['records']
        if not trades:
            return {}
        return {
            'time': trades[0]['ledger_close_time'],
            'side': 'BUY' if trades[0]['base_is_seller'] else 'SELL',
            'price': float(trades[0]['price']['n']) / float(trades[0]['price']['d']),
            'amount': float(trades[0]['base_amount']),
        }

    def submit_transaction(self, operation):
        transaction = TransactionBuilder(
            source_account=self.get_account(),
            network_passphrase=Network.PUBLIC_NETWORK_PASSPHRASE,
            base_fee=10000,
        )
        transaction.append_operation(operation)
        transaction = transaction.set_timeout(30).build()
        transaction.sign(self.keypair)
        self.client.submit_transaction(transaction)

    def post_buy_order(
        self,
        price: float,
        amount: float,
        offer_id=0,
        ):
        amount = str(round(amount, 7))
        price = str(round(price, 7))
        operation = ManageBuyOffer(
            selling=self.counter_asset,
            buying=self.base_asset,
            amount=amount,
            price=price,
            offer_id=offer_id,
        )
        self.submit_transaction(operation)

    def post_sell_order(
        self,
        price: float,
        amount: float,
        offer_id=0,
        ):
        amount = str(round(amount, 7))
        price = str(round(price, 7))
        operation = ManageSellOffer(
            selling=self.base_asset,
            buying=self.counter_asset,
            amount=amount,
            price=price,
            offer_id=offer_id,
        )
        self.submit_transaction(operation)

