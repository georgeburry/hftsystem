import os
import time
from stellar_sdk import Asset, ManageBuyOffer, ManageSellOffer, Network, TransactionBuilder
from stellar_sdk import exceptions
from connectors import create_dydx_connector, create_sdex_connector
from dydx3.constants import (
    ORDER_SIDE_BUY,
    ORDER_SIDE_SELL,
    ORDER_TYPE_MARKET,
    TIME_IN_FORCE_FOK,
)


class DydxIntegration:
    def __init__(self, leverage: float = 1.0):
        self.client = create_dydx_connector()
        self.account = self.get_account() 
        self.asset = os.getenv('ASSET')
        self.market = self.asset + '-USD'
        self.min_order_amount = float(os.getenv('MIN_ORDER_AMOUNT'))
        self.step_size = float(os.getenv('STEP_SIZE'))
        self.max_slippage = float(os.getenv('MAX_SLIPPAGE'))
        self.leverage = leverage

    def get_market_info(self):
        return self.client.public.get_markets(self.market).data

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
            ethereum_address=os.getenv('DYDX_ETH_ADDRESS')
        ).data['account']

    def get_equity(self):
        return float(self.get_account()['equity'])

    def get_open_positions(self):
        open_positions = self.get_account()['openPositions']
        return open_positions[self.market] if open_positions else {}

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
        price = round(price // 0.0001 * 0.0001, 4)
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
        price = round(price // 0.0001 * 0.0001, 4)
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
        self.base_asset = Asset(os.getenv('ASSET'), issuer=issuers[os.getenv('ASSET')])
        self.counter_asset = Asset('USDC', issuer=issuers['USDC'])
        self.price_differential = float(os.getenv('PRICE_DIFFERENTIAL'))
        self.order_type = os.getenv('SDEX_ORDER_TYPE')

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

