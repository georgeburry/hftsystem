import os
from stellar_sdk import Asset, ManageBuyOffer, ManageSellOffer, Network, TransactionBuilder
from connectors import create_dydx_connector, create_sdex_connector
from dydx3.constants import (
    ORDER_SIDE_BUY,
    ORDER_SIDE_SELL,
    ORDER_TYPE_MARKET,
    TIME_IN_FORCE_GTT,
)


class DydxTrader:
    def __init__(self, asset: str, leverage: float = 1.0):
        self.asset = asset
        self.market = asset + '-USD'
        self.leverage = leverage

        self.client = create_dydx_connector()
        self.account = self.get_account() 

    def get_account(self):
        return self.client.private.get_account(
            ethereum_address=os.getenv('DYDX_ETH_ADDRESS')
        )['account']

    def get_equity(self):
        return self.get_account()['equity']

    def get_open_positions(self):
        return self.get_account()['openPositions']

    def create_market_buy_order(price: float, size: float):
        placed_order = client.private.create_order(
            position_id=1, # required for creating the order signature
            market=self.market,
            side=ORDER_SIDE_BUY,
            order_type=ORDER_TYPE_MARKET,
            post_only=False,
            size=str(size),
            price=str(price),
            limit_fee='0.0005',
            expiration_epoch_seconds=1613988637,
            time_in_force=TIME_IN_FORCE_GTT,
        ) 

    def create_market_sell_order(price: float, size: float):
        placed_order = client.private.create_order(
            position_id=1, # required for creating the order signature
            market=self.market,
            side=ORDER_SIDE_SELL,
            order_type=ORDER_TYPE_MARKET,
            post_only=False,
            size=str(size),
            price=str(price),
            limit_fee='0.0005',
            expiration_epoch_seconds=1613988637,
            time_in_force=TIME_IN_FORCE_GTT,
        ) 


class SdexTrader:
    def __init__(self, asset: str):
        issuers = {
            'XLM': None,
            'BTC': 'GDPJALI4AZKUU2W426U5WKMAT6CN3AJRPIIRYR2YM54TL2GDWO5O2MZM',
            'ETH': 'GBFXOHVAS43OIWNIO7XLRJAHT3BICFEIKOJLZVXNT572MISM4CMGSOCC',
            'USDC': 'GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN',
        }
        self.base_asset = Asset(asset, issuer=issuers[asset])
        self.counter_asset = Asset('USDC', issuer=issuers['USDC'])

        self.client, self.keypair = create_sdex_connector()

    def get_account(self):
        return self.client.load_account(account_id=self.keypair.public_key)

    def get_balances(self):
        balances = self.get_account().raw_data['balances']
        reserve = 1.5 + (len(balances) - 1) * .5 + .5  # Assuming an order is open
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
