from binance.client import Client as BinanceClient
from dydx3 import Client as DydxClient
from stellar_sdk import Server
from authentication import (
    get_binance_credentials,
    get_dydx_credentials,
    get_sdex_credentials,
)


def create_binance_connector():
    credentials = get_binance_credentials()
    return BinanceClient(**credentials)


def create_dydx_connector():
    credentials = get_dydx_credentials()
    return DydxClient(**credentials)


def create_sdex_connector():
    credentials = get_sdex_credentials()
    return Server(credentials['host']), credentials['keypair']

