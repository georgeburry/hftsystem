from dydx3 import Client
from stellar_sdk import Server
from authentication import get_dydx_credentials, get_sdex_credentials


def create_dydx_connector():
    credentials = get_dydx_credentials()
    return Client(**credentials)


def create_sdex_connector():
    credentials = get_sdex_credentials()
    return Server(credentials['host']), credentials['keypair']

