import os
from dotenv import load_dotenv
from stellar_sdk import Keypair

load_dotenv()


def get_binance_credentials():
    return {
        'api_key': os.getenv('BINANCE_API_KEY'),
        'api_secret': os.getenv('BINANCE_API_SECRET'),
    }


def get_dydx_credentials(instance: int):
    return {
        'host': os.getenv(f'DYDX_HOST'),
        'stark_private_key': os.getenv(f'DYDX_STARK_PRIVATE_KEY_{instance}'),
        'api_key_credentials': {
            'key': os.getenv(f'DYDX_KEY_{instance}'),
            'secret': os.getenv(f'DYDX_SECRET_{instance}'),
            'passphrase': os.getenv(f'DYDX_PASSPHRASE_{instance}'),
        }
    }


def get_sdex_credentials():
    return {
        'host': os.getenv('SDEX_HOST'),
        'keypair': Keypair.from_secret(key_parser(os.getenv('SDEX_PRIVATE_KEY'))),
    }


def key_parser(key):
    reference = [c for c in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ']

    first_n = None
    r = ''
    for c in key:
        if c.isnumeric():
            if not first_n:
                first_n = c
            else:
                n = first_n + c
                first_n = None
                r += reference[int(n)]
        else:
            r += str(reference.index(c))
    return r

