import os
from dotenv import load_dotenv
from stellar_sdk import Keypair

load_dotenv()


def get_dydx_credentials():
    return {
        'host': os.getenv('DYDX_HOST'),
        'api_key_credentials': {
            'key': os.getenv('DYDX_KEY'),
            'secret': os.getenv('DYDX_SECRET'),
            'passphrase': os.getenv('DYDX_PASSPHRASE'),
        }
    }


def get_sdex_credentials():
    return {
        'host': os.getenv('SDEX_HOST'),
        'keypair': Keypair.from_secret(os.getenv('SDEX_PRIVATE_KEY')),
    }

