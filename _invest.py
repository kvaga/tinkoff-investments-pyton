import os, sys
from traceback import print_tb
import pandas as pd
sys.path.append('..')
import creds

from tinkoff.invest import Client

TOKEN = os.environ["INVEST_TOKEN"]


def main():
    with Client(TOKEN) as client:
        #print(client.users.get_accounts())
        # print(client.operations.get_portfolio(account_id=creds.accountKoganId))
        #r = client.instruments.bonds()
        #r = len(r.instruments)
        #print(r)
        portfolio()

def portfolio():
    with Client(TOKEN) as client:
        print(client.operations.get_positions(account_id=creds.accountKoganId)) 
    

if __name__ == "__main__":
    main()
