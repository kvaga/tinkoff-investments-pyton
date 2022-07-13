from tinkoff.invest import Client, RequestError, PortfolioResponse, PositionsResponse, PortfolioPosition
 
import pandas as pd
import os
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
 
 
"""
Для видео по get_portfolio
https://tinkoff.github.io/investAPI/operations/#portfoliorequest
https://tinkoff.github.io/investAPI
https://github.com/Tinkoff/invest-python
"""
def run():     
 
    try:
        with Client(os.environ['TINKOFF_TOKEN_RO']) as client:
            #getAccounts(client)
            #return
            # т.к. есть валятные активы (у меня etf), то нужно их отконвертить в рубли
            # я работаю только в долл, вам возможно будут нужны и др валюты
            u = client.market_data.get_last_prices(figi=['USD000UTSTOM'])
            usdrur = cast_money(u.last_prices[0].price)
 
            r : PortfolioResponse = client.operations.get_portfolio(account_id='2090759289')
            df = pd.DataFrame([portfolio_pose_todict(p, usdrur) for p in r.positions])
            print(df.head(100))
 
            print("bonds", cast_money(r.total_amount_bonds), df.query("instrument_type == 'bond'")['sell_sum'].sum(), sep=" : ")
            print("etfs", cast_money(r.total_amount_etf), df.query("instrument_type == 'etf'")['sell_sum'].sum(), sep=" : ")
            print(df['comission'].sum())
 
    except RequestError as e:
        print(str(e))
 
def portfolio_pose_todict(p : PortfolioPosition, usdrur):
    r = {
        'figi': p.figi,
        'quantity': cast_money(p.quantity),
        'expected_yield': cast_money(p.expected_yield),
        'instrument_type': p.instrument_type,
        'average_buy_price': cast_money(p.average_position_price),
        'currency': p.average_position_price.currency,
        'nkd': cast_money(p.current_nkd),
    }
 
    if r['currency'] == 'usd':
        # если бы expected_yield быk бы тоже MoneyValue,
        # то конвертацию валюты можно было бы вынести в cast_money
        r['expected_yield'] *= usdrur
        r['average_buy_price'] *= usdrur
        r['nkd'] *= usdrur
 
    r['sell_sum'] = (r['average_buy_price']*r['quantity']) + r['expected_yield'] + (r['nkd']*r['quantity'])
    r['comission'] = r['sell_sum']*0.003
    r['tax'] = r['expected_yield']*0.013 if r['expected_yield'] > 0 else 0
 
    return r

def getAccounts(client):
  r : GetAccountsResponse = client.users.get_accounts()
  df = pd.DataFrame([{'id': p.id, 'name': p.name
  } for p in r.accounts])
  print(df)
"""
Accounts:
            id                         name
0   2111522740              Маржиналка лонг
1   2111426330              Маржиналка шорт
2   2090759289                        Коган
3   2000079539                          Мой
4   2038244386                          ИИС
5   2036073431                Инвесткопилка
6   2111421018  Не выводить - налог большой
7   2111427718           ВТБ Мои Инвестиции
8   2111450124         Коган Товарные рынки
9   2111497117                    Удалить 5
10  2111378143              Открытие Брокер
11  2111713042          Коган Второй эшелон
"""

def cast_money(v):
    """
    https://tinkoff.github.io/investAPI/faq_custom_types/
    :param v:
    :return:
    """
    return v.units + v.nano / 1e9 # nano - 9 нулей

if __name__ == "__main__":
  run()
