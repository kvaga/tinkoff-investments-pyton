from tinkoff.invest import Client, RequestError, PortfolioResponse, PositionsResponse, PortfolioPosition, InstrumentStatus
import sys
# https://habr.com/en/post/483302/
from googleapiclient.discovery import build

import httplib2 
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials

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
def run(spreadsheetId):        
 
    try:
        with Client(os.environ['TINKOFF_TOKEN_RO']) as client:
            # return
            print('Getting all maps from tinkoff API...')
            allShares=getMapOfAllShares(client)
            allBonds = getMapOfAllBonds(client)
            allEtfs = getMapOfAllETFs(client)
            allCurrencies = getMapOfAllCurrencies(client)
            allFutures = getMapOfAllFutures(client)

            commonDataframe = []
            # commonDataframe = pd.DataFrame(columns=['name', 'quantity', 'average_buy_price', 'currency', 'instrument_type', 'expected_yield'])
            # BBG012C34FX0
            #print(allShares)
            #print(allBonds)
            # print(allEtfs)
            #print(getShareNameByFigi('BBG0136BTL03', allShares))
            #getAccounts(client)
            # return
            print('Getting instruments from tinkoff api')
            """
            # т.к. есть валятные активы (у меня etf), то нужно их отконвертить в рубли
            # я работаю только в долл, вам возможно будут нужны и др валюты
            u = client.market_data.get_last_prices(figi=['USD000UTSTOM'])
            usdrur = cast_money(u.last_prices[0].price)
            """
            for x in [\
                        '2111522740', #             Маржиналка лонг /
                        '2111426330', #             Маржиналка шорт /
                        '2090759289', #                       Коган /
                        '2000079539', #                         Мой /
                        '2038244386', #                         ИИС /
                        # '2036073431', #               Инвесткопилка /
                        '2111421018', # Не выводить - налог большой /
                        '2111427718', #          ВТБ Мои Инвестиции /
                        '2111450124', #        Коган Товарные рынки /
                        '2111497117', #                   Удалить 5 /
                        '2111378143', #             Открытие Брокер /
                        '2111713042', #         Коган Второй эшелон /
                    ]: 
                # print(x)
                r : PortfolioResponse = client.operations.get_portfolio(account_id=x)
                df = pd.DataFrame([portfolio_pose_todict(p, allShares, allBonds, allEtfs, allCurrencies, allFutures) for p in r.positions])
                # print(df)
                # commonDataframe.append(df)
                #
                commonDataframe.append(df)
                # pd.concat([commonDataframe,df], ignore_index=True)
                #
                # print(df.sort_values(by=['%'],ascending=False))
                # print(df.size)
                # pd.concat([commonDataframe, df], ignore_index=True, sort=False)
            # print(commonDataframe.sort_values(by=['%'],ascending=False))
            # print(pd.concat(commonDataframe).sort_values(by=['%'],ascending=False))
            # print(getYieldByInstruments(pd.concat(commonDataframe)))
            send2GoogleSpreadSheet(getYieldByInstruments(pd.concat(commonDataframe)),spreadsheetId)
            # print(getYieldByInstruments(commonDataframe))
                        
            """
            print("bonds", cast_money(r.total_amount_bonds), df.query("instrument_type == 'bond'")['sell_sum'].sum(), sep=" : ")
            print("etfs", cast_money(r.total_amount_etf), df.query("instrument_type == 'etf'")['sell_sum'].sum(), sep=" : ")
            print(df['comission'].sum())
            """
 
    except RequestError as e:
        print(str(e))

def send2GoogleSpreadSheet(data, existingSpreadSheeId=''):
    GOOGLE_SHEETS_CREDENTIALS_FILE = os.environ['GOOGLE_PROJECT_CREDENTIALS_FILE_PATH']  # Имя файла с закрытым ключом, вы должны подставить свое
    # Читаем ключи из файла
    credentials = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_SHEETS_CREDENTIALS_FILE, 
                                                                   ['https://www.googleapis.com/auth/spreadsheets', 
                                                                    'https://www.googleapis.com/auth/drive'])
    print("Auth process in Google Sheets")
    httpAuth = credentials.authorize(httplib2.Http()) # Авторизуемся в системе
    service = apiclient.discovery.build('sheets', 'v4', http = httpAuth) # Выбираем работу с таблицами и 4 версию API 
    print("service: ")
    if( not existingSpreadSheeId):
        print("Creating google sheet file...")
        spreadsheet = service.spreadsheets().create(body = {
            'properties': {'title': 'Первый тестовый документ', 'locale': 'ru_RU'},
            'sheets': [{'properties': {'sheetType': 'GRID',
                                    'sheetId': 0,
                                    'title': 'Лист номер один',
                                    'gridProperties': {'rowCount': 100, 'columnCount': 15}}}]
        }).execute()
        print('Created spreadsheet with id: ', spreadsheet['spreadsheetId'])
        spreadsheet_Id = spreadsheet['spreadsheetId']
    else:
        spreadsheet_Id = existingSpreadSheeId #spreadsheet['spreadsheetId'] # сохраняем идентификатор файла
    print("Google Sheet URL: ",'https://docs.google.com/spreadsheets/d/' + spreadsheet_Id)
    
    """
    print("Starting grant access")
    driveService = apiclient.discovery.build('drive', 'v3', http = httpAuth) # Выбираем работу с Google Drive и 3 версию API
    access = driveService.permissions().create(
        fileId = spreadsheet_Id,
        body = {'type': 'user', 'role': 'writer', 'emailAddress': 'kvagalex@gmail.com'},  # Открываем доступ на редактирование
        fields = 'id'
    ).execute()
    """
    # Аутентификация юзера для доступа к электронной таблице Google Sheet
    # auth.authenticate_user()
    # service = build('sheets', 'v4', cache_discovery=False)
    # r = service.spreadsheets().values().get(spreadsheetId=spreadsheet_Id, range="Популярное!A1:A999").execute()
    
    print("Replacing NaN values in a dataframe before saving to google sheet...")
    # data=data.dropna()
    # data=data.dropna(axis=0)
    data.fillna("NaNo", inplace = True)
    
    # print("---------------------")
    # print(data.to_string())
    # print(data.describe())
    # print("Head: ", data.head)
    # print("Colums: ", data.columns)
    print("Filling google sheet with values...")
    values = []
    values.append(['name','currency','instrument_type','quantity', 'average_buy_price', 'expected_yield', 'investments', '%Yield'])
    for index, row in data.iterrows():
        # row = k.tolist()
        # print("===")
        # print(row)
        # print("###")
        # print(row['name'])
        # print("---")
        
        values.append([\
                    row['name'], \
                    row['currency'], \
                    row['instrument_type'], \
                    row['quantity'], \
                    row['average_buy_price'], \
                    row['expected_yield'], \
                    row['investments'], \
                    row['%Yield']\
                    ])
        

    
    body = {
        'valueInputOption' : 'RAW',
        'data' : [
            {'range' : 'Лист номер один', 'values' : values},
        ]
    }

    r = service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet_Id, body=body).execute()

def getYieldByInstruments(dFrame):
    df = dFrame[['name', 'quantity', 'average_buy_price', 'currency', 'instrument_type', 'expected_yield', 'investments']]
    # x = df.groupby(['name']).agg({'quantity':'sum', 'average_buy_price':'mean', 'expected_yield':'sum', 'name':'count', 'investments':'sum'})
    x = df.groupby(['name' , 'currency', 'instrument_type']).agg({'quantity':'sum', 'average_buy_price':'mean', 'expected_yield':'sum', 'name':'count', 'investments':'sum'})
    x['average_buy_price'] = x['investments'] / x['quantity']
    x['%Yield'] = (x['expected_yield'] / x['investments'])  * 100 
    x = x.rename(columns={'name': 'count'})
    x = x.reset_index()
    # x['name'] = list(x['name'])[0]
    # print(dFrame)
    # x = dFrame[['name','quantity']].groupby('name')['quantity'].mean()
    print(x.sort_values(by=['%Yield'],ascending=False))
    # print(x.loc[:, x.isna().any()]) # только те колонки: что содержат Nan
    # print(df)
    # print(x.loc[x['instrument_type'].isin(['shares'])])
    """
    for nameOfInstrument in df['name'].explode().unique():
        quantity = 0
        average_buy_price = 0
        expected_yield = 0
        count = 0
        for index,item in df.iterrows():
            # if item["name"] == nameOfInstrument:
            if item["name"][0] ==  nameOfInstrument:
                count+=1
                
        print("========")
    """
    
    return x.sort_values(by=['%Yield'],ascending=False)
    # agg({'quantity':'sum', 'average_buy_price':'avg', 'expected_yield':'sum'})
    # return isinstance(x, pd.DataFrame)

def getMapOfAllFutures(client):
    instruments: InstrumentsService = client.instruments
    r = pd.DataFrame(
        instruments.futures(instrument_status=InstrumentStatus.INSTRUMENT_STATUS_ALL).instruments,
        columns=['name','figi','ticker','class_code']
        )
    return r
def getFutureInfoByFigi(figi, allFutures):
    return allFutures[allFutures['figi'] == figi]
def getFutureNameByFigi(figi, allFutures):
    r = allFutures[allFutures['figi'] == figi]['name']
    return list(r)[0]


def getMapOfAllCurrencies(client):
    instruments: InstrumentsService = client.instruments
    r = pd.DataFrame(
        instruments.currencies(instrument_status=InstrumentStatus.INSTRUMENT_STATUS_ALL).instruments,
        columns=['name','figi','ticker','class_code']
        )
    return r
def getCurrencyInfoByFigi(figi, allCurrencies):
    return allCurrencies[allCurrencies['figi'] == figi]
def getCurrencyNameByFigi(figi, allCurrencies):
    r = allCurrencies[allCurrencies['figi'] == figi]['name']
    return list(r)[0]
    

def getMapOfAllShares(client):
    instruments: InstrumentsService = client.instruments
    r = pd.DataFrame(
        instruments.shares(instrument_status=InstrumentStatus.INSTRUMENT_STATUS_ALL).instruments,
        columns=['name','figi','ticker','class_code']
        )
    return r
def getShareInfoByFigi(figi, allShares):
    return allShares[allShares['figi'] == figi]
def getShareNameByFigi(figi, allShares):
    r = allShares[allShares['figi'] == figi]['name']
    return list(r)[0]
    
def getMapOfAllBonds(client):
    instruments: InstrumentsService = client.instruments
    r = pd.DataFrame(
        instruments.bonds(instrument_status=InstrumentStatus.INSTRUMENT_STATUS_ALL).instruments,
        columns=['name','figi','ticker','class_code']
        )
    return r
def getBondInfoByFigi(figi, allBonds):
    return allBonds[allBonds['figi'] == figi]
def getBondNameByFigi(figi, allBonds):
    r = allBonds[allBonds['figi'] == figi]['name']
    return list(r)[0]


def getMapOfAllETFs(client):
    instruments: InstrumentsService = client.instruments
    r = pd.DataFrame(
        instruments.etfs(instrument_status=InstrumentStatus.INSTRUMENT_STATUS_ALL).instruments,
        columns=['name','figi','ticker','class_code']
        )
    return r
def getETFInfoByFigi(figi, allETFs):
    return allETFs[allETFs['figi'] == figi]
def getETFNameByFigi(figi, allETFS):
    r = allETFS[allETFS['figi'] == figi]['name']
    return list(r)[0]

def portfolio_pose_todict(p : PortfolioPosition, allShares, allBonds, allETFs, allCurrencies, allFutures):
    r = {
        'name': getShareNameByFigi(p.figi, allShares) if p.instrument_type == 'share' else getBondNameByFigi(p.figi, allBonds) if p.instrument_type == 'bond' else getETFNameByFigi(p.figi, allETFs) if p.instrument_type == 'etf' else getCurrencyNameByFigi(p.figi, allCurrencies) if p.instrument_type == 'currency' else getFutureNameByFigi(p.figi, allFutures) if p.instrument_type == 'futures' else 'UnknownInstrumentType_'+p.instrument_type, 
        'figi': p.figi,
        'quantity': cast_money(p.quantity),
        'expected_yield': cast_money(p.expected_yield),
        'instrument_type': p.instrument_type,
        'average_buy_price': cast_money(p.average_position_price),
        'currency': p.average_position_price.currency,
        'nkd': cast_money(p.current_nkd),
    }
    
    """
    if r['currency'] == 'usd':
        # если бы expected_yield быk бы тоже MoneyValue,
        # то конвертацию валюты можно было бы вынести в cast_money
        r['expected_yield'] *= usdrur
        r['average_buy_price'] *= usdrur
        r['nkd'] *= usdrur
    """
    r['sell_sum'] = (r['average_buy_price']*r['quantity']) + r['expected_yield'] + (r['nkd']*r['quantity'])
    r['comission'] = r['sell_sum']*0.003
    r['tax'] = r['expected_yield']*0.013 if r['expected_yield'] > 0 else 0
    r['investments'] = r['average_buy_price']*r['quantity']
    r['%'] = r['expected_yield'] / (r['average_buy_price']*r['quantity']) * 100 if (r['average_buy_price']*r['quantity'])!=0 else 0
    return r

def getCurrentPrice(quantity, average_buy_price, expected_yield):
    # return r['average_buy_price']*r['quantity']) + r['expected_yield']
    return (average_buy_price*quantity + expected_yield)/quantity if quantity>0 else -1

def getAccounts(client):
    r : GetAccountsResponse = client.users.get_accounts()
    df = pd.DataFrame([{'id': p.id, 'name': p.name
    } for p in r.accounts])
    #   print(df)
    return df

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

def loadCredentilas(runfile):
    with open(runfile,"r") as rnf:
        exec(rnf.read())
    print('GOOGLE_PROJECT_CREDENTIALS_FILE_PATH: ', os.environ['GOOGLE_PROJECT_CREDENTIALS_FILE_PATH'])

if __name__ == "__main__":
    loadCredentilas(sys.argv[1])
    run(spreadsheetId=os.environ['GOOGLE_SPREADSHEET_ID'])
