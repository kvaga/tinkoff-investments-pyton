from ctypes import cast
from tinkoff.invest import Client, RequestError, PositionsResponse, AccessLevel, OperationsResponse, Operation, \
    OperationState, OperationType,PortfolioPosition, PortfolioResponse, InstrumentStatus
import sys
# https://habr.com/en/post/483302/
from googleapiclient.discovery import build
import moex
from typing import Optional

import httplib2 
#import apiclient.discovery
import googleapiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

import pandas as pd
from pandas import DataFrame
import os
import math
"""
- To fix an error
you must change schemas.py file in the python's lib and add the row
REAL_EXCHANGE_DEALER = 4

- To fix error: 65 is not a valid OperationType
in the _grpc_helpers.py in the method protobuf_to_dataclass
add overwriting the variable:
if(pb_value==65): pb_value=22
"""
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
 
#1
def getFullAmountOfInvestmentsInRub(\
                                    data, 
                                    usdrur, hkdrur, cnyrur, eurrur, 
                                    rublesPerAccount \
                                    ):
    print('Getting full amount of investments...')
    data.fillna("NaNo", inplace = True)
    fullAmount=0
    count=0
    amountOfUSD=0; amountOfRUB=0; amountOfCNY=0; amountOfHKD=0; amountOfEUR=0; amountOfOther=0

    print('')
    for index, row in data.iterrows():
        count+=1
        print('name: "', row['name'], '", expected_yield: ', row['expected_yield'], ', investments: ', row['investments'], ', currency: ', row['currency'], ', expected_yield: ',row['expected_yield'])
        # print(row)
        if row['instrument_type'] == 'futures' or row['instrument_type'] == 'currency':
            print('Skipped future or currency: ' + row['name'])
            continue
        if row['currency'] == 'rub':
            """
            if row['name'].strip() == 'USDRUBF Доллар - Рубль':
                fullAmount+=(row['expected_yield']+row['investments']*1000) # todo: добавить запрос стоимость пункта цены для фьючерсов
            else:
                fullAmount+=(row['expected_yield']+row['investments'])
            """
            fullAmount+=(row['expected_yield']+row['investments'])
            amountOfRUB+=row['expected_yield']+row['investments']
            print('[RUB] ',row['name'], ': ', (row['expected_yield']+row['investments']))
            # print(row)
            
        elif row['currency'] == 'usd':
            fullAmount+=(row['expected_yield']+row['investments'])*usdrur
            amountOfUSD+=(row['expected_yield']+row['investments'])
            print('[USD] ',row['name'], ': ', (row['expected_yield']+row['investments'])*usdrur)
        elif row['currency'] == 'cny':
            fullAmount+=(row['expected_yield']+row['investments'])*cnyrur
            amountOfCNY+=row['expected_yield']+row['investments']
            print('[CNY] ',row['name'], ': ', (row['expected_yield']+row['investments'])*cnyrur)
        elif row['currency'] == 'eur':
            fullAmount+=(row['expected_yield']+row['investments'])*eurrur
            amountOfEUR+=row['expected_yield']+row['investments']
            print('[EUR] ',row['name'], ': ', (row['expected_yield']+row['investments'])*eurrur)
        elif row['currency'] == 'hkd':
            fullAmount+=(row['expected_yield']+row['investments'])*hkdrur
            amountOfHKD+=row['expected_yield']+row['investments']
            print('[HKD] ',row['name'], ': ', (row['expected_yield']+row['investments'])*hkdrur)
        else:
            amountOfOther+=row['expected_yield']+row['investments']
            print('[Unknown currency]: ', row['currency'])
    for rub in rublesPerAccount:
        print('[RUB] Amount: ', rub['amount'])
        fullAmount+=rub['amount']
        count+=1
    print('Total Investments Amount in Rubles: ', fullAmount)
    print('--  RUB  --> ', amountOfRUB)
    print('--  USD  --> ', amountOfUSD)
    print('--  EUR  --> ', amountOfEUR)
    print('--  HKD  --> ', amountOfHKD)
    print('--  CNY  --> ', amountOfCNY)
    print('-- Other --> ', amountOfOther)
    print()


    print('Total Count Of Instruments: ', count)
    return fullAmount
def getTimestamp():
    return datetime.timestamp(datetime.now())
def getDateTime():
    # Getting the current date and time
    dt = datetime.now()
    # getting the timestamp
    # ts = datetime.timestamp(dt)
    # print("Date and time is:", dt)
    return dt.strftime("%Y.%m.%d %H:%M:%S")
def getDataFromSpecificPortfolioAndSave2GoogleSheet(accountId, googleSheetName, existingSpreadSheeId, client, allShares, allBonds, allEtfs, allCurrencies, allFutures):
    #2111426330
    r : PortfolioResponse = client.operations.get_portfolio(account_id=accountId)
    df = pd.DataFrame([portfolio_pose_todict(p, allShares, allBonds, allEtfs, allCurrencies, allFutures) for p in r.positions])
    #commonDataframe = []
    #commonDataframe.append(df)
    #print(commonDataframe)
    storePortfolioInfo2GoogleSheet(df, googleSheetName, existingSpreadSheeId)

def storePortfolioInfo2GoogleSheet(data, googleSheetName, existingSpreadSheeId=''):
    GOOGLE_SHEETS_CREDENTIALS_FILE = os.environ['GOOGLE_PROJECT_CREDENTIALS_FILE_PATH']  # Имя файла с закрытым ключом, вы должны подставить свое
    # Читаем ключи из файла
    credentials = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_SHEETS_CREDENTIALS_FILE,
                                                            ['https://www.googleapis.com/auth/spreadsheets',
                                                                     'https://www.googleapis.com/auth/drive'])
    print("Auth process in Google Sheets")
    httpAuth = credentials.authorize(httplib2.Http()) # Авторизуемся в системе
    service = googleapiclient.discovery.build('sheets', 'v4', http = httpAuth) # Выбираем работу с таблицами и 4 версию API
    print("service: ")
    #if( not existingSpreadSheeId):
    #    print("Creating google sheet file...")
    #    spreadsheet = service.spreadsheets().create(body = {
    #                                                       'properties': {'title': 'Первый тестовый документ', 'locale': 'ru_RU'},
    #                                                       'sheets': [{'properties': {'sheetType': 'GRID',
    #                                                       'sheetId': 0,
    #                                                       'title': 'INVESTMENTS ALL',
    #                                                       'gridProperties': {'rowCount': 100, 'columnCount': 15}}}]
    #                                                        }).execute()
    #    print('Created spreadsheet with id: ', spreadsheet['spreadsheetId'])
    #    spreadsheet_Id = spreadsheet['spreadsheetId']
    #else:
    spreadsheet_Id = existingSpreadSheeId #spreadsheet['spreadsheetId'] # сохраняем идентификатор файла
    print("Google Sheet URL: ",'https://docs.google.com/spreadsheets/d/' + spreadsheet_Id)
    print("Replacing NaN values in a dataframe before saving to google sheet...")
    data.fillna("NaNo", inplace = True)
    values = []
    values.append(['name ('+getDateTime()+')',''' '%FromTotalInvestments' ''',''' 'yield_of_bond' ''', 'ticker', 'currency','instrument_type','quantity', 'average_buy_price', 'expected_yield', 'investments'])
    for index, row in data.iterrows():
        values.append([\
                       '' if row['name']==None else row['name'] , \
                       #row['%FromTotalInvestments'],\
                       #row['yield_of_bond'],
                       row['ticker'],\
                       row['currency'], \
                       row['instrument_type'], \
                       row['quantity'], \
                       row['average_buy_price'], \
                       row['expected_yield'], \
                       row['investments']
                      ])
    ### Clearing sheets from old data
    numOfColumns = 20
    numOfRows = 200
    valuesForClearing = []
    ## fulfill columns
    listWithColumns = [''] * numOfColumns
    ## fulfill rows 
    for i in range(numOfRows):
        valuesForClearing.append(listWithColumns)
    fulfillSpreadSheet(service,spreadsheet_Id,googleSheetName, valuesForClearing)
    ### Fullfill sheets with business data
    fulfillSpreadSheet(service,spreadsheet_Id,googleSheetName, values)

def storeOperationsHistoryInfo2GoogleSheet(data, googleSheetName, existingSpreadSheeId=''):
    GOOGLE_SHEETS_CREDENTIALS_FILE = os.environ['GOOGLE_PROJECT_CREDENTIALS_FILE_PATH']  # Имя файла с закрытым ключом, вы должны подставить свое
    # Читаем ключи из файла
    credentials = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_SHEETS_CREDENTIALS_FILE,
                                                            ['https://www.googleapis.com/auth/spreadsheets',
                                                                     'https://www.googleapis.com/auth/drive'])
    print("Auth process in Google Sheets")
    httpAuth = credentials.authorize(httplib2.Http()) # Авторизуемся в системе
    service = googleapiclient.discovery.build('sheets', 'v4', http = httpAuth) # Выбираем работу с таблицами и 4 версию API
    print("service: ")
    spreadsheet_Id = existingSpreadSheeId #spreadsheet['spreadsheetId'] # сохраняем идентификатор файла
    print("Google Sheet URL: ",'https://docs.google.com/spreadsheets/d/' + spreadsheet_Id)
    print("Replacing NaN values in a dataframe before saving to google sheet...")
    data.fillna("NaNo", inplace = True)
    # data.dt.strftime('%Y-%m-%d %H:%M:%S')
    data['date'] = data['date'].astype(str)
    values = []
    values.append(['acc ('+getDateTime()+')',''' 'date' ''', ''' 'type' ''' ,'otype', 'currency','instrument_type','ticker', 'figi', 'quantity', 'state','   payment', 'price'])
    for index, row in data.iterrows():
        values.append([\
                       row['acc'] , \
                       row['date'],\
                       row['type'],
                       row['otype'],\
                       row['currency'], \
                       row['instrument_type'], \
                       row['ticker'], \
                       row['figi'], \
                       row['quantity'], \
                       row['state'], \
                       row['payment'], \
                       row['price'], \

                      ])
    ### Clearing sheets from old data
    numOfColumns = 20
    numOfRows = 200000
    # valuesForClearing = []
    ## fulfill columns
    listWithColumns = [''] * numOfColumns
    ## fulfill rows 
    # for i in range(numOfRows):
    #     valuesForClearing.append(listWithColumns)
    # fulfillSpreadSheet(service,spreadsheet_Id,googleSheetName, valuesForClearing)
    ### Fullfill sheets with business data
    fulfillSpreadSheet(service,spreadsheet_Id,googleSheetName, values)

def get_operations_df(client, account_id : str, allShares, allBonds, allETFs, allCurrencies, allFutures) -> Optional[DataFrame]:
        """
        Преобразую PortfolioResponse в pandas.DataFrame

        :param account_id:
        :return:
        """
        r: OperationsResponse = client.operations.get_operations(
            account_id=account_id,
            from_=datetime(2015,1,1),
            to=datetime.utcnow()
        )

        if len(r.operations) < 1: return None
        df = pd.DataFrame([operation_todict(p, account_id, allShares, allBonds, allETFs, allCurrencies, allFutures) for p in r.operations])
        return df
def operation_todict(o : Operation, account_id : str, allShares, allBonds, allETFs, allCurrencies, allFutures):
        """
        Преобразую PortfolioPosition в dict
        :param p:
        :return:
        """
        ticker = getCommonInstrumetTickerByFigi(o.figi, allShares) if o.instrument_type == 'share' else getCommonInstrumetTickerByFigi(o.figi, allBonds) if o.instrument_type == 'bond' else getCommonInstrumetTickerByFigi(o.figi, allETFs) if o.instrument_type == 'etf' else getCommonInstrumetTickerByFigi(o.figi, allCurrencies) if o.instrument_type == 'currency' else getCommonInstrumetTickerByFigi(o.figi, allFutures) if o.instrument_type == 'futures' else 'UnknownInstrumentType_'+o.instrument_type;
        r = {
            'acc': account_id,
            'date': o.date,
            'type': o.type,
            'otype': o.operation_type,
            'currency': o.currency,
            'instrument_type': o.instrument_type,
            'ticker': ticker,
            'figi': o.figi,
            'quantity': o.quantity,
            'state': o.state,
            'payment': cast_money(o.payment),
            'price': cast_money(o.price),
        }

        return r


def storePricesOfInstrumentsToGoogleSheet(values, sheetName, googleService, spreadsheet_Id):
    # values.fillna("NaNo", inplace = True)
    # values.dt.strftime('%Y-%m-%d %H:%M:%S')
    #values['date'] = values['date'].astype(str)
    # _values = []
    #_values.append(['name ('+getDateTime()+')','%Yield', '%FromTotalInvestments','yield_of_bond', 'ticker', 'currency','instrument_type','quantity', 'average_buy_price', 'expected_yield', 'investments'])
    # for index, row in values.iterrows():
    #     _values.append([\
    #                 row['name'], \
    #                 row['ticker'],\
    #                 row['price']
    #                 ])

    fulfillSpreadSheet(googleService, spreadsheet_Id, sheetName, values)


"""
Для видео по get_portfolio
https://tinkoff.github.io/investAPI/operations/#portfoliorequest
https://tinkoff.github.io/investAPI
https://github.com/Tinkoff/invest-python
"""
def run(yieldsOfAllBonds, googleSheetName, spreadsheetId):        
 
    try:
        with Client(os.environ['TINKOFF_TOKEN_RO']) as client:
            GOOGLE_SHEETS_CREDENTIALS_FILE = os.environ['GOOGLE_PROJECT_CREDENTIALS_FILE_PATH']  # Имя файла с закрытым ключом, вы должны подставить свое
            # Читаем ключи из файла
            credentials = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_SHEETS_CREDENTIALS_FILE, 
                                                                   ['https://www.googleapis.com/auth/spreadsheets', 
                                                                    'https://www.googleapis.com/auth/drive'])
            print("Auth process in Google Sheets")
            httpAuth = credentials.authorize(httplib2.Http()) # Авторизуемся в системе
            googleService = googleapiclient.discovery.build('sheets', 'v4', http = httpAuth) # Выбираем работу с таблицами и 4 версию API

            # r: OperationsResponse = client.operations.get_operations(
            #     account_id=2090759289,
            #     from_=datetime(2015,1,1),
            #     to=datetime.utcnow()
            # )
            #for index, x in getAccounts(client).iterrows():
            #    print(x['id'])
            #getDataFromSpecificPortfolioAndSave2GoogleSheet(client)
            #return
            
            
            print('Getting all maps from tinkoff API...')
            allShares=      getMapOfAllShares(client)
            allBonds =      getMapOfAllBonds(client)
            allEtfs =       getMapOfAllETFs(client)
            allCurrencies = getMapOfAllCurrencies(client)
            allFutures =    getMapOfAllFutures(client)
            
            print('Get Ticker from Figi for Current Prices')
            currentPricesOfAllInstruments = [['ticker ('+getDateTime()+')', 'figi', 'lastPrice']]
            r: GetLastPricesResponse = client.market_data.get_last_prices()
            for instrument in r.last_prices:
                ticker = getCommonInstrumetTickerByFigi(instrument.figi, allShares)
                if(ticker.startswith('figi:')):
                    ticker = getCommonInstrumetTickerByFigi(instrument.figi, allBonds)
                if(ticker.startswith('figi:')):
                    ticker = getCommonInstrumetTickerByFigi(instrument.figi, allEtfs)
                if(ticker.startswith('figi:')):
                    ticker = getCommonInstrumetTickerByFigi(instrument.figi, allFutures)
                if(ticker.startswith('figi:')):
                    ticker = getCommonInstrumetTickerByFigi(instrument.figi, allCurrencies)
                if(ticker.startswith('figi:')):
                    continue
                currentPricesOfAllInstruments.append([ticker, instrument.figi,cast_money(instrument.price)])
                # print()
            # US9100471096
            # allShares=getMapOfAllShares(client)
            # print(allShares).
            storePricesOfInstrumentsToGoogleSheet(currentPricesOfAllInstruments, 'TinkoffAllShares', googleService, spreadsheetId)
            # return
            
            historyOfOperationsSheetName = 'History Of Operations'
            #getDataFromSpecificPortfolioAndSave2GoogleSheet('2111426330', 'My Strategy Portfolio', spreadsheetId, client, allShares, allBonds, allEtfs, allCurrencies, allFutures)
            #return
            commonDataframe = []
            rublesPerAccount = []
            operationsHistoryDf = pd.DataFrame()
            # commonDataframe = pd.DataFrame(columns=['name', 'quantity', 'average_buy_price', 'currency', 'instrument_type', 'expected_yield'])
            # BBG012C34FX0
            #print(allShares)
            #print(allBonds)
            # print(allEtfs)
            #print(getShareNameByFigi('BBG0136BTL03', allShares))
            #getAccounts(client)
            # return
            print('Getting instruments from tinkoff api...')
            
            
            # т.к. есть валятные активы (у меня etf), то нужно их отконвертить в рубли
            # я работаю только в долл, вам возможно будут нужны и др валюты
            # Доллар США :  BBG0013HGFT4
            # Гонконгский доллар :  BBG0013HSW87
            # Евро :  BBG0013HJJ31
            # Юань :  BBG0013HRTL0
            # u = client.market_data.get_last_prices(figi=['USD000UTSTOM'])
            # u = pd.DataFrame([p.LastPrice[] for p in r.last_prices])
            # print(u)
            # u = u[figi=['BBG0013HGFT4']]
            u = client.market_data.get_last_prices(figi=['BBG0013HGFT4'])
            usdrur = cast_money(u.last_prices[0].price)
            u = client.market_data.get_last_prices(figi=['BBG0013HSW87'])
            hkdrur = cast_money(u.last_prices[0].price)
            u = client.market_data.get_last_prices(figi=['BBG0013HJJ31'])
            eurrur = cast_money(u.last_prices[0].price)
            u = client.market_data.get_last_prices(figi=['BBG0013HRTL0'])
            cnyrur = cast_money(u.last_prices[0].price)
            print('usdrur: ', usdrur)
            print('hkdrur: ', hkdrur)
            print('eurrur: ', eurrur)
            print('cnyrur: ', cnyrur)            
            
            for index, row in getAccounts(client).iterrows():
                print(row['id'],': ' , row['name'])
            #for x in [\
            #            '2111522740', #             Маржиналка лонг /
            #            '2111426330', #             Маржиналка шорт /
            #            '2090759289', #                       Коган /
            #            '2000079539', #                         Мой /
            #            '2038244386', #                         ИИС /
            #            '2111421018', # Не выводить - налог большой /
            #            '2111427718', #          ВТБ Мои Инвестиции /
            #            '2111450124', #        Коган Товарные рынки /
            #            '2111497117', #                   Удалить 5 /
            #            '2111378143', #             Открытие Брокер /
            #        ]: 
                #print(x)
                x = row['id']
                
                # print(x)
                print('Processing of account: ', row['name'])
                if x == '2036073431':
                    print('Account ', row['name'], ' was skipped')
                    continue
                
                print('Getting history of operations of account "%s"'%(x))
                # operationsHistoryDf = operationsHistoryDf.append(get_operations_df(client, x, allShares, allBonds, allEtfs, allCurrencies, allFutures), ignore_index=True)
                #operationsHistoryArray.append(operationsHistoryDf)
                operationsHistoryDf = pd.concat([operationsHistoryDf, pd.DataFrame(get_operations_df(client, x, allShares, allBonds, allEtfs, allCurrencies, allFutures))], ignore_index=True)
                print()
                r : PortfolioResponse = client.operations.get_portfolio(account_id=x)
                df = pd.DataFrame([portfolio_pose_todict(p, allShares, allBonds, allEtfs, allCurrencies, allFutures) for p in r.positions])
                # commonDataframe.append(df)
                #
                # print('accountId: ', x, 'totalAmountCurrencies: ', cast_money(r.total_amount_currencies))
                # rublesPerAccount.append({'accountId': x,'amount': cast_money(r.total_amount_currencies)})
                rublesPerAccount.append({'accountId': x,'amount': cast_money(r.total_amount_currencies)})
                """

                # Get all currencies from accounts
                df = df.reset_index()
                for index,k in df.iterrows():
                    if k['instrument_type'] == 'currency':
                        print(k['name'],': ',k['figi'])
                """
                commonDataframe.append(df)
                # pd.concat([commonDataframe,df], ignore_index=True)
                #
                # print(df.sort_values(by=['%'],ascending=False))
                # print(df.size)
                # pd.concat([commonDataframe, df], ignore_index=True, sort=False)
            # print(commonDataframe.sort_values(by=['%'],ascending=False))
            # print(pd.concat(commonDataframe).sort_values(by=['%'],ascending=False))
            # print(getYieldByInstruments(pd.concat(commonDataframe)))
            
            print('Storing history of operations to sheetName=%s and spreadSheetId=%s' % (historyOfOperationsSheetName, spreadsheetId))
            print(operationsHistoryDf)
            storeOperationsHistoryInfo2GoogleSheet(operationsHistoryDf, historyOfOperationsSheetName, spreadsheetId)
            fullAmountOfInvestmentsInRubles = getFullAmountOfInvestmentsInRub(
                                            pd.concat(commonDataframe), 
                                            usdrur, hkdrur, cnyrur, eurrur,
                                            rublesPerAccount
                                            )
            # return
            
            send2GoogleSpreadSheet(fullAmountOfInvestmentsInRubles,
                addYieldForBondsToDataframe(
                    getYieldByInstruments(pd.concat(commonDataframe), fullAmountOfInvestmentsInRubles, yieldsOfAllBonds),yieldsOfAllBonds),
                googleSheetName, spreadsheetId)
                # (x, )

            # print(getYieldByInstruments(commonDataframe))
            getDataFromSpecificPortfolioAndSave2GoogleSheet('2111426330', 'My Strategy Portfolio', spreadsheetId, client, allShares, allBonds, allEtfs, allCurrencies, allFutures)           
            """
            print("bonds", cast_money(r.total_amount_bonds), df.query("instrument_type == 'bond'")['sell_sum'].sum(), sep=" : ")
            print("etfs", cast_money(r.total_amount_etf), df.query("instrument_type == 'etf'")['sell_sum'].sum(), sep=" : ")
            print(df['comission'].sum())
            """
 
    except RequestError as e:
        print(str(e))

def getMarginAccountInfo():
    for index, acc in getAccounts(client).iterrows():
        try:
            print("--> '", acc['id'],'\'')
            r : GetMarginAttributesResponse = client.users.get_margin_attributes(account_id=acc['id'])
            print(acc['name'],': ', cast_money(r.liquid_portfolio))
        except RequestError as err:
            print('Error on account id: ', acc['id'], '. Error: ', err)
        except:
            print('Unknown error on account id: ', acc['id'])
        return

def send2GoogleSpreadSheet(fullAmountOfInvestmentsInRubles, data, googleSheetName, existingSpreadSheeId=''):
    GOOGLE_SHEETS_CREDENTIALS_FILE = os.environ['GOOGLE_PROJECT_CREDENTIALS_FILE_PATH']  # Имя файла с закрытым ключом, вы должны подставить свое
    # Читаем ключи из файла
    credentials = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_SHEETS_CREDENTIALS_FILE, 
                                                                   ['https://www.googleapis.com/auth/spreadsheets', 
                                                                    'https://www.googleapis.com/auth/drive'])
    print("Auth process in Google Sheets")
    httpAuth = credentials.authorize(httplib2.Http()) # Авторизуемся в системе
    service = googleapiclient.discovery.build('sheets', 'v4', http = httpAuth) # Выбираем работу с таблицами и 4 версию API 
    print("service: ")
    if( not existingSpreadSheeId):
        print("Creating google sheet file...")
        spreadsheet = service.spreadsheets().create(body = {
            'properties': {'title': 'Первый тестовый документ', 'locale': 'ru_RU'},
            'sheets': [{'properties': {'sheetType': 'GRID',
                                    'sheetId': 0,
                                    'title': 'INVESTMENTS ALL',
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
        body = {'type': 'user', 'role': 'writer', 'emailAddress': '***@gmail.com'},  # Открываем доступ на редактирование
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
    values.append(['totalAmountInRubles', fullAmountOfInvestmentsInRubles])
    values.append(['name ('+getDateTime()+')','%Yield', '%FromTotalInvestments','yield_of_bond', 'ticker', 'currency','instrument_type','quantity', 'average_buy_price', 'expected_yield', 'investments'])
    for index, row in data.iterrows():
        # row = k.tolist()
        # print("===")
        # print(row)
        # print("###")
        # print(row['name'])
        # print("---")
        # print('QQQ: ' + row['yield_of_bond'])
        values.append([\
                    row['name'], \
                    row['%Yield'],\
                    row['%FromTotalInvestments'],\
                    row['yield_of_bond'],
                    row['ticker'],\
                    row['currency'], \
                    row['instrument_type'], \
                    0 if row['quantity']==-math.inf else row['quantity'], \
                    -123 if row['average_buy_price']==-math.inf else row['average_buy_price'] , \
                    -123 if row['expected_yield']==-math.inf else row['expected_yield'], \
                    row['investments']
                    ])
    ### Clearing sheets from old data
    valuesForClearing = []
    numOfColumns = 20
    numOfRows = 200
    ## fulfill columns
    listWithColumns = [''] * numOfColumns
    # for i in range(numOfColumns):
        # list.append()
    ## fulfill rows
    # for i in range(numOfRows):
        # valuesForClearing.append(listWithColumns)
    # fulfillSpreadSheet(service,spreadsheet_Id,googleSheetName, valuesForClearing)
    clearSheetByName(googleSheetName, service, spreadsheet_Id)
    ### Fullfill sheets with business data
    fulfillSpreadSheet(service,spreadsheet_Id,googleSheetName, values)

def clearSheetByName(sheetName, service, spreadsheet_Id, rangeForClearingData='A1:Z'):
    print('Clearing [{}] sheet...'.format(sheetName))
    rangeAll = '{}!{}'.format( sheetName, rangeForClearingData )
    body = {}
    resultClear = service.spreadsheets( ).values( ).clear( spreadsheetId=spreadsheet_Id, range=rangeAll,
                                                       body=body ).execute( )
def fulfillSpreadSheet(sheetsService, spreadsheet_Id, sheetName, values, rangeForClearingData='A1:Z'):
    clearSheetByName(sheetName, sheetsService, spreadsheet_Id, rangeForClearingData)
    print('Storing prices of instrumets to the sheet {}'.format(sheetName))
    body = {
        'valueInputOption' : 'RAW',
        'data' : [
            {'range' : sheetName, 'values' : values},
        ]
    }
    # print(body)
    r = sheetsService.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet_Id, body=body).execute()

def addYieldForBondsToDataframe(dFrame, yieldsOfAllBonds):
    print('Adding yield for bonds...');
    # print(type(dFrame))
    dFrame['yield_of_bond']='aa'
    for index, row_ in dFrame.iterrows():
        # print('ticker: ' + row_['ticker'])
        dFrame.at[index,'yield_of_bond'] = moex.loadYieldOfBondByTicker(row_['ticker'], yieldsOfAllBonds)
    # for index, row in dFrame.iterrows():
    #     print(row)
    return dFrame
def makeLinksForGoogleSheetsCells(dFrame,):
    for index, row_ in dFrame.iterrows():
        value = "\"userEnteredValue\": {\"formulaValue\":\"=HYPERLINK(\"https://www.tinkoff.ru/invest/"+('futures' if row_['instrument_type']=='futures' else 'bonds' if row_['instrument_type']=='bond' else 'shares' if row_['instrument_type']=='share' else 'currencies' if row_['instrument_type']=='currency' else '') +"/"+row_['ticker']+"/\"; \""+row_['name']+"\")\"}"
        dFrame.at[index,'name'] = value
        print(value)
def getYieldByInstruments(dFrame, fullAmountOfInvestmentsInRubles, yieldsOfAllBonds):
    print("Getting yield...")
    # print('$$$ ' + dFrame['ticker'])
    # print('q: ' + dFrame['yield_of_bond'])
    #_row['ticker'] and dFrame['instrument_type'] == 'bond' else 'Not a bond'
    # print(dFrame['yield_of_bond'])
    # makeLinksForGoogleSheetsCells(dFrame)
    df = dFrame[['name', 'ticker', 'quantity', 'average_buy_price', 'currency', 'instrument_type', 'expected_yield', 'investments']]
    # x = df.groupby(['name']).agg({'quantity':'sum', 'average_buy_price':'mean', 'expected_yield':'sum', 'name':'count', 'investments':'sum'})
    x = df.groupby(['name' , 'ticker', 'currency', 'instrument_type']).agg({'quantity':'sum', 'average_buy_price':'mean', 'expected_yield':'sum', 'name':'count', 'investments':'sum'})
    x['average_buy_price'] = x['investments'] / x['quantity']
    x['%Yield'] = (x['expected_yield'] / x['investments'])  * 100
    x['%FromTotalInvestments'] = (x['investments'] + x['expected_yield'])/fullAmountOfInvestmentsInRubles*100
    # addYieldForBondsToDataframe(x, yieldsOfAllBonds)

    x = x.rename(columns={'name': 'count'})
    x = x.reset_index()
    # x['name'] = list(x['name'])[0]
    # print(dFrame)
    # x = dFrame[['name','quantity']].groupby('name')['quantity'].mean()
    # print(x.sort_values(by=['%Yield'],ascending=False))
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
    return 'figi:'+figi if r.empty else list(r)[0]
    

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
    return 'figi:'+figi if r.empty else list(r)[0]

def getCommonInstrumetTickerByFigi(figi, instruments):
    r = instruments[instruments['figi'] == figi]['ticker']
    return 'figi:'+figi if r.empty else list(r)[0]
    
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
    ticker = getCommonInstrumetTickerByFigi(p.figi, allShares) if p.instrument_type == 'share' else getCommonInstrumetTickerByFigi(p.figi, allBonds) if p.instrument_type == 'bond' else getCommonInstrumetTickerByFigi(p.figi, allETFs) if p.instrument_type == 'etf' else getCommonInstrumetTickerByFigi(p.figi, allCurrencies) if p.instrument_type == 'currency' else getCommonInstrumetTickerByFigi(p.figi, allFutures) if p.instrument_type == 'futures' else 'UnknownInstrumentType_'+p.instrument_type;
    r = {
        'name': getShareNameByFigi(p.figi, allShares) if p.instrument_type == 'share' else getBondNameByFigi(p.figi, allBonds) if p.instrument_type == 'bond' else getETFNameByFigi(p.figi, allETFs) if p.instrument_type == 'etf' else getCurrencyNameByFigi(p.figi, allCurrencies) if p.instrument_type == 'currency' else getFutureNameByFigi(p.figi, allFutures) if p.instrument_type == 'futures' else 'UnknownInstrumentType_'+p.instrument_type, 
        'figi': p.figi,
        'ticker': ticker,
        'quantity': cast_money(p.quantity),
        'expected_yield': cast_money(p.expected_yield),
        'instrument_type': p.instrument_type,
        'average_buy_price': cast_money(p.average_position_price),
        'currency': p.average_position_price.currency,
        'nkd': cast_money(p.current_nkd)
    }
    
    # print('p.average_position_price: ', p.average_position_price)
    # print('cast_money: ', cast_money(p.average_position_price))
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
    yieldOfAllBonds = moex.loadYieldsOfAllBonds();
    
    # print(moex.loadyield_of_bondByTicker('XS2157526315', bondsInfo))
    loadCredentilas(sys.argv[1])
    run(yieldOfAllBonds, 'Лист номер один', spreadsheetId=os.environ['GOOGLE_SPREADSHEET_ID'])
