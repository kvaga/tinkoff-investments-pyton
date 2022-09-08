import xmltodict
import pprint
import json
import requests

YIELD_URL_JSON = "https://iss.moex.com/iss/engines/stock/markets/bonds/boards/TQCB/securities.json?iss.meta=off&iss.only=securities&securities.columns=SECID,SHORTNAME,YIELDATPREVWAPRICE"


def loadYieldOfBondByTicker(ticker, yieldsOfBonds):
    for item in yieldsOfBonds['securities']['data']:
        # print('--> ticker: ', ticker, ', item: ', item)
        if item[0] == ticker:
            return item[2]
    return -123
    # print(my_dict['securities']['data'][0])
    # print(my_dict['audience']['id']['@what'])
    #return "" #my_dict

def loadYieldsOfAllBonds():
    bondsInfoJson = getHTTPGetContentJsonFormat(YIELD_URL_JSON)
    return bondsInfoJson

def getHTTPGetContentJsonFormat(_url):
    # location given here
    # location = "delhi technological university"
    # defining a params dict for the parameters to be sent to the API
    # PARAMS = {'address':location}
    # sending get request and saving the response as response object
    r = requests.get(url = _url) #, params = PARAMS)
    # extracting data in json format
    data = r.json()
    return data
  
  
# extracting latitude, longitude and formatted address 
# of the first matching location
# latitude = data['results'][0]['geometry']['location']['lat']
# longitude = data['results'][0]['geometry']['location']['lng']
# formatted_address = data['results'][0]['formatted_address']
  
# printing the output
# print("Latitude:%s\nLongitude:%s\nFormatted Address:%s"
#       %(latitude, longitude,formatted_address))

if __name__ == "__main__":
    # bondsUrlXML = "https://iss.moex.com/iss/engines/stock/markets/bonds/boards/TQCB/securities.xml?iss.meta=off&iss.only=securities&securities.columns=SECID,SHORTNAME,YIELDATPREVWAPRICE"
    bondsInfo = loadYieldsOfAllBonds();
    print(loadYieldOfBondByTicker('XS2157526315', bondsInfo))
