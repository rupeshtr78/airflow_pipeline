import json
import requests
import schedule
import datetime
import numpy as np
from pytz import timezone
from util.config import config,myTimeZone
from util.util import symbol_list
from kafka import KafkaProducer
from newsapi.newsapi_client import NewsApiClient
import pprint


def get_news():

    api='49bad93fbbd647248bb4de72e48fd187'
    newsapi = NewsApiClient(api_key=api)
    top_headlines = newsapi.get_top_headlines(country='us',category='business',page_size=70,language='en')
    return top_headlines

def get_historical_data(symbol=symbol_list[0],outputsize='compact'):
    url="https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={}&outputsize={}&interval=1min&apikey={}".format(symbol,outputsize,config['api_key'])

    req=requests.get(url)
    if req.status_code==200:
        raw_data=json.loads(req.content)
        try:
            price=raw_data['Time Series (Daily)']

        except KeyError:
            print(raw_data)
            exit()

        rename={'symbol':'symbol',
                'time':'time',
                '1. open':'open',
                '2. high':'high',
                '3. low':'low',
                '4. close':'close',
                '5. adjusted close':'adjusted_close',
                '6. volume':'volume',
                '7. dividend amount':'dividend_amount',
                '8. split coefficient':'split_coefficient'}

        for k,v in price.items():
            v.update({'symbol':symbol,'time':k})
        price=dict((key,dict((rename[k],v) for (k,v) in value.items())) for (key, value) in price.items())
        price=list(price.values())
        print("Get {}/'s historical data today.".format(symbol))

        return price



def get_intraday_data(symbol=symbol_list[0],outputsize='compact',freq='1min'):
    # get data using AlphaAvantage's API
    url="https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={}&outputsize={}&interval={}&apikey={}".format(symbol,outputsize,freq,config['api_key'])
    req=requests.get(url)

    # if request success
    if req.status_code==200:
        raw_data=json.loads(req.content)
        try:
            price=raw_data['Time Series (1min)']
            meta=raw_data['Meta Data']

        except KeyError:
            print(raw_data)
            exit()
        time_zone=meta['6. Time Zone']

        if outputsize=='compact':
            #only get the most recent price
            last_price=price[max(price.keys())]

            #organize data to dict
            value={"symbol":symbol,
                   "time":meta['3. Last Refreshed'],
                   "open":last_price['1. open'],
                   "high":last_price['2. high'],
                   "low":last_price['3. low'],
                   "close":last_price['4. close'],
                   "volume":last_price['5. volume']}

            print('Get {}\'s latest min data at {}'.format(symbol,datetime.datetime.now(timezone(time_zone))))

        # if outputsize='full' get full-length intraday time series
        else:
            rename={'symbol':'symbol',
                    'time':'time',
                    '1. open':'open',
                    '2. high':'high',
                    '3. low':'low',
                    '4. close':'close',
                    '5. volume':'volume'}

            for k,v in price.items():
                v.update({'symbol':symbol,'time':k})
            price=dict((key,dict((rename[k],v) for (k,v) in value.items())) for (key, value) in price.items())
            value=list(price.values())
            print('Get {}\'s full length intraday data.'.format(symbol))


    # if request failed, return a fake data point
    else:
        time_zone=myTimeZone
        print('  Failed: Cannot get {}\'s data at {}:{} '.format(symbol,datetime.datetime.now(timezone(time_zone)),req.status_code))
        value={"symbol":'None',
               "time":'None',
               "open":0.,
               "high":0.,
               "low":0.,
               "close":0.,
               "volume":0.}

    return value,time_zone


if __name__ == '__main__':
    pp = pprint.PrettyPrinter(indent=4)
    # pp.pprint(get_news())
    pp.pprint(get_intraday_data())