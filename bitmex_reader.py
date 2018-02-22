import sys
import requests
import pandas as pd
import datetime
import time
import os
import urllib
import logging
# https://testnet.bitmex.com/api/explorer/

# rate limit = 150/5min
exchange = 'BitMEX'
max_count = 500
api_url = 'https://www.bitmex.com/api/v1/'
# def probe_start()


def cont_record(end_point, symbol):
    filename = '{}_{}_{}.csv'.format(end_point, symbol, exchange)
    print('Data file is {}.'.format(filename))
    if os.path.exists(filename):
        hist = pd.read_csv(filename, index_col=0)
        this_start = hist.index[-1]
    else:
        this_start = ''
    while pd.to_datetime(this_start) < datetime.datetime.today() - datetime.timedelta(hours=1): #bad condition!!!
        print(this_start)
        df = bitmex_rest_call(end_point, symbol, this_start)
        if os.path.exists(filename):
            df.iloc[1:].to_csv(filename, mode='a', header=False)
        else:
            df.to_csv(filename)
        if this_start == df.index[-1]:
            raise IOError('500 records on one timestamp or no more new data, please check!')
        this_start = df.index[-1]


def bitmex_rest_call(end_point, symbol, this_start):
    params = {'symbol': symbol, 'count': max_count, '_format': 'csv', 'startTime': this_start}
    r = requests.get(url=api_url + end_point, params=params)
    for i in range(5):
        try:
            df = pd.read_csv(r.url, index_col=0)
            return df
        except urllib.request.URLError as e:
            print('time out and retry')
    raise IOError('Time out after 5 retries')


if __name__ == "__main__":
    cont_record(*sys.argv[1:])