import sys
import requests
import pandas as pd
import datetime
import time
import os
import urllib
import glob
import logging
# https://testnet.bitmex.com/api/explorer/

# rate limit = 150/5min
exchange = 'BitMEX'
max_count = 500
api_url = 'https://www.bitmex.com/api/v1/'


def concat_data_files():
    pass


def cont_record(end_point, symbol):
    data_label = '{}_{}_{}'.format(end_point, symbol, exchange).replace('/', '_')

    data_directory = os.path.join(os.getcwd(), '..', 'data')
    if not os.path.isdir(data_directory):
        os.mkdir(data_directory)

    this_data_directory = os.path.join(data_directory, data_label)
    if not os.path.isdir(this_data_directory):
        os.mkdir(this_data_directory)

    logging.basicConfig(filename=os.path.join(this_data_directory, 'download.log'), level=logging.INFO)

    this_start = '2017-05-01T00:00:00.000Z'
    data_files = glob.glob(os.path.join(this_data_directory, '{}_*.csv'.format(data_label)))
    if data_files:
        logging.info(max(data_files))
        df = pd.read_csv(max(data_files), index_col=0)
        this_start = df.index[-1]

    while not pd.to_datetime(this_start) > datetime.datetime.today() - datetime.timedelta(hours=1):
        res = bitmex_rest_call(end_point, symbol, this_start)
        filename = '{}_{}.csv'.format(data_label, this_start)
        location_file_per_call = os.path.join(this_data_directory, filename)
        with open(location_file_per_call, 'wb') as f:
            f.write(res)
        df = pd.read_csv(location_file_per_call, index_col=0)
        if this_start == df.index[-1]:
            raise IOError('500 records on one timestamp or no more new data, please check!')
        this_start = df.index[-1]


def bitmex_rest_call(end_point, symbol, this_start):
    params = {'symbol': symbol, 'count': max_count, '_format': 'csv', 'startTime': this_start}
    for i in range(5):
        try:
            r = requests.get(url=api_url + end_point, params=params)
            try:
                limit = r.headers['X-RateLimit-remaining']
                logging.info('Starting timestamp: {}, API call limit left: {}'.format(this_start, limit))
                if int(limit) < 10:
                    logging.info('API rate limit running low. Sleep 5 sec.')
                    time.sleep(5)
            except Exception as e:
                logging.warning(e)
            return r.content
        except urllib.request.URLError as e:
            logging.warning('time out and retry')
    raise IOError('Time out after 5 retries')


if __name__ == "__main__":
    cont_record(*sys.argv[1:])