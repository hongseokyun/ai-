import time
import requests
import datetime
import pandas as pd
import csv
import os
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)

timestamp = last_update_time = datetime.datetime.now()

def write_csv(fn, df):
    should_write_header = os.path.exists(fn)
    if not should_write_header:
        df.to_csv(fn, index=False, header=True, mode='a')
    else:
        with open(fn, 'a') as f:
            f.write('\n')
        df.to_csv(fn, index=False, header=False, mode='a')

while True:
    book = {}
    response = requests.get('https://api.bithumb.com/public/orderbook/BTC_KRW/?count=5')
    book = response.json()

    data = book['data']
    
    #print(data['timestamp'])
    
    timestamp = datetime.datetime.now()
    if ((timestamp-last_update_time).total_seconds() < 1.0):
        continue
    last_update_time = timestamp

    req_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
    #print(req_timestamp)
    
    bids = pd.DataFrame(data['bids']).apply(pd.to_numeric, errors='ignore')
    bids = bids.reset_index(drop=True)
    bids['type'] = 0
    bids['timestamp'] = req_timestamp
    bids.sort_values('price', ascending=False, inplace=True)

    asks = pd.DataFrame(data['asks']).apply(pd.to_numeric, errors='ignore')
    asks = asks.reset_index(drop=True)
    asks['type'] = 1
    asks['timestamp'] = req_timestamp
    asks.sort_values('price', ascending=True, inplace=True)

    df = pd.concat([bids, asks], ignore_index=True)

    write_csv("/root/2023-11-11-bithumb-btc-orderbook.csv", df)

    time.sleep(0.94)
