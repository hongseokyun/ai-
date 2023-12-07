import time
import requests
import datetime
import pandas as pd
import csv
import os
import warnings


#필요없는 경고 무시하는 설정
warnings.filterwarnings("ignore", category=FutureWarning)

timestamp = last_update_time = datetime.datetime.now()


def write_csv(fn, data_dict):
    should_write_header = not os.path.exists(fn)
    
    with open(fn, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=data_dict.keys())
        if should_write_header:
            writer.writeheader()
        writer.writerow(data_dict)

        
mid_type = ''

def cal_mid_price (gr_bid_level, gr_ask_level):

    level = 5
    #gr_rB = gr_bid_level.head(level)
    #gr_rT = gr_ask_level.head(level)

    if len(gr_bid_level) > 0 and len(gr_ask_level) > 0:
        bid_top_price = gr_bid_level.iloc[0].price
        bid_top_level_qty = gr_bid_level.iloc[0].quantity
        ask_top_price = gr_ask_level.iloc[0].price
        ask_top_level_qty = gr_ask_level.iloc[0].quantity
        mid_price = (bid_top_price + ask_top_price)* 0.5

        return (mid_price)

    else:
        print ('Error: serious cal_mid_price')
        return (-1)



def live_cal_book_i_v1(param, gr_bid_level, gr_ask_level, var, mid, seq):

    ratio = param[0]
    level = param[1]
    interval = param[2]
    #print ('processing... %s %s,level:%s,interval:%s' % (sys._getframe().f_code.co_name,ratio,level,interval)), 
    
        
    _flag = var['_flag']
        
    if _flag: #skipping first line
        var['_flag'] = False
        return 0.0,0.0
    quant_sum_bid = gr_bid_level.quantity.sum()
    quant_v_bid = gr_bid_level.quantity**ratio
    price_v_bid = gr_bid_level.price * quant_v_bid
    
    quant_sum_ask = gr_ask_level.quantity.sum()
    quant_v_ask = gr_ask_level.quantity**ratio
    price_v_ask = gr_ask_level.price * quant_v_ask
 
    #quant_v_bid = gr_r[(gr_r['type']==0)].quantity**ratio
    #price_v_bid = gr_r[(gr_r['type']==0)].price * quant_v_bid

    #quant_v_ask = gr_r[(gr_r['type']==1)].quantity**ratio
    #price_v_ask = gr_r[(gr_r['type']==1)].price * quant_v_ask
    book_delta = quant_sum_bid - quant_sum_ask
    askQty = quant_v_ask.values.sum()
    bidPx = price_v_bid.values.sum()
    bidQty = quant_v_bid.values.sum()
    askPx = price_v_ask.values.sum()
    bid_ask_spread = interval
        
    book_price = (((askQty*bidPx)/bidQty) + ((bidQty*askPx)/askQty)) / (bidQty+askQty)
    
    indicator_value = (book_price - mid) / bid_ask_spread
    #indicator_value = (book_price - mid_price)
    seq += 1
    if seq < 5:
        return_value = 0
    else:
        df_2 = pd.read_csv(server_csv_file_path)
        seq5_mid_price = df_2.iloc[-4, 2]
        seq1_mid_price = mid_price
        return_value = (seq5_mid_price-seq1_mid_price)/seq1_mid_price
    

    return indicator_value , book_delta, return_value, seq


csv_file_path = "/Users/hongseog-yun/Desktop/2-2/ai 암화이/2023-11-11-bithumb-btc-orderbook_24h.csv"
df = pd.read_csv(csv_file_path)

server_csv_file_path = "/Users/hongseog-yun/Desktop/2-2/ai 암화이/feature_24h.csv"
if os.path.exists(server_csv_file_path):
    df_2 = pd.read_csv(server_csv_file_path)
    
param = [0.2, 5, 1] #ratio,level,interval 
var = {'_flag': False}

seq = 0

grouped_df = df.groupby('timestamp')

for timestamp, group in grouped_df:
    # 각 시간대별로 bids와 asks를 추출
    bids = group[group['type'] == 0].reset_index(drop=True)
    asks = group[group['type'] == 1].reset_index(drop=True)

    listA = {'book-delta': [],
             'book-imbalance': [],
             'mid_price': [],
             'timestamp': [],
             'return_value': []
             }

    # mid_price와 book_imbalance를 계산
    mid_price = cal_mid_price(bids, asks)
    book_imbalance , book_delta, return_value, seq = live_cal_book_i_v1(param, bids, asks, var, mid_price, seq)
    
    # listA에 값을 추가
    listA['mid_price'] = mid_price
    listA['timestamp'] = pd.to_datetime(timestamp)
    listA['book-delta'] = book_delta
    listA['book-imbalance'] = book_imbalance
    listA['return_value'] = '{:.15f}'.format(return_value)

    # CSV 파일에 기록
    write_csv("/Users/hongseog-yun/Desktop/2-2/ai 암화이/feature_24h.csv", listA)
