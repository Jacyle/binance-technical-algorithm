# -*- coding: utf-8 -*-
"""
Created on Fri Jan 19 11:46:30 2018

@author: Jacyle
"""

from BinanceAPI import BinanceAPI
from technical_indicators import TechInd
import pandas as pd
import psycopg2
import itertools
from sqlalchemy import create_engine
import config
import time

c = BinanceAPI(config.api_key,config.api_secret)

start_time = time.time()

ticker = config.ticker

roi_len = 60

days = 7
length = days * 24 * 60
    
USDT_ticker = 5000.00
USDT_pair = 5000.00    

min_balance = 0.05 * ((USDT_ticker + USDT_pair)/2.0)

fee = .0005

df_roi = pd.DataFrame(index=range(0,roi_len+1))

for i in range(1,days+1):
    df_roi['ROI_%s' % i] = pd.Series(index=range(0,roi_len+1))

loop = itertools.product(range(1,(roi_len+1)), range((24*60),((days+1) * 24 * 60),(24*60)))

# GET CURRENCY PAIR VALUE AND ORDER MINIMUMS
ticker_info = c.get_ticker_info()['symbols']
ticker_info_list = {item['symbol']: item for item in ticker_info}
ticker_min_qty = float(ticker_info_list['%s' % ticker.upper()]['filters'][1]['minQty'])
ticker_step_size =  ticker_info_list['%s' % ticker.upper()]['filters'][1]['stepSize'].rstrip('0')[::-1].find('.')
ticker_min_value = float(ticker_info_list['%s' % ticker.upper()]['filters'][2]['minNotional'])
ticker_baseAssetPrecision = ticker_info_list['%s' % ticker.upper()]['baseAssetPrecision']
ticker_quotePrecision = ticker_info_list['%s' % ticker.upper()]['quotePrecision']

engine = create_engine('$DB_CONNECTION')

try:
    conn = psycopg2.connect("$DB_CONNECTION")
    cur = conn.cursor()
except:
    print ("I am unable to connect to the database")

cur.execute("""SELECT closetime, to_number(lastprice,'99999999.99999999') as lastprice, to_number(highprice,'99999999.99999999') as highprice,to_number(lowprice,'99999999.99999999') as lowprice from binance.ticker_data where symbol = %s order by closetime desc limit %s """,(ticker.upper(),length))
df = pd.DataFrame(cur.fetchall()).rename(columns={0: "closetime", 1: "lastprice", 2:"highprice", 3: "lowprice"}).astype(float)


for i in loop:
    TechInd.Trade_Logic(i,df,engine,df_roi,min_balance,USDT_ticker,USDT_pair,fee,ticker_step_size,ticker_quotePrecision,ticker_min_value,ticker_min_qty)

df_roi.index.name = 'Window'
df_roi[1:].to_csv('roi_back_testing.csv')

cur.close()
conn.close()
print("--- %s seconds ---" % (((time.time() - start_time)/60)*60))          