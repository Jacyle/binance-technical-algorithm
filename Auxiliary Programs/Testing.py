# -*- coding: utf-8 -*-
"""
Created on Fri Jan 19 11:46:30 2018

@author: Jacyle
"""

from BinanceAPI import BinanceAPI
from technical_indicators import TechInd
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import config
import sys
import time

start_time = time.time()

ticker = config.ticker

roi_len = 60

days = 7
length = days * 24 * 60

c = BinanceAPI(config.api_key,config.api_secret)

engine = create_engine('$DB_CONNECTION')

df_roi = pd.DataFrame(index=range(0,roi_len+1),columns=['roi'])

run = 0

for i in range(1,roi_len+1):
    
    try:
        conn = psycopg2.connect("dbname='$DATABASE_NAME' user='$USER_NAME' host='localhost' password='$PASSWORD'")
        cur = conn.cursor()
    except:
        print ("I am unable to connect to the database")
        
    minutes = i
    
    cur.execute("""SELECT closetime, to_number(lastprice,'99999999.99999999') as lastprice, to_number(highprice,'99999999.99999999') as highprice,to_number(lowprice,'99999999.99999999') as lowprice from $TABLE where symbol = %s order by closetime desc limit %s """,(ticker.upper(),length))
    df = pd.DataFrame(cur.fetchall()).rename(columns={0: "closetime", 1: "lastprice", 2:"highprice", 3: "lowprice"}).sort_values(by=['closetime']).astype(float)
    
    ema_short = df['lastprice'].ewm(span=12*minutes,min_periods=0,adjust=True,ignore_na=False).mean()
    ema_long = df['lastprice'].ewm(span=24*minutes,min_periods=0,adjust=True,ignore_na=False).mean()
    sma7 = df['lastprice'].ewm(span=6*minutes,min_periods=0,adjust=True,ignore_na=False).mean()
    MACD = ema_short - ema_long   
    MACDsignal = df['lastprice'].ewm(span=6*minutes,min_periods=0,adjust=True,ignore_na=False).mean()
    MACDcross = MACD-MACDsignal
    WMS = TechInd.WMS(df,8*minutes)
    
    #MACDsignal 
    ###Bands
    MACDsignal_mean = MACDsignal.rolling(window=9*minutes,center=False).mean()
    MACDsignal_std = MACDsignal.rolling(window=9*minutes,center=False).std()
    
    MACDsignal_ub2 = MACDsignal_mean + (MACDsignal_std*2)
    MACDsignal_lb2 = MACDsignal_mean - (MACDsignal_std*2)    
    MACDsignal_ub1 = MACDsignal_mean + (MACDsignal_std)
    MACDsignal_lb1 = MACDsignal_mean - (MACDsignal_std)    
    MACDsignal_ub_5 = MACDsignal_mean + (MACDsignal_std*.5)
    MACDsignal_lb_5 = MACDsignal_mean - (MACDsignal_std*.5)    
    ###DataFrame
    MACDsignal_df = MACDsignal.to_frame()
    MACDsignal_ub_df = MACDsignal_ub1.to_frame()
    MACDsignal_lb_df = MACDsignal_lb1.to_frame()
    ###DataFrame with Logic
    MACDsignal_final = MACDsignal_df.join(MACDsignal_ub_df, how='inner',rsuffix='_ub').join(MACDsignal_lb_df, how='inner',rsuffix='_lb')
    MACDsignal_final.loc[(MACDsignal_final['lastprice'] >= MACDsignal_final['lastprice_ub']),'test'] = 'Upper'
    MACDsignal_final.loc[(MACDsignal_final['lastprice'] <= MACDsignal_final['lastprice_lb']),'test'] = 'Lower'
    MACDsignal_final.loc[(MACDsignal_final['lastprice'] < MACDsignal_final['lastprice_ub']) & (MACDsignal_final['lastprice'] > MACDsignal_final['lastprice_lb']),'test'] = 'Mid'
    
    #MACDcross 
    ###Bands       
    MACDcross_mean = MACDcross.rolling(window=9*minutes,center=False).mean()
    MACDcross_std = MACDcross.rolling(window=9*minutes,center=False).std()
    MACDcross_ub2 = MACDcross_mean + (MACDcross_std*2)
    MACDcross_lb2 = MACDcross_mean - (MACDcross_std*2)    
    MACDcross_ub1 = MACDcross_mean + (MACDcross_std)
    MACDcross_lb1 = MACDcross_mean - (MACDcross_std)    
    MACDcross_ub_5 = MACDcross_mean + (MACDcross_std*.5)
    MACDcross_lb_5 = MACDcross_mean - (MACDcross_std*.5)    
    ###DataFrame
    MACDcross_df = MACDcross.to_frame()
    MACDcross_ub_df = MACDcross_ub1.to_frame()
    MACDcross_lb_df = MACDcross_lb1.to_frame()
    ###DataFrame with Logic
    MACDcross_final = MACDcross_df.join(MACDcross_ub_df, how='inner',rsuffix='_ub').join(MACDcross_lb_df, how='inner',rsuffix='_lb')
    MACDcross_final.loc[(MACDcross_final['lastprice'] >= MACDcross_final['lastprice_ub']),'test'] = 'Upper'
    MACDcross_final.loc[(MACDcross_final['lastprice'] <= MACDcross_final['lastprice_lb']),'test'] = 'Lower'
    MACDcross_final.loc[(MACDcross_final['lastprice'] < MACDcross_final['lastprice_ub']) & (MACDcross_final['lastprice'] > MACDcross_final['lastprice_lb']),'test'] = 'Mid'
    
    #SMA 7
    ###Bands
    sma7_mean = sma7.rolling(window=7*minutes,center=False).mean()
    sma7_std = sma7.rolling(window=7*minutes,center=False).std()
    sma7_ub2 = sma7_mean + (sma7_std*2)
    sma7_lb2 = sma7_mean - (sma7_std*2)    
    sma7_ub1_5 = sma7_mean + (sma7_std*1.5)
    sma7_lb1_5 = sma7_mean - (sma7_std*1.5) 
    sma7_ub1_25 = sma7_mean + (sma7_std*1.25)
    sma7_lb1_25 = sma7_mean - (sma7_std*1.25) 
    sma7_ub1 = sma7_mean + (sma7_std)
    sma7_lb1 = sma7_mean - (sma7_std)    
    sma7_ub_5 = sma7_mean + (sma7_std*.5)
    sma7_lb_5 = sma7_mean - (sma7_std*.5)  
    ###DataFrame
    sma7_df = sma7.to_frame()
    sma7_ub_df = sma7_ub1_5.to_frame()
    sma7_lb_df = sma7_lb1_5.to_frame()
    ###DataFrame with Logic
    df1 = df['lastprice'].to_frame()
    sma7_final = df1.join(sma7_ub_df, how='inner',rsuffix='_ub').join(sma7_lb_df, how='inner',rsuffix='_lb')
    sma7_final.loc[(sma7_final['lastprice'] >= sma7_final['lastprice_ub']),'test'] = 'Upper'
    sma7_final.loc[(sma7_final['lastprice'] <= sma7_final['lastprice_lb']),'test'] = 'Lower'
    sma7_final.loc[(sma7_final['lastprice'] < sma7_final['lastprice_ub']) & (sma7_final['lastprice'] > sma7_final['lastprice_lb']),'test'] = 'Mid'
    
    #RSI
    RSI = TechInd.RSI(df1,(8*minutes))
    
    #Final DataFrame
    logic = sma7_final.join(MACDcross_final['test'], how='inner',rsuffix='_MACDcross').join(MACDsignal_final['test'], how='inner',rsuffix='_MACDsignal').join(RSI,how='inner',rsuffix='_rsi').join(WMS, how='inner').dropna()
    
    ### MACD Signal 'Upper'
    logic.loc[(logic['test_MACDsignal'] == 'Upper') & (logic['test_MACDcross'] == 'Upper') & (logic['test'] == 'Upper'),'orderType'] = 'uuu_Sell'
    logic.loc[(logic['test_MACDsignal'] == 'Upper') & (logic['test_MACDcross'] == 'Upper') & (logic['test'] == 'Mid'),'orderType'] = 'Hold'
    logic.loc[(logic['test_MACDsignal'] == 'Upper') & (logic['test_MACDcross'] == 'Upper') & (logic['test'] == 'Lower'),'orderType'] = 'Hold'
    
    logic.loc[(logic['test_MACDsignal'] == 'Upper') & (logic['test_MACDcross'] == 'Mid') & (logic['test'] == 'Upper'),'orderType'] = 'umu_Sell'
    logic.loc[(logic['test_MACDsignal'] == 'Upper') & (logic['test_MACDcross'] == 'Mid') & (logic['test'] == 'Mid'),'orderType'] = 'Hold'
    logic.loc[(logic['test_MACDsignal'] == 'Upper') & (logic['test_MACDcross'] == 'Mid') & (logic['test'] == 'Lower'),'orderType'] = 'Hold'
    
    logic.loc[(logic['test_MACDsignal'] == 'Upper') & (logic['test_MACDcross'] == 'Lower') & (logic['test'] == 'Upper'),'orderType'] = 'Hold'
    logic.loc[(logic['test_MACDsignal'] == 'Upper') & (logic['test_MACDcross'] == 'Lower') & (logic['test'] == 'Mid'),'orderType'] = 'Hold'
    logic.loc[(logic['test_MACDsignal'] == 'Upper') & (logic['test_MACDcross'] == 'Lower') & (logic['test'] == 'Lower'),'orderType'] = 'ull_Buy'
    
    ### MACD Signal 'Mid'
    logic.loc[(logic['test_MACDsignal'] == 'Mid') & (logic['test_MACDcross'] == 'Upper') & (logic['test'] == 'Upper'),'orderType'] = 'Hold'
    logic.loc[(logic['test_MACDsignal'] == 'Mid') & (logic['test_MACDcross'] == 'Upper') & (logic['test'] == 'Mid'),'orderType'] = 'Hold'
    logic.loc[(logic['test_MACDsignal'] == 'Mid') & (logic['test_MACDcross'] == 'Upper') & (logic['test'] == 'Lower'),'orderType'] = 'Hold'
    
    logic.loc[(logic['test_MACDsignal'] == 'Mid') & (logic['test_MACDcross'] == 'Mid') & (logic['test'] == 'Upper'),'orderType'] = 'Hold'
    logic.loc[(logic['test_MACDsignal'] == 'Mid') & (logic['test_MACDcross'] == 'Mid') & (logic['test'] == 'Mid'),'orderType'] = 'Hold'
    logic.loc[(logic['test_MACDsignal'] == 'Mid') & (logic['test_MACDcross'] == 'Mid') & (logic['test'] == 'Lower'),'orderType'] = 'Hold'
    
    logic.loc[(logic['test_MACDsignal'] == 'Mid') & (logic['test_MACDcross'] == 'Lower') & (logic['test'] == 'Upper'),'orderType'] = 'Hold'
    logic.loc[(logic['test_MACDsignal'] == 'Mid') & (logic['test_MACDcross'] == 'Lower') & (logic['test'] == 'Mid'),'orderType'] = 'Hold'
    logic.loc[(logic['test_MACDsignal'] == 'Mid') & (logic['test_MACDcross'] == 'Lower') & (logic['test'] == 'Lower'),'orderType'] = 'Hold'
    
    ### MACD Signal 'Lower'
    logic.loc[(logic['test_MACDsignal'] == 'Lower') & (logic['test_MACDcross'] == 'Upper') & (logic['test'] == 'Upper'),'orderType'] = 'Hold'
    logic.loc[(logic['test_MACDsignal'] == 'Lower') & (logic['test_MACDcross'] == 'Upper') & (logic['test'] == 'Mid'),'orderType'] = 'Hold'
    logic.loc[(logic['test_MACDsignal'] == 'Lower') & (logic['test_MACDcross'] == 'Upper') & (logic['test'] == 'Lower'),'orderType'] = 'Hold'
    
    logic.loc[(logic['test_MACDsignal'] == 'Lower') & (logic['test_MACDcross'] == 'Mid') & (logic['test'] == 'Upper'),'orderType'] = 'Hold'
    logic.loc[(logic['test_MACDsignal'] == 'Lower') & (logic['test_MACDcross'] == 'Mid') & (logic['test'] == 'Mid'),'orderType'] = 'Hold'
    logic.loc[(logic['test_MACDsignal'] == 'Lower') & (logic['test_MACDcross'] == 'Mid') & (logic['test'] == 'Lower'),'orderType'] = 'Hold'
    
    logic.loc[(logic['test_MACDsignal'] == 'Lower') & (logic['test_MACDcross'] == 'Lower') & (logic['test'] == 'Upper'),'orderType'] = 'Hold'
    logic.loc[(logic['test_MACDsignal'] == 'Lower') & (logic['test_MACDcross'] == 'Lower') & (logic['test'] == 'Mid'),'orderType'] = 'Hold'
    logic.loc[(logic['test_MACDsignal'] == 'Lower') & (logic['test_MACDcross'] == 'Lower') & (logic['test'] == 'Lower'),'orderType'] = 'lll_Buy'

    logic.to_sql('back_test', engine, schema='binance', if_exists='replace')

    
    # GET CURRENCY PAIR VALUE AND ORDER MINIMUMS
    ticker_info = c.get_ticker_info()['symbols']
    ticker_info_list = {item['symbol']: item for item in ticker_info}
    ticker_min_qty = float(ticker_info_list['%s' % ticker.upper()]['filters'][1]['minQty'])
    ticker_step_size =  ticker_info_list['%s' % ticker.upper()]['filters'][1]['stepSize'].rstrip('0')[::-1].find('.')
    ticker_min_value = float(ticker_info_list['%s' % ticker.upper()]['filters'][2]['minNotional'])
    ticker_baseAssetPrecision = ticker_info_list['%s' % ticker.upper()]['baseAssetPrecision']
    ticker_quotePrecision = ticker_info_list['%s' % ticker.upper()]['quotePrecision']
    

    # GET ACCOUNT BALANCES FOR THE CURRENCY PAIR
    
    USDT_ticker = 5000.00
    USDT_pair = 5000.00    
    USDT_total = USDT_ticker + USDT_pair
    balance_ticker = USDT_ticker/(logic['lastprice'][0])

    # SET BALANCE LOWER LIMITS BASED ON TOTAL ALLOCATION($) AND SPLIT BETWEEN TRADING PAIR
    min_balance = 0.05 * (USDT_total/2.0)
    
    # trading fee
    # 0.1% per trade, 0.05% if your trading BNB
    fee = .0005
# ORDER QUERY
# CALCULATE USDT_ticker value, (BALANCE_TICKER*row['lastprice'])
# Removed slope, need to work on incorporating
    def TradeTest(row):
        global USDT_total, min_balance, USDT_ticker, USDT_pair, balance_ticker, fee, ticker_step_size, ticker_quotePrecision, ticker_min_value, ticker_min_qty
        
        if row.orderType[-3:] == 'Buy':
            order_qty = round(((((USDT_pair*(row['lastprice_rsi']*row['WMS']))/float(1.00))/row['lastprice'])*(1-(USDT_pair/USDT_total))),ticker_step_size)
            balance_threshold_check = (USDT_pair - (order_qty*row['lastprice']))
            if balance_threshold_check > min_balance:
                order_value = round(order_qty*(row['lastprice']*float(1.00)),ticker_quotePrecision)
                if order_value > (ticker_min_value*float(1.00)) and order_qty > ticker_min_qty:
                    order_msg = "Buy" + " Order: " + str(order_qty) + ", Value: " + str(order_value) 
                    order_id = 'BUY'                   
                    fee_charged = order_value*fee
                    USDT_pair -= order_value
                    balance_ticker += order_qty
                    USDT_ticker = ((balance_ticker * row['lastprice']) - fee_charged)
                    
                    USDT_total = USDT_pair + USDT_ticker
                else:
                    order_msg = "BUY ORDER SIZE BELOW MINIMUM." + " Order:" + str(order_qty) + ", Value: " + str(order_value) 
                    order_id = 'No Order'
                    fee_charged = 0
                    USDT_pair
                    balance_ticker
                    USDT_ticker = balance_ticker * row['lastprice']
                    USDT_total = USDT_pair + USDT_ticker
            else:
                order_value = round(order_qty*(row['lastprice']*float(1.00)),ticker_quotePrecision)
                order_msg = "BUY ORDER EXCEEDS MINIMUM BALANCE LIMIT." + " Order:" + str(order_qty) + ", Value: " + str(order_value) 
                order_id = 'No Order'
                fee_charged = 0
                USDT_pair
                balance_ticker
                USDT_ticker = balance_ticker * row['lastprice']
                USDT_total = USDT_pair + USDT_ticker
                
        elif row.orderType[-4:] == 'Sell':
            order_qty = round(((((USDT_ticker*(row['lastprice_rsi']*(1-row['WMS'])))/float(1.00))/row['lastprice'])*(1-(USDT_ticker/USDT_total))),ticker_step_size)
            balance_threshold_check = (USDT_ticker - (order_qty*row['lastprice']))
            if balance_threshold_check > min_balance:
                order_value = round((order_qty*(row['lastprice']*float(1.00))),ticker_quotePrecision)
                if order_value >= (ticker_min_value*float(1.00)) and order_qty >= ticker_min_qty:
                    order_msg = "Sell" + " Order: " + str(order_qty) + ", Value: " + str(order_value) 
                    order_id = 'SELL'                    
                    fee_charged = order_value*fee
                    USDT_pair += (order_value - fee_charged)
                    balance_ticker -= order_qty
                    USDT_ticker = balance_ticker * row['lastprice']
                    USDT_total = USDT_pair + USDT_ticker
                else:
                    order_msg = "SELL ORDER SIZE BELOW MINIMUM." + " Order:" + str(order_qty) + ", Value: " + str(order_value) 
                    order_id = 'No Order'
                    fee_charged = 0
                    USDT_pair
                    balance_ticker
                    USDT_ticker = balance_ticker * row['lastprice']
                    USDT_total = USDT_pair + USDT_ticker
            else:
                order_value = round((order_qty*(row['lastprice']*float(1.00))),ticker_quotePrecision)
                order_msg = "SELL ORDER EXCEEDS MINIMUM BALANCE LIMIT." + " Order:" + str(order_qty) + ", Value: " + str(order_value) 
                order_id = 'No Order'
                fee_charged = 0
                USDT_pair
                balance_ticker
                USDT_ticker = balance_ticker * row['lastprice']
                USDT_total = USDT_pair + USDT_ticker
        else:
            order_msg = 'Hold - No Action'
            order_id = 'Hold - No Action'
            order_qty = 0
            order_value = 0
            fee_charged = 0
            USDT_pair
            balance_ticker
            USDT_ticker = balance_ticker * row['lastprice']
            USDT_total = USDT_pair + USDT_ticker
            
        return [order_msg, order_id, order_qty, order_value, fee_charged, USDT_pair, USDT_ticker, balance_ticker]
    
    logic['order_msg'], logic['order_id'], logic['order_qty'], logic['order_value'], logic['fee_charged'], logic['USDT_pair'], logic['USDT_ticker'], logic['balance_ticker'] = zip(*logic.apply(TradeTest, axis=1))
    logic['USDT_total']= logic['USDT_pair'] + logic['USDT_ticker']

    old = float(logic['USDT_total'][:1]) 
    new = float(logic['USDT_total'][-1:])
    
    roi = ((new-old)/old)*100
    df_roi.iloc[i] = roi    

    run = run +1
    print("Run " + str(run))    

    if roi >=20:
        sys.exit()

    cur.close()
    conn.close()

df_roi.plot()

print(df_roi[df_roi['roi']== max(df_roi['roi'].dropna())])
print("--- %s seconds ---" % (time.time() - start_time))        