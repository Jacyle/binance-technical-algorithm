# -*- coding: utf-8 -*-

import pandas as pd
from technical_indicators import TechInd
from sqlalchemy import create_engine
import config

conn = config.conn
cur = config.cur

tickers = config.ticker

for x in tickers:
    ticker = x
    cur.execute("""select closetime,lastprice,highprice,lowprice from binance.ticker_data where symbol='%s' order by closetime desc limit 720""" % ticker.upper())   
    df = pd.DataFrame(cur.fetchall()).rename(index=str, columns={0: "closetime", 1: "lastprice", 2: "highprice", 3: "lowprice"}).sort_values(by=['closetime'])
    
    minutes = 15
    
    ##### MACD Indicators #####

    ema_short = df['lastprice'].ewm(span=(12*minutes),min_periods=0,adjust=True,ignore_na=False).mean()
    ema_long = df['lastprice'].ewm(span=24*minutes,min_periods=0,adjust=True,ignore_na=False).mean()
    sma7 = df['lastprice'].ewm(span=6*minutes,min_periods=0,adjust=True,ignore_na=False).mean()
    MACD = ema_short - ema_long   
    MACDsignal = MACD.ewm(span=6*minutes,min_periods=0,adjust=True,ignore_na=False).mean()
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
    MACDsignal_final = MACDsignal_df.join(MACDsignal_ub_df, how='inner',rsuffix='_ub').join(MACDsignal_lb_df, how='inner',rsuffix='_lb').dropna() 
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
    MACDcross_final = MACDcross_df.join(MACDcross_ub_df, how='inner',rsuffix='_ub').join(MACDcross_lb_df, how='inner',rsuffix='_lb').dropna() 
    MACDcross_final.loc[(MACDcross_final['lastprice'] >= MACDcross_final['lastprice_ub']),'test'] = 'Upper'
    MACDcross_final.loc[(MACDcross_final['lastprice'] <= MACDcross_final['lastprice_lb']),'test'] = 'Lower'
    MACDcross_final.loc[(MACDcross_final['lastprice'] < MACDcross_final['lastprice_ub']) & (MACDcross_final['lastprice'] > MACDcross_final['lastprice_lb']),'test'] = 'Mid'
    
    #SMA 7
    ###Bands

    sma7_mean = sma7.rolling(window=9*minutes,center=False).mean()
    sma7_std = sma7.rolling(window=9*minutes,center=False).std()
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
    df1 = pd.to_numeric(df['lastprice'],downcast='float').to_frame()
    sma7_final = df1.join(sma7_ub_df, how='inner',rsuffix='_ub').join(sma7_lb_df, how='inner',rsuffix='_lb').dropna() 
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
    
    logic.loc[(logic['test_MACDsignal'] == 'Upper') & (logic['test_MACDcross'] == 'Mid') & (logic['test'] == 'Upper'),'orderType'] = 'Hold'
    logic.loc[(logic['test_MACDsignal'] == 'Upper') & (logic['test_MACDcross'] == 'Mid') & (logic['test'] == 'Mid'),'orderType'] = 'Hold'
    logic.loc[(logic['test_MACDsignal'] == 'Upper') & (logic['test_MACDcross'] == 'Mid') & (logic['test'] == 'Lower'),'orderType'] = 'Hold'
    
    logic.loc[(logic['test_MACDsignal'] == 'Upper') & (logic['test_MACDcross'] == 'Lower') & (logic['test'] == 'Upper'),'orderType'] = 'Hold'
    logic.loc[(logic['test_MACDsignal'] == 'Upper') & (logic['test_MACDcross'] == 'Lower') & (logic['test'] == 'Mid'),'orderType'] = 'Hold'
    logic.loc[(logic['test_MACDsignal'] == 'Upper') & (logic['test_MACDcross'] == 'Lower') & (logic['test'] == 'Lower'),'orderType'] = 'Hold'
    
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
    
    # Write logic to table 
    engine = create_engine('$DB_CONNECTION')
    logic.to_sql(ticker, engine, schema='binance', if_exists='replace')