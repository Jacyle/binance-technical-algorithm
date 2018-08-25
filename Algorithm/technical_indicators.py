# -*- coding: utf-8 -*-
"""
Created on Thu Dec 28 15:06:41 2017

@author: Jake
"""
import pandas as pd
import numpy as np
from scipy.stats import linregress

class TechInd:

#Simple Moving Average
    def sma(data, window):
        sma = pd.rolling_mean(data["lastprice"], window)
        return sma
   
#Eponential Moving Average
    def ewma(data, window):
       ewma = pd.ewma(data["lastprice"], span = window)
       return ewma
    
#Simple Moving Average Volitility
    def sma_ind(data, window):
        sma = pd.rolling_mean(data["lastprice"], window)
        sma_ratio = (data["lastprice"]/sma)-1
        sma_mean = pd.stats.moments.rolling_mean(sma_ratio,20)
        sma_std = pd.stats.moments.rolling_std(sma_ratio,20)
        sma_ub2 = sma_mean + (sma_std*2)
        sma_lb2 = sma_mean - (sma_std*2)
    
        if pd.Series(sma_ratio).lt(0) == True:
            sma_ind = (abs(sma_ratio)/abs(sma_lb2))*-100
        else:
            sma_ind = (sma_ratio/sma_ub2)*100
        return sma_ind

#Exponential Moving Average Volitility
    def ewma_ind(data, window):
        ewma = pd.ewma(data["lastprice"], span = window)
        ewma_ratio = (data["lastprice"]/ewma)-1
        ewma_mean = pd.stats.moments.rolling_mean(ewma_ratio,60)
        ewma_std = pd.stats.moments.rolling_std(ewma_ratio,60)
        ewma_ub2 = ewma_mean + (ewma_std*2)
        ewma_lb2 = ewma_mean - (ewma_std*2)

        if pd.Series(ewma_ratio).any() < 1:
            ewma_ind = (abs(ewma_ratio)/abs(ewma_lb2))*-100
        else:
            ewma_ind = (ewma_ratio/ewma_ub2)*100
        return ewma_ind
    
#MACD Line and Crossover
    def MACD(data, span_short, span_long):
        ema_short = pd.ewma(data["lastprice"], span=span_short)
        ema_long = pd.ewma(data["lastprice"], span=span_long)
        MACD = ema_short - ema_long   
        return MACD
  
#MACD Cross
    def MACDcross(data, span_short, span_long, span_signal):
        ema_short = pd.ewma(data["lastprice"], span=span_short)
        ema_long = pd.ewma(data["lastprice"], span=span_long)
        MACD = ema_short - ema_long   
        MACDsigline = pd.ewma(MACD, span=span_signal)
        MACDcross = MACD-MACDsigline
        return MACDcross

#Death/Golden Cross (50ma,200ma). Cross up is bullish trend, cross down is bearish trend.
    def DG_Cross(data,short,long):
        short_ma = pd.ewma(data["lastprice"], span = short) 
        long_ma = pd.ewma(data["lastprice"], span = long)
        DGcross = short_ma - long_ma
        return DGcross   

#Williams Overbought/Oversold Index
#Overbought market condition: 20 or less, 
#Oversold market condition: 80 to 100
    def WMS(data,period):
        Ct = data["lastprice"]
#        Hn = pd.ewma(data['highprice'],span = period)
        Hn = data['highprice'].ewm(span = period).mean()
#        Ln = pd.ewma(data['lowprice'],span = period)
        Ln = data['lowprice'].ewm(span = period).mean()
        WMS = (Hn - Ct)/((Hn - Ln) * 1)
        WMS = pd.Series(WMS,name='WMS')
        WMS = WMS.clip(0.2,0.8)
        return WMS

#RSI
    def RSI(data,period):
        rsi_data = data['lastprice']
        rsi_delta = rsi_data.diff().dropna()
        
        up, down = rsi_delta.copy(),rsi_delta.copy()
        up[up<0]=0
        down[down>0]=0
        
        rsi_sma_up = up.rolling(period).mean()
        down = down.abs()
        rsi_sma_down = down.rolling(period).mean()
        
        rsi_sma_1 = rsi_sma_up/rsi_sma_down
        rsi_sma = 100.0 - (100.0/(1.0+rsi_sma_1))
        rsi_series = abs(rsi_sma - rsi_sma.mean())/100
        rsi = rsi_series.dropna()
        return rsi
        
# Weighted SlopeWeighted
    def SlopeWeighted(data,minutes):
        
        length = minutes * 24
        trend = pd.DataFrame(np.arange(1,length+1,1),columns=['trend']).set_index(data.index)
        df24 = data.assign(trend = trend['trend'])
        df12 = df24[int(length/2):]
        df6 = df24[int(length/4):]
        df3 = df24[int(length/8):]
        df1 = df24[int(length/24):]
        
        df24norm = (df24 - df24.min()) / (df24.max() - df24.min())
        df12norm = (df12 - df12.min()) / (df12.max() - df12.min())
        df6norm = (df6 - df6.min()) / (df6.max() - df6.min())
        df3norm = (df3 - df3.min()) / (df3.max() - df3.min())
        df1norm = (df1 - df1.min()) / (df1.max() - df1.min())
        
        output24 = linregress(df24norm['trend'],df24norm['lastprice'])
        output12 = linregress(df12norm['trend'],df12norm['lastprice'])
        output6 = linregress(df6norm['trend'],df6norm['lastprice'])
        output3 = linregress(df3norm['trend'],df3norm['lastprice'])
        output1 = linregress(df1norm['trend'],df1norm['lastprice'])
        
        weighted_slope = (output24[0]*(24/46)) + (output12[0]*(12/46)) + (output6[0]*(6/46)) + (output3[0]*(3/46)) + (output1[0]*(1/46))
        return weighted_slope

#Trading logic for backtesting
    def Trade_Logic(loop,data,engine,output,min_balance,USDT_ticker,USDT_pair,fee,ticker_step_size,ticker_quotePrecision,ticker_min_value,ticker_min_qty):
            
        length = loop[1]
        minutes = loop[0]
        
        df = data[:length].sort_values(by=['closetime'])
        
        ema_short = df['lastprice'].ewm(span=12*minutes,min_periods=0,adjust=True,ignore_na=False).mean()
        ema_long = df['lastprice'].ewm(span=26*minutes,min_periods=0,adjust=True,ignore_na=False).mean()
        sma7 = df['lastprice'].ewm(span=7*minutes,min_periods=0,adjust=True,ignore_na=False).mean()
        MACD = ema_short - ema_long   
        MACDsignal = df['lastprice'].ewm(span=9*minutes,min_periods=0,adjust=True,ignore_na=False).mean()
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
    
        balance_ticker = USDT_ticker/(logic['lastprice'][0])
        
        logic['order_msg'], logic['order_id'], logic['order_qty'], logic['order_value'], logic['fee_charged'], logic['USDT_pair'], logic['USDT_ticker'], logic['balance_ticker'] = zip(*logic.apply(TechInd.TradeTest, axis=1, args =[min_balance,USDT_ticker,USDT_pair,balance_ticker,fee,ticker_step_size,ticker_quotePrecision,ticker_min_value,ticker_min_qty]))
    
        logic['USDT_total']= logic['USDT_pair'] + logic['USDT_ticker']
    
        old = float(logic['USDT_total'][:1]) 
        new = float(logic['USDT_total'][-1:])
        
        roi = ((new-old)/old)*100
        output['ROI_%s' % int(length/(24 * 60))].iloc[minutes] = roi
        print(roi)
        return output,logic
        
#Trading algorithm for backtesting
    def TradeTest(row, min_balance, USDT_ticker, USDT_pair, balance_ticker, fee, ticker_step_size, ticker_quotePrecision, ticker_min_value, ticker_min_qty):
        
        if row.orderType[-3:] == 'Buy':
            order_qty = round(((((USDT_pair*(row['lastprice_rsi']*row['WMS']))/float(1.00))/row['lastprice'])*(1-(USDT_pair/(USDT_ticker+USDT_pair)))),ticker_step_size)
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

                else:
                    order_msg = "BUY ORDER SIZE BELOW MINIMUM." + " Order:" + str(order_qty) + ", Value: " + str(order_value) 
                    order_id = 'No Order'
                    fee_charged = 0
                    USDT_pair
                    balance_ticker
                    USDT_ticker = balance_ticker * row['lastprice']

            else:
                order_value = round(order_qty*(row['lastprice']*float(1.00)),ticker_quotePrecision)
                order_msg = "BUY ORDER EXCEEDS MINIMUM BALANCE LIMIT." + " Order:" + str(order_qty) + ", Value: " + str(order_value) 
                order_id = 'No Order'
                fee_charged = 0
                USDT_pair
                balance_ticker
                USDT_ticker = balance_ticker * row['lastprice']

                
        elif row.orderType[-4:] == 'Sell':
            order_qty = round(((((USDT_ticker*(row['lastprice_rsi']*(1-row['WMS'])))/float(1.00))/row['lastprice'])*(1-(USDT_ticker/(USDT_ticker+USDT_pair)))),ticker_step_size)
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

                else:
                    order_msg = "SELL ORDER SIZE BELOW MINIMUM." + " Order:" + str(order_qty) + ", Value: " + str(order_value) 
                    order_id = 'No Order'
                    fee_charged = 0
                    USDT_pair
                    balance_ticker
                    USDT_ticker = balance_ticker * row['lastprice']

            else:
                order_value = round((order_qty*(row['lastprice']*float(1.00))),ticker_quotePrecision)
                order_msg = "SELL ORDER EXCEEDS MINIMUM BALANCE LIMIT." + " Order:" + str(order_qty) + ", Value: " + str(order_value) 
                order_id = 'No Order'
                fee_charged = 0
                USDT_pair
                balance_ticker
                USDT_ticker = balance_ticker * row['lastprice']

        else:
            order_msg = 'Hold - No Action'
            order_id = 'Hold - No Action'
            order_qty = 0
            order_value = 0
            fee_charged = 0
            USDT_pair
            balance_ticker
            USDT_ticker = balance_ticker * row['lastprice']
            
        return [order_msg, order_id, order_qty, order_value, fee_charged, USDT_pair, USDT_ticker, balance_ticker]
    