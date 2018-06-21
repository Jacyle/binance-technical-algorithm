
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
        Ct =data["lastprice"].astype('float')
        Hn = data['highprice'].ewm(com=period,min_periods=0,adjust=True,ignore_na=False).mean().dropna()
        Ln = data['lowprice'].ewm(com=period,min_periods=0,adjust=True,ignore_na=False).mean().dropna()
        WMS = (Hn - Ct)/((Hn - Ln) * 1)
        WMS = pd.Series(WMS,name='WMS')
        WMS = WMS.clip(0.01,0.99)
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
    def SlopeWeighted(data):
        length = int(1440)

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