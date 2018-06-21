
import requests
import config
from urllib.parse import urlencode

class queryBinance:
    BASE_URL = "https://www.binance.com/api/v1"

    def __init__(self, key, secret):
        self.key = key
        self.secret = secret

    def get_ticker(self, market):
        path = "%s/ticker/24hr" % self.BASE_URL
        params = {"symbol": market}
        return self._get_no_sign(path, params)

    def _get_no_sign(self, path, params={}):
        query = urlencode(params)
        url = "%s?%s" % (path, query)
        return requests.get(url, timeout=30, verify=True).json()  
        
client = queryBinance(config.api_key,config.api_secret)

try:
    conn = config.conn
    cur = config.cur
except:
    print ("I am unable to connect to the database")

# Select tickers to scrape data for	
	
tickers = ['BTCUSDT','ETHUSDT','NEOUSDT','BNBUSDT','XRPUSDT','BCCUSDT','EOSUSDT',
           'BNBETH','NEOETH','XRPETH','EOSETH','IOTAETH','BCCETH',
           'ETHBTC','EOSBTC','XRPBTC','IOTABTC','BCCBTC','BNBBTC','NEOBTC'
           ]

for x in tickers:
        x = client.get_ticker("%s" % x)
        try:   
            cur.execute("""INSERT INTO binance.ticker_data(timeId,closetime,lastid,prevcloseprice,lastprice,askqty,askprice,count,symbol,quotevolume,volume,bidprice,firstid,lastqty,
                                                   lowprice,bidqty,pricechangepercent,pricechange,highprice,opentime,weightedavgprice,openprice)
                                                   VALUES (to_timestamp(%(closeTime)s/1000),%(closeTime)s,%(lastId)s,%(prevClosePrice)s,%(lastPrice)s,%(askQty)s,%(askPrice)s,%(count)s,%(symbol)s,%(quoteVolume)s,
                                                   %(volume)s,%(bidPrice)s,%(firstId)s,%(lastQty)s,%(lowPrice)s,%(bidQty)s,%(priceChangePercent)s,%(priceChange)s,
                                                   %(highPrice)s,%(openTime)s,%(weightedAvgPrice)s,%(openPrice)s)""", x)   
            conn.commit()                                               
        except:
            print("Load fail")

#Uncomment the following to run algorithm			
			
#import Logic

#import Trading_Algorithm
            
cur.close()
conn.close()