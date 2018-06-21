
from BinanceAPI import BinanceAPI
import config
import pandas as pd
from technical_indicators import TechInd
import datetime

try:
    conn = config.conn
    cur = config.cur
except:
    print ("I am unable to connect to the database")

c = BinanceAPI(config.api_key,config.api_secret)
 
time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")        

tickers = config.ticker

for x in tickers:
    ticker = x

    # CANCEL OPEN ORDERS
    open_order = c.get_open_orders('%s' % ticker.upper())
    
    if type(open_order) is dict:
        order_msg = open_order['msg']
        cur.execute("INSERT INTO binance.trade_log VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (time,order_msg,'','','',0,0,0,0,0,0,0,0))   
        conn.commit()
        BinanceAPI.email(order_msg)
        exit()
    elif open_order == []:    
        prev_order_status = "No open orders"
    else:
        if ticker.upper() == open_order[0]['symbol']:
            open_order_id = open_order[0]['orderId']
            cancel_order = c.cancel('%s' % ticker.upper(),open_order_id)
            try:
                cancel_order['code']
                order_msg = cancel_order
                BinanceAPI.email(order_msg)
                prev_order_status = 'Error cancelling order'
            except KeyError:
                prev_order_status = "Order " + str(cancel_order['orderId']) + " cancelled"
        else:
            prev_order_status = "No open orders"
            
    # FETCH TRADING LOGIC FROM DATABASE
    order_logic = pd.read_sql_query("select * from binance.%s order by index limit 1" % ticker,conn)
    
    order_ticker = '%s' % ticker.upper()
    order_type = order_logic['orderType']
    order_type = order_type[0]
    order_price = float(order_logic['lastprice'])
    # GET CURRENCY VALUE
    if ticker[3:] == 'btc':
        pair_quote = c.get_ticker('BTCUSDT')
    elif ticker[3:] == 'eth':
        pair_quote = c.get_ticker('ETHUSDT')
    elif ticker[3:] == 'bnb':
        pair_quote = c.get_ticker('BNBUSDT')
    else:
        pair_quote=  {'lastPrice' : str(1.0)}
    
    # GET CURRENCY PAIR VALUE AND ORDER MINIMUMS
    ticker_info = c.get_ticker_info()['symbols']
    ticker_info_list = {item['symbol']: item for item in ticker_info}
    ticker_min_qty = float(ticker_info_list['%s' % ticker.upper()]['filters'][1]['minQty'])
    ticker_step_size =  ticker_info_list['%s' % ticker.upper()]['filters'][1]['stepSize'].rstrip('0')[::-1].find('.')
    ticker_min_value = float(ticker_info_list['%s' % ticker.upper()]['filters'][2]['minNotional'])
    ticker_baseAssetPrecision = ticker_info_list['%s' % ticker.upper()]['baseAssetPrecision']
    ticker_quotePrecision = ticker_info_list['%s' % ticker.upper()]['quotePrecision']
    
    # GET ACCOUNT BALANCES FOR THE CURRENCY PAIR
    balance_ticker = round(float(c.balance('%s' % ticker[:3].upper())),ticker_quotePrecision)
    balance_pair = round(float(c.balance('%s' % ticker[3:].upper())),ticker_baseAssetPrecision)
    USDT_ticker = (balance_ticker*(order_price*float(pair_quote['lastPrice'])))
    USDT_pair = balance_pair * float(pair_quote['lastPrice'])
    balance = USDT_ticker + USDT_pair
    order_ticker_adj = (1-(USDT_ticker/balance))
    order_pair_adj = (1-(USDT_pair/balance)) 
    
    # SET BALANCE LOWER LIMITS BASED ON TOTAL ALLOCATION($) AND SPLIT BETWEEN TRADING PAIR
    min_balance = 0.10 * (balance/2.0)
    
    # RSI, WMS and Slope FOR ORDER SIZE
    rsi = float(order_logic['lastprice_rsi'])
    wms = float(order_logic['WMS'])

    cur.execute("""select closetime,lastprice from binance.ticker_data where symbol='%s' order by closetime desc limit %s""" % (ticker.upper(),1440))   
    df = pd.DataFrame(cur.fetchall()).rename(index=str, columns={0: "closetime", 1: "lastprice"}).sort_values(by=['closetime']).astype(float)
    
    weighted_slope = TechInd.SlopeWeighted(df)
    
    if weighted_slope > 0 and order_type[-4:] == 'Sell':
        slope = abs(weighted_slope)
    elif weighted_slope < 0 and order_type[-3:] == 'Buy':
        slope = abs(weighted_slope)
    else:
        slope = 1
        
    # ORDER QUERY
    if order_type[-3:] == 'Buy':
        order_qty = round(((((USDT_pair*(rsi*wms))/float(pair_quote['lastPrice']))/order_price)*order_pair_adj)*slope,ticker_step_size)
        balance_threshold_check = (USDT_pair - (USDT_pair*(rsi*wms)))
        if balance_threshold_check >= min_balance:
            order_value = round(order_qty*(order_price*float(pair_quote['lastPrice'])),ticker_quotePrecision)
            if order_value >= (ticker_min_value*float(pair_quote['lastPrice'])) and order_qty >= ticker_min_qty:
                order = c.buy_limit(order_ticker,order_qty,order_price)
                try:
                    order_msg = order['msg']
                    order_id = 'ERROR'
                except KeyError:
                    order_msg = order_ticker + " " + order_type + " order placed for " + str(order_qty) + " at " + str(order_price)
                    order_id = order['clientOrderId']
                    #order_id = 'order made'
            else:
                order_msg = "BUY ORDER SIZE BELOW MINIMUM." + " Order:" + str(order_qty) + ", Value: " + str(order_qty*order_price) 
                order_id = 'No Order'
        else:
            order_msg = "BUY ORDER EXCEEDS MINIMUM BALANCE LIMIT." + " Order:" + str(order_qty) + ", Value: " + str(order_qty*order_price) 
            order_id = 'No Order'
            
    elif order_type[-4:] == 'Sell':
        order_qty = round(((((USDT_ticker*(rsi*(1-wms)))/float(pair_quote['lastPrice']))/order_price)*order_ticker_adj)*slope,ticker_step_size)
        balance_threshold_check = (USDT_ticker - (USDT_ticker*(rsi*wms)))
        if balance_threshold_check >= min_balance:
            order_value = round((order_qty*(order_price*float(pair_quote['lastPrice']))),ticker_quotePrecision)
            if order_value >= (ticker_min_value*float(pair_quote['lastPrice'])) and order_qty >= ticker_min_qty:
                order = c.sell_limit(order_ticker,order_qty,order_price)
                try:
                    order_msg = order['msg']
                    order_id = 'ERROR'
    
                except KeyError:
                    order_msg = order_ticker + " " + order_type + " order placed for " + str(order_qty) + " at " + str(order_price)
                    order_id = order['clientOrderId']
                    #order_id = 'order made'
            else:
                order_msg = "SELL ORDER SIZE BELOW MINIMUM." + " Order:" + str(order_qty) + ", Value: " + str(order_qty*order_price) 
                order_id = 'No Order'
        else:
            order_msg = "SELL ORDER EXCEEDS MINIMUM BALANCE LIMIT." + " Order:" + str(order_qty) + ", Value: " + str(order_qty*order_price) 
            order_id = 'No Order'
    
    else:
        order_qty = 0
        try:
            order_msg = 'Hold - No Action'
        except NameError:
            order_msg = 'Hold - No Action'
    
        try:
            order_id = 'Hold - No Action'
        except NameError:
            order_id = 'Hold - No Action'
             
    cur.execute("INSERT INTO binance.trade_log VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(time, order_msg, order_ticker, order_id, order_type, order_price, rsi, order_qty, balance_ticker, USDT_ticker, balance_pair, USDT_pair, balance))   
    conn.commit() 