# -*- coding: UTF-8 -*-
# @yasinkuyu

import time
import hashlib
import requests
import config
import hmac
import smtplib

try:
    from urllib import urlencode
except ImportError:
    from urllib.parse import urlencode


class BinanceAPI:
    BASE_URL_v1 = "https://www.binance.com/api/v1"
    BASE_URL_v3 = "https://api.binance.com/api/v3"
    PUBLIC_URL = "https://www.binance.com/exchange/public/product"

    def __init__(self, key, secret):
        self.key = key
        self.secret = secret


    def get_ticker(self, market):
        path = "%s/ticker/24hr" % self.BASE_URL_v1
        params = {"symbol": market}
        return self._get_no_sign(path, params)

    def get_ticker_info(self):
        path = "%s/exchangeInfo" % self.BASE_URL_v1
        return self._get_no_sign(path)

    def get_orderbooks(self, market, limit=50):
        path = "%s/depth" % self.BASE_URL_v1
        params = {"symbol": market, "limit": limit}
        return self._get_no_sign(path, params)


    def get_account(self):
        path = "%s/account" % self.BASE_URL_v3
        return self._get(path, {})


    def get_open_orders(self, market, limit = 100):
        path = "%s/openOrders" % self.BASE_URL_v3
        params = {}
        return self._get(path, params)


    def buy_limit(self, market, quantity, rate):
        path = "%s/order" % self.BASE_URL_v3
        params = self._order(market, quantity, "BUY", rate)
        return self._post(path, params)

    def test_buy_limit(self, market, quantity, rate):
        path = "%s/order/test" % self.BASE_URL_v3
        params = self._order(market, quantity, "BUY", rate)
        return self._post(path, params)

    def sell_limit(self, market, quantity, rate):
        path = "%s/order" % self.BASE_URL_v3
        params = self._order(market, quantity, "SELL", rate)
        return self._post(path, params)

    def test_sell_limit(self, market, quantity, rate):
        path = "%s/order/test" % self.BASE_URL_v3
        params = self._order(market, quantity, "SELL", rate)
        return self._post(path, params)

    def buy_market(self, market, quantity):
        path = "%s/order" % self.BASE_URL_v3
        params = {"symbol": market, "side": "BUY", \
            "type": "MARKET", "quantity": quantity}
        return self._post(path, params)


    def sell_market(self, market, quantity):
        path = "%s/order" % self.BASE_URL_v3
        params = {"symbol": market, "side": "SELL", \
            "type": "MARKET", "quantity": quantity}
        return self._post(path, params)


    def query_order(self, market, orderId):
        path = "%s/order" % self.BASE_URL_v3
        params = {"symbol": market, "orderId": orderId}
        return self._get(path, params)


    def cancel(self, market, order_id):
        path = "%s/order" % self.BASE_URL_v3
        params = {"symbol": market, "orderId": order_id}
        return self._delete(path, params)


    def _get_no_sign(self, path, params={}):
        query = urlencode(params)
        url = "%s?%s" % (path, query)
        return requests.get(url, timeout=30, verify=True).json()

    def _sign(self, params={}):
        data = params.copy()

        ts = str(int(1000 * time.time()))
        data.update({"timestamp": ts})

        h = urlencode(data)
        b = bytearray()
        b.extend(self.secret.encode())
        signature = hmac.new(b, msg=h.encode('utf-8'), digestmod=hashlib.sha256).hexdigest()
        data.update({"signature": signature})
        return data  

    def _get(self, path, params={}):
        q = self._sign(params)
        q_t = q.get('timestamp')
        q_s = q.get('signature')
        query = 'timestamp=%s&signature=%s' % (q_t, q_s)
        url = "%s?%s" % (path, query)
        header = {"X-MBX-APIKEY": self.key}
        return requests.get(url, headers=header, \
            timeout=30, verify=True).json()

    def _post(self, path, params={}):
        params.update({"recvWindow": config.recv_window})
        query = urlencode(self._sign(params))
        url = "%s?%s" % (path, query)
        header = {"X-MBX-APIKEY": self.key}
        return requests.post(url, headers=header, \
            timeout=30, verify=True).json()

    def _order(self, market, quantity, side, rate=None):
        params = {}
         
        if rate is not None:
            params["type"] = "LIMIT"
            params["price"] = self._format(rate)
            params["timeInForce"] = "GTC"
        else:
            params["type"] = "MARKET"

        params["symbol"] = market
        params["side"] = side
        params["quantity"] = '%.8f' % quantity
        
        return params

    def _format(self, price):
        return "{:.8f}".format(price)

    def _delete(self, path, params={}):
        params.update({"recvWindow": config.recv_window})
        query = urlencode(self._sign(params))
        url = "%s?%s" % (path, query)
        header = {"X-MBX-APIKEY": self.key}
        return requests.delete(url, headers=header, \
            timeout=30, verify=True).json()
            
    def balance(self, asset):
        self.client = BinanceAPI(config.api_key,config.api_secret)
        balances = self.client.get_account()
        balances['balances'] = {item['asset']: item for item in balances['balances']}
        return balances['balances'][asset]['free']