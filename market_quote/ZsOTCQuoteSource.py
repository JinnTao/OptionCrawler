#!/usr/bin/env python
# coding=utf-8

'''浙商'''


import re
import datetime
import json

import requests
import grequests
import pandas as pd

from utils import replace_expiry_month
from common import dates
import market
import black
import numpy as np
import common
import requests
import mysql.connector

counterparty_id = 15

def fetch_symbols():
    symbols = []
    return True, symbols

def fetch_quote(symbols):
    expiry_dates = []
    notes = []
    opt_prices = []
    call_asks = []
    call_bids = []
    put_asks = []
    put_bids = []
    symbols = symbols
    exchanges = []

    headers = {
        'Origin': 'http://wx.cnzsqh.com',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/71.0.3578.98 Chrome/71.0.3578.98 Safari/537.36',
        'Content-type': 'application/x-www-form-urlencoded',
        'Accept': '*/*',
        'Referer': 'http://wx.cnzsqh.com/QDQuest_page/weixin/pages-react/index.html',
        #'Cookie': 'token=59bea888-d99c-4076-9e58-612bb2526e9d; expire_time=20190123184204',
        'Cookie': 'token=59bea888-d99c-4076-9e58-612bb2526e9d',
        'Connection': 'keep-alive'
    }

    for expiry_date, note in zip(dates(datetime.date.today()), '5T 10T 30C'.split()):
        if note == '5T': continue

        url = 'http://wx.cnzsqh.com/QDQuest/service/weixinService/queryOptions'
        params = 'params=["%s"]' % expiry_date.replace('-', '')

        r = requests.post(url, headers=headers, data=params)
        if not r.ok:
            continue

        datas = r.json()
        if datas['errorCode'] != 0: continue

        for price_level in json.loads(datas['data']):
            exchange = price_level['exchId']
            symbol = price_level['instrumentId']

            opt_price = float(price_level['underlyingPrice'])
            call_bid0 = float(price_level['askPrice'])
            call_ask0 = float(price_level['bidPrice'])
            put_bid0 = float(price_level['askPrice'])
            put_ask0 = float(price_level['bidPrice'])

            opt_prices.append(opt_price)
            call_asks.append(call_bid0)
            call_bids.append(call_ask0)
            put_asks.append(put_bid0)
            put_bids.append(put_ask0)

            symbols.append(replace_expiry_month(symbol))
            expiry_dates.append(expiry_date)
            notes.append(note)
            exchanges.append(exchange)

    return pd.DataFrame({
        'exchange': exchanges,
        'symbol': symbols,
        'time': [datetime.datetime.now()] * len(symbols),
        'expiry_date': expiry_dates,
        'note': notes,
        'opt_price': opt_prices,
        'call_ask': call_asks,
        'call_bid': call_bids,
        'put_ask': put_asks,
        'put_bid': put_bids,
        'rate': [1] * len(symbols)
    })



if __name__ == "__main__":
    #update_vol_info(7)
    import sys
    print fetch_quote()
