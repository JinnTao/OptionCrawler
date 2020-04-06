#!/usr/bin/env python
# coding=utf-8


import datetime
from urllib import urlencode
import json

import requests
import grequests
import pandas as pd

from utils import replace_expiry_month
from common import dates
import market
import re
import black
import numpy as np
import common
import requests
import mysql.connector
symbols = []

counterparty_id = 5

def fetch_symbols():
    global symbols
    if symbols:
        return True, symbols

    url = 'https://xinhu.tongyuquant.com/bct/quote'
    headers = {
        'Origin': 'https://xinhu.tongyuquant.com:9140',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/70.0.3538.77 Chrome/70.0.3538.77 Safari/537.36',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://xinhu.tongyuquant.com:9140/',
        'Connection': 'keep-alive'
    }

    payload = 'json=%7B%22method%22%3A%22quotingATM%22%7D'

    r = requests.post(url, headers=headers, data=payload)
    if not r.ok:
        return False, None

    data = r.json()
    if data['success'] != True:
        return False, None

    for item in data['data']['result']:
        symbol = item['underlying']
        symbols.append(symbol)

    return True, symbols


def on_error(request, exception):
    print "request failed [request] %s [error] %s" % (request, exception)


def fetch_quote(symbols):
    assert type(symbols) is list

    expiry_dates = []
    notes = []
    opt_prices = []
    call_asks = []
    call_bids = []
    put_asks = []
    put_bids = []
    ss = []
    exchanges = []

    params = []
    http_headers = []

    urls = []
    for symbol in symbols:
        for expiry_date, note in zip(dates(datetime.date.today()), '5T 10T 30C'.split()):
            header = {
                'Origin': 'https://xinhu.tongyuquant.com:9140',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/70.0.3538.77 Chrome/70.0.3538.77 Safari/537.36',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'application/json, text/plain, */*',
                'Referer': 'https://xinhu.tongyuquant.com:9140/option?code=%s' % symbol,
                'Connection': 'keep-alive'
            }

            http_headers.append(header)

            data = {
                "method": "tQuoting",
                "params": {
                    "underlying": symbol,
                    "expireDate": expiry_date,
                    "optionName": "euro"
                }
            }
            param = urlencode({'json': json.dumps(data)})
            params.append(param)
            url = 'https://xinhu.tongyuquant.com/bct/quote'
            urls.append(url)
            exchanges.append(market.exchange(symbol))
            ss.append(replace_expiry_month(symbol))
            expiry_dates.append(expiry_date)
            notes.append(note)

    http_requests = (grequests.post(url, headers=headers, data=param) for url, headers, param in zip(urls, http_headers, params))
    http_responses = grequests.map(http_requests,
                                   size=18,
                                   exception_handler=on_error)

    for r in http_responses:
        opt_price = call_bid0 = call_ask0 = put_bid0 = put_ask0 = None
        if r.ok:
            data = r.json()

            if data['success'] == True:
                if data['data'] != None:
                    price_levels = data['data']['quotingUnits']
                    index = int(len(price_levels)*0.5)
                    price_level = price_levels[index]

                    opt_price = float(data['data']['spot'])
                    call_bid0 = float(price_level['buyCall'])
                    call_ask0 = float(price_level['sellCall'])
                    put_bid0 = float(price_level['buyPut'])
                    put_ask0 = float(price_level['sellPut'])

        opt_prices.append(opt_price)
        call_asks.append(call_bid0)
        call_bids.append(call_ask0)
        put_asks.append(put_bid0)
        put_bids.append(put_ask0)

    return pd.DataFrame({
        'exchange': exchanges,
        'symbol': ss,
        'time': [datetime.datetime.now()] * len(ss),
        'expiry_date': expiry_dates,
        'note': notes,
        'opt_price': opt_prices,
        'call_ask': call_asks,
        'call_bid': call_bids,
        'put_ask': put_asks,
        'put_bid': put_bids,
        'rate': [1] * len(ss)
    })



if __name__ == "__main__":
    #update_vol_info(5)

    ok,symbols = fetch_symbols()
    print ok,symbols
    print fetch_quote(symbols)

    # symbol = 'a1905'
    # header = {
    #     'Origin': 'https://xinhu.tongyuquant.com:9140',
    #     'Accept-Encoding': 'gzip, deflate, br',
    #     'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    #     'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/70.0.3538.77 Chrome/70.0.3538.77 Safari/537.36',
    #     'Content-Type': 'application/x-www-form-urlencoded',
    #     'Accept': 'application/json, text/plain, */*',
    #     'Referer': 'https://xinhu.tongyuquant.com:9140/option?code=%s' % symbol,
    #     'Connection': 'keep-alive'
    # }
    #
    # data = {
    #     "method": "tQuoting",
    #     "params": {
    #         "underlying": symbol,
    #         "expireDate": '2019-01-20',
    #         "optionName": "euro"
    #     }
    # }
    # param = urlencode({'json': json.dumps(data)})
    # print param
    # url = 'https://xinhu.tongyuquant.com/bct/quote'
    #
    # r = requests.post(url, headers=header, data=param)
    # print r.status_code
    # print r.json()