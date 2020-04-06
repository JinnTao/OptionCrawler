#!/usr/bin/env python
# coding=utf-8


import datetime
import time
import sys
import requests
import grequests
import pandas as pd

import utils
from common import dates
import market

import common
from utils import replace_expiry_month


def time_stamp(date_str):
    '''
    :param date_str: 日期字符串format YYYY-MM-DD
    :return: 对应的时间戳
    '''
    timeArray = time.strptime(date_str, "%Y-%m-%d")
    return (int(time.mktime(timeArray)) + 3600*8)*1000


def fetch_symbols():
    return True, []


def fetch_quote(symbols = None):
    expiry_dates = []
    notes = []
    opt_prices = []
    call_asks = []
    call_bids = []
    put_asks = []
    put_bids = []
    ss = []
    exchanges = []

    for expiry_date, note in zip(dates(datetime.date.today()), '5T 10T 30C'.split()):
        ts = time_stamp(expiry_date)
        url = 'http://ysp.heheresource.com/portal/updown/queryTarget'
        params = {
            "pageSize": 99,
            "pageNo": 1,
            "endDate": ts
        }

        r = requests.post(url, json=params)
        if r.ok:
            if r.json()['code'] == 0:
                for item in r.json()['data']['results']:
                    symbol = replace_expiry_month(item['cTarget'])
                    exchange = market.exchange(symbol)

                    opt_price = float(item['cPrice'])
                    call_bid0 = float(item['buyPrice'])
                    call_ask0 = float(item['sellPrice'])
                    put_bid0 = float(item['buyPrice'])
                    put_ask0 = float(item['sellPrice'])

                    opt_prices.append(opt_price)
                    call_asks.append(call_ask0)
                    call_bids.append(call_bid0)
                    put_asks.append(put_ask0)
                    put_bids.append(put_bid0)
                    exchanges.append(exchange)
                    ss.append(symbol)

                    expiry_dates.append(expiry_date)

                    notes.append(note)

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
    print fetch_quote([])