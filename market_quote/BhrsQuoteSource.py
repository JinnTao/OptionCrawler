#!/usr/bin/env python
# coding=utf-8


import datetime
from urllib import urlencode
import json

import requests

import pandas as pd

from utils import replace_expiry_month
from common import dates, getExpiryByShift, getHolidays
import market
from GtjaOTCQuoteSource import AESCipher


import common
import black
import numpy as np
import mysql.connector
import re


aes = AESCipher()
encrypt_key = 'DuFYNnsaM+Qm5zhR'
decrypt_key = 'xaXP7ok78MXOjpXr'

symbols = []

counterparty_id = 14
def fetch_symbols():
    global symbols
    if symbols:
        return True, symbols

    start_day = datetime.date.today().strftime("%Y-%m-%d")

    encrypt_date = aes.encrypt(start_day, encrypt_key)
    encrypt_date = urlencode({'queryDate': encrypt_date})
    url = 'http://101.95.0.114:8092/quotation/queryOptionQuotation?' + encrypt_date
    r = requests.post(url)
    if not r.ok:
        return False, None

    datas = json.loads(aes.decrypt(r.json()['msgContent'], decrypt_key))
    for item in datas:
        symbol = item['underlyingCode']
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

    begin_date = getExpiryByShift(datetime.date.today(), 9, 'T', getHolidays()).strftime("%Y-%m-%d")
    #print 'begin_date', begin_date

    for expiry_date, note in zip(dates(datetime.date.today()), '5T 10T 30C'.split()):
        if expiry_date > begin_date:

            encrypt_date = aes.encrypt(expiry_date, encrypt_key)
            encrypt_date = urlencode({'valueDate': encrypt_date})
            url = 'http://101.95.0.114:8092/quotation/atMoneyOptionQuotes?' + encrypt_date

            r = requests.post(url)

            opt_price = call_bid0 = call_ask0 = put_bid0 = put_ask0 = None

            if r.ok:
                #print r.content

                data = json.loads(aes.decrypt(r.json()['msgContent'], decrypt_key))
                #print data

                for item in data:
                    opt_price = float(item['spotPrice'])
                    call_bid0 = float(item['buyOptionPrice'])
                    call_ask0 = float(item['sellOptionPrice'])
                    put_bid0 = float(item['buyOptionPrice'])
                    put_ask0 = float(item['sellOptionPrice'])

                    exchanges.append(item['marketCode'])
                    symbol = item['underlyingCode']

                    opt_prices.append(opt_price)
                    call_asks.append(call_ask0)
                    call_bids.append(call_bid0)
                    put_asks.append(put_ask0)
                    put_bids.append(put_bid0)
                    ss.append(replace_expiry_month(symbol))

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
    fetch_symbols()
    print fetch_quote(['CS1905'])
    #update_vol_info(14)