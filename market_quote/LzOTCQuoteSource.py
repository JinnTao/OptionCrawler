#!/usr/bin/env python
# coding=utf-8


import re
import datetime

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

symbols = []

symbol_re = re.compile(r'.option value="\w+".(\w+)..option.')

counterparty_id = 7
def fetch_symbols():
    if symbols:
        return True, symbols

    url = 'http://www.lzqh.net.cn/rest/weixin/otcpricelist'
    r = requests.get(url)
    if not r.ok:
        return False, None

    for line in r.content.split('\n'):
        m = symbol_re.search(line)
        if m:
            symbol = m.groups()[0]
            symbols.append(symbol)

    if symbols:
        return True, symbols

    return False, None


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

    urls = []

    for symbol in symbols:
        for expiry_date, note in zip(dates(datetime.date.today()), '5T 10T 30C'.split()):
            url = 'http://www.lzqh.net.cn/rest/weixin/getOptionPrice?contractcode=%s&expiryDate=%s' % (symbol,
                                                                                                       expiry_date)
            urls.append(url)
            exchanges.append(market.exchange(symbol))
            ss.append(replace_expiry_month(symbol))  # M905 -> M1905
            expiry_dates.append(expiry_date)
            notes.append(note)

    http_requests = (grequests.get(url) for url in urls)
    http_responses = grequests.map(http_requests,
                                   size=18,
                                   exception_handler=on_error)

    for r in http_responses:
        opt_price = call_bid0 = call_ask0 = put_bid0 = put_ask0 = None

        if r.ok:
            data = r.json()

            if data['success'] == True:
                price_levels = data["obj"]["priceList"]
                index = int(len(price_levels)*0.5)
                #print index
                price_level = price_levels[index]

                #assert len(price_levels) == 11

                opt_price = float(price_level['optPrice'])
                call_bid0 = float(price_level['call1'])
                call_ask0 = float(price_level['call2'])
                put_bid0 = float(price_level['put1'])
                put_ask0 = float(price_level['put2'])

        opt_prices.append(opt_price)
        call_asks.append(call_ask0)
        call_bids.append(call_bid0)
        put_asks.append(put_ask0)
        put_bids.append(put_bid0)

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
    #update_vol_info(7)
    pass