#!/usr/bin/env python
# coding=utf-8

'''上海中期场外期权报价'''

import datetime
import sys
import requests
import grequests
import pandas as pd

import utils
from common import dates
import market

import common
from utils import replace_expiry_month

counterparty_id = 19

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
        if note in '5T 10T'.split(): continue

        url = 'http://dsdx.shcifco.com:10084/shcifco_option/custom/atm.json'

        params = {
            'qryDay': expiry_date.replace('-', '')
        }

        r = requests.post(url, data=params)

        if r.ok:
            for item in r.json():
                symbol = replace_expiry_month(item['instrumentId'])
                exchange = market.exchange(symbol)
                opt_price = float(item['lastprice'])
                call_bid0 = float(item['absBuyPrice'])
                call_ask0 = float(item['absSalePrice'])
                put_bid0 = float(item['absBuyPrice'])
                put_ask0 = float(item['absSalePrice'])

                opt_prices.append(opt_price)
                call_asks.append(call_ask0)
                call_bids.append(call_bid0)
                put_asks.append(put_ask0)
                put_bids.append(put_bid0)
                exchanges.append(exchange)
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
    print fetch_quote()