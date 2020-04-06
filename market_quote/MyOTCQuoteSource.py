
# coding=utf-8


import re
import datetime
import sys
import requests
import grequests
import pandas as pd

sys.path.append("..")
import utils
from common import dates
import market

import common

import black

import numpy as np
import mysql.connector

from utils import last_settlement_date, close_price

symbols = []
cookie = '26425bbe4dd6d1d3c292ff4070d3000d9523dd52; sessionid=7e70fc16-2da4-11e9-b0e6-9418820a0208'
counterparty_id = 8
def fetch_symbols():
    global symbols
    if symbols:
        return True, symbols

    header = {
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/71.0.3578.80 Chrome/71.0.3578.80 Safari/537.36',
        'accept': 'application/json',
        'referer': 'https://otc.mycapital.net/oop/pricing-management2.html',
        'authority': 'otc.mycapital.net',
        'cookie': cookie
    }

    r = requests.get('https://otc.mycapital.net/api/v1/otc_lw/symbols', headers=header)

    if not r.ok:
        return False, None

    symbols = r.json()['data']

    return True, symbols


def on_error(request, exception):
    print "request failed [request] %s [error] %s" % (request, exception)

def mul(a, b):
    if None in [a, b]:
        return None

    return a*b

def fetch_quote(symbols):
    assert type(symbols) is list

    exchanges = []
    ss = []
    notes = []
    expiry_dates = []
    bid_vols = []
    bid_pcts = []
    ask_vols = []
    ask_pcts = []

    header = {
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/71.0.3578.80 Chrome/71.0.3578.80 Safari/537.36',
        'accept': 'application/json',
        'referer': 'https://otc.mycapital.net/oop/pricing-management2.html',
        'authority': 'otc.mycapital.net',
        'cookie': cookie
    }

    r = requests.get('https://otc.mycapital.net/api/v1/otc_lw/atm_option/pricing?show_diff=true', headers = header)

    quote_date = None

    if r.ok:
        if r.json()['code'] == 0:
            quote_date = r.json()['data']['date']
            for datas in r.json()['data']['pricing']:
                for data in datas['rows']:
                    # print datas
                    # print '\n', data
                    expiry_date = bid_vol = bid_pct = ask_vol = ask_pct = None

                    symbol = utils.replace_expiry_month(data['contract'])

                    bid_vol = data['atm_bid_vol']
                    bid_pct = data['atm_bid_percentage']

                    ask_vol = data['atm_ask_vol']
                    ask_pct = data['atm_ask_percentage']
                    expiry_date = data['maturity']

                    bid_vols.append(bid_vol)
                    bid_pcts.append(bid_pct)
                    ask_vols.append(ask_vol)
                    ask_pcts.append(ask_pct)
                    expiry_dates.append(expiry_date)
                    ss.append(symbol)
                    notes.append('30C')
                    exchanges.append(market.exchange(symbol))

    close_prices = [close_price(symbol, last_settlement_date()) for symbol in ss]
    bids = [mul(pct, price) for pct, price in zip(bid_pcts, close_prices)]
    asks = [mul(pct, price) for pct, price in zip(ask_pcts, close_prices)]

    return pd.DataFrame({
        'exchange': exchanges,
        'symbol': ss,
        'quote_date': [quote_date] * len(ss),
        'expiry_date': expiry_dates,
        'time': [datetime.datetime.now()] * len(ss),
        'note': notes,
        'bid_vol_src': bid_vols,
        'bid_pct': bid_pcts,
        'ask_vol_src': ask_vols,
        'ask_pct': ask_pcts,
        'opt_price': close_prices,
        'call_bid': bids,
        'call_ask': asks,
        'put_bid': bids,
        'put_ask': asks,
        'rate': [1] * len(ss)
    })


if __name__ == "__main__":
    ok, symbols = fetch_symbols()
    print fetch_quote(symbols)