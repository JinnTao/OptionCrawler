#!/usr/bin/env python
# coding=utf-8


'''东征润和　场外期权报价'''
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


requests.adapters.DEFAULT_RETRIES = 5

begin_date_re = re.compile(r"PageParams.StartDate = '(\d+)-(\d+)-(\d+)';")

counterparty_id = 12

def begin_date():
    s = requests.session()
    s.keep_alive = False
    r = s.get('http://rh.dzqh.com.cn/')

    if not r.ok:
        return False, None

    m = begin_date_re.search(r.content)
    if not m:
        return False, None

    year, month, day = m.groups()
    return datetime.datetime(int(year), int(month), int(day)).strftime("%Y-%m-%d")


symbols = []
def fetch_symbols():
    global symbols
    if symbols:
        return True, symbols

    session = requests.session()
    session.keep_alive = False
    shfe_url = 'http://rh.dzqh.com.cn/GetTargetListByExchage/?Exchange=%E4%B8%8A%E6%9C%9F%E6%89%80'
    r = session.get(shfe_url)
    if not r.ok:
        return False, None

    shfe_symbols = r.json().keys()

    ine_url = 'http://rh.dzqh.com.cn/GetTargetListByExchage/?Exchange=%E4%B8%8A%E8%83%BD%E6%BA%90'
    r = session.get(ine_url)
    if not r.ok:
        return False, None
    ine_symbols = r.json().keys()

    dce_url = 'http://rh.dzqh.com.cn/GetTargetListByExchage/?Exchange=%E5%A4%A7%E5%95%86%E6%89%80'
    r = session.get(dce_url)
    if not r.ok:
        return False, None
    dce_symbols = r.json().keys()

    czce_url = 'http://rh.dzqh.com.cn/GetTargetListByExchage/?Exchange=%E9%83%91%E5%95%86%E6%89%80'
    r = session.get(czce_url)
    if not r.ok:
        return False, None
    czce_symbols = r.json().keys()

    symbols = shfe_symbols + ine_symbols + dce_symbols + czce_symbols
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

    urls = []

    start_date = begin_date()
    #print 'start_date', start_date

    for symbol in symbols:
        for expiry_date, note in zip(dates(datetime.date.today()), '5T 10T 30C'.split()):
            url = 'http://rh.dzqh.com.cn/RefreshPrice/?TargetCode=%s&EndDate=%s' % (symbol, expiry_date)
            #print url
            urls.append(url)
            exchanges.append(market.exchange(symbol))
            ss.append(utils.replace_expiry_month(symbol))
            expiry_dates.append(expiry_date)
            notes.append(note)

    http_requests = (grequests.get(url) for url in urls)
    http_responses = grequests.map(http_requests,
                                   size=12,
                                   exception_handler=on_error)

    for r, expiry_date in zip(http_responses, expiry_dates):
        opt_price = call_bid0 = call_ask0 = put_bid0 = put_ask0 = None

        if r.ok:
            if expiry_date >= start_date:
                data = r.json()

                price_levels = data
                price_level = price_levels[5]

                assert len(price_levels) == 11

                opt_price = float(price_level[0])
                call_bid0 = float(price_level[1])
                call_ask0 = float(price_level[2])
                put_bid0 = float(price_level[3])
                put_ask0 = float(price_level[4])

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
    # sys.exit(main("""
    # ./DzOTCQuoteSource.py
    # --input=
    # --output=
    # """.split()))
    # update_vol_info(12)
    #search_date = datetime.datetime.now().date()
    #print covert_to_stand_df(fetch_quote(['i1905']),12,1.0,search_date)
    ok , list_sym = fetch_symbols()
    print ok,list_sym
    print fetch_quote(list_sym)

    # print start_date
    # print zip(dates(datetime.date.today()), '5T 10T 30C'.split())
    #pass
