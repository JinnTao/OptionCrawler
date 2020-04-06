#!/usr/bin/env python
# coding=utf-8

'''济海贸发场外期权'''

import re
import datetime
import json

import requests
import pandas as pd
from bs4 import BeautifulSoup

from utils import replace_expiry_month
from common import dates
import market

counterparty_id = 17
symbols = []

def fetch_symbols():
    global symbols
    if symbols:
        return True, symbols

    r = requests.get('http://www.jhotc.com/QuoteList')
    if not r.ok:
        return False, None

    soup = BeautifulSoup(r.content, 'lxml')
    for index, select in enumerate(soup.find_all('select')):
        if index == 1:
            for option in select.find_all('option'):
                symbols.append(option.get_text())

    return True, symbols


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

    for expiry_date, note in zip(dates(datetime.date.today()), '5T 10T 30C'.split()):
        if note in '5T 10T'.split(): continue

        r = requests.get('http://www.jhotc.com/QuoteList')
        if not r.ok:
            return False, None

        trs = []
        soup = BeautifulSoup(r.content, 'lxml')
        for index, tr in enumerate(soup.find_all('tr')):
            if index < 3: continue
            trs.append(tr)

        for symbol, tr in zip(symbols, trs):
            tds = tr.find_all('td')

            opt_price = float(tds[2].get_text())
            call_bid0 = float(tds[1].get_text())
            call_ask0 = float(tds[0].get_text())
            put_bid0 = float(tds[4].get_text())
            put_ask0 = float(tds[3].get_text())
            exchange = market.exchange(symbol)

            opt_prices.append(opt_price)
            call_asks.append(call_ask0)
            call_bids.append(call_bid0)
            put_asks.append(put_ask0)
            put_bids.append(put_bid0)
            exchanges.append(exchange)
            ss.append(replace_expiry_month(symbol))

            expiry_dates.append(expiry_date)

            notes.append('30C')

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
    ok, symbols = fetch_symbols()
    print fetch_quote(symbols)