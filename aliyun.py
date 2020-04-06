#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import urllib2
import base64
import re
import datetime as dt
import requests
import datetime

import pandas as pd

from bs4 import BeautifulSoup

import market
from utils import replace_expiry_month
import db
from utils import last_settlement_date, close_price

def image2base64(img_file):
    if img_file.startswith('http'):
        r = requests.get(img_file)
        if not r.ok:
            return False, None

        return True, base64.b64encode(r.content)

    with open(img_file, 'rb') as infile:
        s = infile.read()
        return True, base64.b64encode(s)


def predict(url,
            appcode,
            img_base64,
            kv_config,
            old_format):
    if not old_format:
        param = {}
        param['image'] = img_base64
        if kv_config is not None:
            param['configure'] = json.dumps(kv_config)
        body = json.dumps(param)
    else:
        param = {}
        pic = {}
        pic['dataType'] = 50
        pic['dataValue'] = img_base64
        param['image'] = pic

        if kv_config is not None:
            conf = {}
            conf['dataType'] = 50
            conf['dataValue'] = json.dumps(kv_config)
            param['configure'] = conf

        inputs = {"inputs": [param]}
        body = json.dumps(inputs)

    headers = {'Authorization': 'APPCODE %s' % appcode}
    request = urllib2.Request(url=url, headers=headers, data=body)
    try:
        response = urllib2.urlopen(request, timeout=10)
        return response.code, response.headers, response.read()
    except urllib2.HTTPError as e:
        return e.code, e.headers, e.read()


def image_to_html(image_file):
    appcode = '3a2d4c46683f4bb2a19caa31b61facfa'
    url = 'https://form.market.alicloudapi.com/api/predict/ocr_table_parse'

    is_old_format = False
    config = {'format': 'html', 'finance': False, 'dir_assure': True}

    ok, img_base64data = image2base64(image_file)
    if not ok:
        print 'image to html error!'
        return None

    stat, header, content = predict(url, appcode, img_base64data, config, is_old_format)
    if stat != 200:
        print 'Http status code: ', stat
        print 'Error msg in header: ', header['x-ca-error-message'] if 'x-ca-error-message' in header else ''
        print 'Error msg in body: ', content
        exit()
    if is_old_format:
        result_str = json.loads(content)['outputs'][0]['outputValue']['dataValue']
    else:
        result_str = content

    result_str = json.loads(result_str)

    assert result_str['success'] == True

    html_table = result_str['tables']
    return html_table


def guangzi_otc_option_quote(image_file):
    table = image_to_html(image_file)

    symbols = []
    bid_vols = []
    ask_vols = []
    bids = []
    asks = []
    expiry_dates = []
    soup = BeautifulSoup(table, 'lxml')
    for index, tr in enumerate(soup.find_all('tr')):
        if index != 0:
            tds = tr.find_all('td')
            symbol = tds[1].get_text().replace(' ', '.').split('.')[0]
            # print symbol
            bid_vol = tds[5].get_text().replace(' ', '.').replace('%', '')
            bid_price = tds[6].get_text().replace(' ', '.').replace('%', '')

            ask_price = tds[7].get_text().replace(' ', '.').replace('%', '')
            ask_vol = tds[8].get_text().replace(' ', '.').replace('%', '')

            print  symbol, bid_vol, bid_price, ask_vol, ask_price

            symbols.append(symbol)
            bid_vols.append(float(bid_vol) / 100)
            ask_vols.append(float(ask_vol) / 100)
            bids.append(float(bid_price) / 100)
            asks.append(float(ask_price) / 100)

    return pd.DataFrame({
        'symbol': symbols,
        'bid_vol': bid_vols,
        'ask_vol': ask_vols,
        'bids': bids,
        'asks': asks
    })


symbol_re = re.compile(r'(\w{1,2}\d{3,4})\.\w+')
def parse_symbol(contents):
    m = symbol_re.search(contents)
    if not m:
        return False, None

    symbol = m.groups()[0]
    # TODO: simple check here
    return True, symbol


date_re = re.compile(r'(\d{4})[\/-]?(\d{1,2})[\/-]?(\d{1,2})')
def parse_date(contents):
    print
    m = date_re.search(contents)
    if not m:
        return None

    year, month, day = m.groups()

    return dt.date(int(year),
                   int(month),
                   int(day))

rate_re = re.compile(r'(\d+\.\d+)%')
def parse_rate(contents):
    contents = contents.replace(' ', '.')
    m = rate_re.search(contents)
    if not m:
        return None

    return float(m.groups()[0]) / 100


def mul(a, b):
    if None in [a, b]:
        return None

    return a*b


def keyway_otc_option_quote(image_file, quote_date):
    table = image_to_html(image_file)

    exchanges = []
    symbols = []
    bid_vols = []
    ask_vols = []
    expiry_dates = []
    notes = []

    soup = BeautifulSoup(table, 'lxml')

    for index, tr in enumerate(soup.find_all('tr')):
        tds = tr.find_all('td')

        if index == 0:
            pass
            # m = date_re.search(tds[0].get_text())
            # if not m:
            #     print 'date error!'
            #     return None
            #
            # quote_date = m.groups()[0]
        else:
            ok, data = parse_symbol(tds[2].get_text().replace(' ', '.'))
            if ok:
                symbol = data
                exchange = market.exchange(symbol)
                if exchange == None:
                    print 'error [symbol]', symbol
                    continue

                expiry_date = parse_date(tds[4].get_text())
                bid_vol = parse_rate(tds[5].get_text())
                ask_vol = parse_rate(tds[6].get_text())

                expiry_date2 = parse_date(tds[7].get_text())
                bid_vol2 = parse_rate(tds[8].get_text())
                ask_vol2 = parse_rate(tds[9].get_text())

                symbols.append(replace_expiry_month(symbol))
                symbols.append(replace_expiry_month(symbol))

                expiry_dates.append(expiry_date)
                expiry_dates.append(expiry_date2)

                if expiry_date > expiry_date2:
                    notes.append('30C')
                    notes.append('10T')
                else:
                    notes.append('10T')
                    notes.append('30C')

                bid_vols.append(bid_vol)
                bid_vols.append(bid_vol2)

                ask_vols.append(ask_vol)
                ask_vols.append(ask_vol2)

    close_prices = [close_price(symbol, last_settlement_date()) for symbol in symbols]
    bids = [mul(vol, price) for vol, price in zip(bid_vols, close_prices)]
    asks = [mul(vol, price) for vol, price in zip(ask_vols, close_prices)]

    return pd.DataFrame({
        'exchange': [market.exchange(symbol) for symbol in symbols],
        'symbol': symbols,
        'quote_date': [quote_date] * len(symbols),
        'time': [datetime.datetime.now()] * len(symbols),
        'expiry_date': expiry_dates,
        'note': notes,
        'bid_vol': bid_vols,
        'ask_vol': ask_vols,
        'opt_price': close_prices,
        'call_bid': bids,
        'call_ask': asks,
        'put_bid': bids,
        'put_ask': asks,
        'rate': [1] * len(symbols)
    })


if __name__ == '__main__':
    print last_settlement_date()
    print  keyway_otc_option_quote('/home/jinntao/Desktop/1747913568.jpg')