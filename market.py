#!/usr/bin/env python
# coding=utf-8

def exchange(symbol):
    symbol = symbol.lower()[0:2]

    if 'ic' == symbol: return 'CFFEX'
    if 'if' == symbol: return 'CFFEX'
    if 'ih' == symbol: return 'CFFEX'
    if 't1' == symbol: return 'CFFEX'
    if 'tf' == symbol: return 'CFFEX'
    if 'ts' == symbol: return 'CFFEX'

    if 'sc' == symbol: return "INE"

    if 'a1' == symbol: return "DCE"
    if 'a2' == symbol: return "DCE"
    if 'b1' == symbol: return "DCE"
    if 'bb' == symbol: return "DCE"
    if 'c1' == symbol: return "DCE"
    if 'cs' == symbol: return "DCE"
    if 'eg' == symbol: return "DCE"
    if 'fb' == symbol: return "DCE"
    if 'i1' == symbol: return "DCE"
    if 'j1' == symbol: return "DCE"
    if 'jd' == symbol: return "DCE"
    if 'jm' == symbol: return "DCE"
    if 'l1' == symbol: return "DCE"
    if 'm1' == symbol: return "DCE"
    if 'p1' == symbol: return "DCE"
    if 'pp' == symbol: return "DCE"
    if 'v1' == symbol: return "DCE"
    if 'y1' == symbol: return "DCE"

    if 'ag' == symbol: return "SHFE"
    if 'al' == symbol: return "SHFE"
    if 'au' == symbol: return "SHFE"
    if 'bu' == symbol: return "SHFE"
    if 'cu' == symbol: return "SHFE"
    if 'fu' == symbol: return "SHFE"
    if 'hc' == symbol: return "SHFE"
    if 'ni' == symbol: return "SHFE"
    if 'pb' == symbol: return "SHFE"
    if 'rb' == symbol: return "SHFE"
    if 'ru' == symbol: return "SHFE"
    if 'sn' == symbol: return "SHFE"
    if 'sp' == symbol: return "SHFE"
    if 'wr' == symbol: return "SHFE"
    if 'zn' == symbol: return "SHFE"

    if 'ap' == symbol: return 'CZCE'
    if 'cf' == symbol: return 'CZCE'
    if 'cy' == symbol: return 'CZCE'
    if 'fg' == symbol: return 'CZCE'
    if 'jr' == symbol: return 'CZCE'
    if 'lr' == symbol: return 'CZCE'
    if 'ma' == symbol: return 'CZCE'
    if 'oi' == symbol: return 'CZCE'
    if 'pm' == symbol: return 'CZCE'
    if 'ri' == symbol: return 'CZCE'
    if 'rm' == symbol: return 'CZCE'
    if 'rs' == symbol: return 'CZCE'
    if 'sf' == symbol: return 'CZCE'
    if 'sm' == symbol: return 'CZCE'
    if 'sr' == symbol: return 'CZCE'
    if 'ta' == symbol: return 'CZCE'
    if 'wh' == symbol: return 'CZCE'
    if 'zc' == symbol: return 'CZCE'

    return None