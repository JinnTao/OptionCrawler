#!/usr/bin/env python
# coding=utf-8

import requests
import re

from aliyun import keyway_otc_option_quote

img_re = re.compile(r'<img src="(http://img.+.png)".+alt=\"商品期权报价 (\d+).png\"')

def fetch_quote(symbols = []):
    assert type(symbols) is list
    url = 'http://3651198.s1.simu800.com/mobile/mobile/redirectView?params=eyJtZW51Q29kZSI6MTg1MTA5MDgsInRlbXBsYXRlVHlwZSI6MSwiY29tcGFueUNvZGUiOjM2NTExOTgsInRlbXBsYXRlUGF0aCI6ImZtXzFfMDAxIiwidXJsVHlwZSI6MSwibWVudVNob3dUeXBlIjoxLCJ2aWV3U3RhdGUiOmZhbHNlLCJjdXN0b21lckNvZGUiOjAsIndpZCI6Im9DRFpVcy1qRk9VUWF2dzZSOUh6YXJubDI0RmMiLCJ3ZWlYaW5JRCI6Im9DRFpVcy1qRk9VUWF2dzZSOUh6YXJubDI0RmMiLCJjb21wYW55VHlwZSI6IjEiLCJ1c2VyQ29kZSI6MCwidXNlclR5cGUiOjB9'
    r = requests.get(url)
    if not r.ok:
        return False, None

    m = img_re.search(r.content)
    if not m:
        return False, None

    img_src, quote_date = m.groups()
    print img_src
    print quote_date

    return keyway_otc_option_quote(img_src, quote_date)


if __name__ == "__main__":
    fetch_quote()