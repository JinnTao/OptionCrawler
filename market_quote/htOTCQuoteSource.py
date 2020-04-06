#!/usr/bin/env python
# coding=utf-8
'''华泰采价'''
import websocket
import pandas as pd
import numpy as np
import json
import re
import datetime as dt
import requests
import mysql.connector
import common

counterparty_id = 2

def obtain_otc_quote(counterparty_id,search_date,date_list):
    quote_data = []
    MarketInformation = ["SHFE","DCE","CZCE","INE","SSE","SZSE","CFFEX","SGE"]
    for v in date_list:
        
        url='http://www.htoption.cn/weixin/app/index.php? \
                i=4&c=entry&do=getatmvol&m=ht_otc&mounth={}'.format(v['exp'].strftime('%Y%m%d'))
        #print url
        r = requests.get(url)
        info = json.loads(r.text)
        #print info['Date'],type(info['Date'])
        latestDate = dt.datetime.strptime(info['Date'],'%Y/%m/%d').date()
        #print latestDate
        info = {k:v for k,v in info.iteritems() if k not in ['CodetoEx','Date']}
        dfvv = pd.DataFrame(info)
        quote_table = dfvv.loc[:,['Sina_ContractCode','AskVol','BidVol','MidVol','ExchangeCode']]
        #quote_data.loc[:,'avgVol'] = (quote_data['AskVol'] + quote_data['BidVol'])/2.0
        #print latestDate <= v['exp'], latestDate ,v['exp']
        def vol_convert(vol,start_date,end_date):
            c_days = (end_date - start_date).days
            t_days = common.getBdaysToExpiry(start_date,end_date)
            print __name__,c_days,t_days
            return vol / np.sqrt(365.0/c_days) * np.sqrt(244.0/t_days)

        if latestDate <= v['exp']:
            quote_table['bid_vol'] = quote_table['BidVol'].apply(lambda x : vol_convert(x/100.0,search_date,v['exp']) if x > 0 else np.nan)
            quote_table['ask_vol'] = quote_table['AskVol'].apply(lambda x : vol_convert(x/100.0,search_date,v['exp']) if x > 0 else np.nan)
        else:
            quote_table['bid_vol'] = np.nan
            quote_table['ask_vol'] = np.nan
        quote_table['strike_price_ratio'] = 1.0
        quote_table.loc[:,'counterparty_id'] = counterparty_id
        quote_table.loc[:,'period_comment'] = v['Type']
        quote_table['quote_date'] = search_date
        #quote_table.drop(['AskVol','BidVol','MidVol'],inplace=True,axis = 1)
        quote_table['expiring_date'] = v['exp']
        for key,value in quote_table.iterrows():
            #print item,key,value
            future_code = value['Sina_ContractCode']
            exchange = [x for x in MarketInformation if value['ExchangeCode'] in x][0]
            matchObj = re.search(r'([a-zA-Z]*)([0-9]*)',future_code)
            instr = 'FUT_' + exchange + '_' +  matchObj.group(1).upper() + ":20" + matchObj.group(2)
            quote_table.loc[key,'instrument_name'] = instr
        quote_data.append(quote_table)
    quote_table = pd.concat(quote_data)
    quote_table.sort_values(['Sina_ContractCode','expiring_date'],inplace = True)
    return quote_table

def store_to_db(conn,quote_table):
    cursor = conn.cursor()
    for idx, row in quote_table.iterrows():
        stmt = (
            "REPLACE INTO marketdata.market_impvol_info " +
            "( updatetime, instrument_name, expiry_date, bid_vol, ask_vol,counterparty_id,period_comment,strike_price_ratio) VALUES " +
            "( '{}'".format(dt.datetime.now()) +
            ", '{}'".format(row['instrument_name']) + 
            ", '{}'".format(row['expiring_date'] ) +
            ", {}".format(row['bid_vol']  if not np.isnan(row['bid_vol']) else 'null') +
            ", {}".format(row['ask_vol']   if not np.isnan(row['ask_vol']) else 'null')+
            ", {}".format(row['counterparty_id'] ) +
            ", '{}'".format(row['period_comment'] ) + 
            ", {})".format(row['strike_price_ratio'] )
        )
        #print stmt
        cursor.execute(stmt)
        conn.commit()
        
    cursor.close()
def update_vol_info(id,is_store=False):
    #counterparty_id 
    counterparty_id = id
    # database prepared
    database = {
    'host': 'instance0.mysql.rds.aliyuncs.com',
    'user': 'longbeach',
    'password': 'L0n9beach',
    }
    conn = mysql.connector.connect(**database)

    # prepare date dict
    search_date = dt.datetime.now().date()
    date_list = common.generate_quote_exp_list(common.g_days_to_expiry_group,search_date)
    #print date_list

    # obtain nanhua otc Quote data
    quote_table = obtain_otc_quote(counterparty_id, search_date,date_list)
    #print quote_table

    # update database
    if is_store:
        store_to_db(conn,quote_table)

    return quote_table
if __name__ == "__main__":
    search_date = dt.datetime.now().date()
    date_list = common.generate_quote_exp_list(common.g_days_to_expiry_group,search_date)
    print date_list
    update_vol_info(2)