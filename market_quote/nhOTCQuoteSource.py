#!/usr/bin/env python
# coding=utf-8
import sys
sys.path.append("/home/jinntao/longbeach/src/quantbox/tools/python")
import websocket
import pandas as pd
import numpy as np
import json
import re
import datetime as dt
import requests
import mysql.connector
import common


counterparty_id = 1
def market_code_obj(marketinfo,exp_date):
    MarketInformation = ["SHFE","DCE","CZCE","INE","SSE","SZSE","CFFEX","SGE"]
    arr = []
    MarketArr = []
    strike_ratio = 1
    for j in range(len(MarketInformation)):
        obj = {}
        for i in range(len(marketinfo) ):
            if marketinfo[i]['market'] == MarketInformation[j] :
                obj['market'] = marketinfo[i]['market']
                obj['code'] = marketinfo[i]['code']
                obj['expiring_date'] = exp_date
                obj['strike_price_ratio'] = [strike_ratio]
            if len(obj) > 0:

                arr.append(obj)
                obj = {}

    return arr
def short_lived_connection(search_date,date_list,counterparty_id):
    # Short-lived one-off send-receive
    url = 'ws://fd.nanhuacapital.com/fdwsotccfg'
    quote_table = None
    # start
    ws = websocket.create_connection(url,timeout=10)
    ws.send('{"f":260,"r":"H1nzpDxnQvGLmui1Ts7EUA==","b":{}}')
    ws.send('{"f":257,"r":"eT/uZ6FjTjSRHvIO7EexqA==","b":{"market":["SHFE","DCE","CZCE","INE","SSE","SZSE","CFFEX","SGE"]}}')
    #ws.send('{"f":261,"r":"DpoF4ECPkRzCtzPxfataqg==","b":{"market":["SHFE","DCE","CZCE","INE","SSE","SZSE","CFFEX","SGE"]}}')
    # recieve
    data = None
    #content = []
    festival_list = []
    code_list = []
    for i in range(2):
        data = ws.recv()
        data = json.loads(data)
        #print data['r']
        if data['r'] == "H1nzpDxnQvGLmui1Ts7EUA==":
            #print data['b']['info']
            if data['b']['info'] == 'success':
            #    print data['b']['rest_day']
                festival_list.extend(data['b']['rest_day'])
            else:
                festival_list = []
        elif data['r'] == 'eT/uZ6FjTjSRHvIO7EexqA==':
            code_list.extend(data['b']['code_list'])
            
    quote_data = []
    for v in date_list:
        request_data = market_code_obj(code_list,v['exp'].strftime('%Y%m%d'))
        ws.send('{"f":265,"r":"yvzc5rjOst+lEWqK5qH7PA==","b":{"query_list":' + json.dumps(request_data) + "}}")
        vol_data = ws.recv()
        vol_data = json.loads(vol_data)
        quote_table = pd.DataFrame.from_dict(data=vol_data['b']['volatility_list'])
        def vol_convert(vol,start_date,end_date):
            c_days = (end_date - start_date).days
            t_days = common.getBdaysToExpiry(start_date,end_date)
            print __name__,c_days,t_days
            return vol / np.sqrt(365.0/c_days) * np.sqrt(244.0/t_days)
        print quote_table
        quote_table['bid_vol'] = quote_table.apply(lambda x : vol_convert(x['ask_volatility'][0],search_date, dt.datetime.strptime(str(x['expiring_date']),"%Y%m%d").date()) if x['ask_volatility'][0] > 0 else np.nan,axis = 1)
        quote_table['ask_vol'] = quote_table.apply(lambda x : vol_convert(x['bid_volatility'][0],search_date, dt.datetime.strptime(str(x['expiring_date']),"%Y%m%d").date()) if x['bid_volatility'][0] > 0 else np.nan,axis = 1)
        quote_table['strike_price_ratio'] = quote_table['strike_price_ratio'].apply(lambda x : x[0])
        quote_table.loc[:,'counterparty_id'] = counterparty_id
        quote_table.loc[:,'period_comment'] = v['Type']
        quote_table['quote_date'] = search_date
        quote_table.drop(['ask_volatility','bid_volatility'],inplace=True,axis = 1)
        quote_table['expiring_date'] = quote_table['expiring_date'].apply(lambda x : dt.datetime.strptime(str(x),"%Y%m%d").date())
        for key,value in quote_table.iterrows():
            #print item,key,value
            future_code = value['code']
            exchange = value['market']
            matchObj = re.search(r'([a-zA-Z]*)([0-9]*)',future_code)
            instr = 'FUT_' + exchange + '_' +  matchObj.group(1).upper() + ":20" + matchObj.group(2)
            quote_table.loc[key,'instrument_name'] = instr
        
        quote_data.append(quote_table)
    quote_table = pd.concat(quote_data)
    quote_table.sort_values('code',inplace=True)
    ws.close()
    return quote_table

def store_to_db(conn,quote_table):
    cursor = conn.cursor()
    for idx,row in quote_table.iterrows():
        stmt = (
            "REPLACE INTO marketdata.market_impvol_info " +
            "(quote_date, instrument_name, expiry_date, bid_vol, ask_vol,counterparty_id,period_comment,strike_price_ratio) VALUES " +
            "( '{}'".format(row['quote_date']) +
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
   
def update_vol_info(id,is_store = False):
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
    #date_list = [{'Type': '5T', 'exp': dt.date(2019, 1, 15)}, {'Type': '10T', 'exp': dt.date(2019, 1, 22)}, {'Type': '30C', 'exp': dt.date(2019, 2, 11)}]
    print date_list

    # obtain nanhua otc Quote data
    quote_table = short_lived_connection(search_date,date_list,id)
    #print quote_table[quote_table['instrument_name'].str.contains('FUT_DCE_J:')]

    # update database
    if is_store:
        store_to_db(conn,quote_table)
    return quote_table
    
if __name__ == "__main__":
    update_vol_info(1)