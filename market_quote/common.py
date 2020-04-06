#!/usr/bin/env python
# coding=utf-8
import websocket
import pandas as pd
import numpy as np
import json
import re
import datetime as dt
import calendar
import requests
import mysql.connector
import black
# global config

#exp_date = '20190114' 
g_strike_ratio = 1
#days_to_expiry_group = {7:'C',14:'C',30:'C'}
g_days_to_expiry_group = {5:'T',10:'T',30:'C'}

def getHolidays():
    query = "SELECT * FROM core.holidays"
    database = {
    'host': 'instance0.mysql.rds.aliyuncs.com',
    'user': 'longbeach',
    'password': 'L0n9beach',
    }  
    conn = mysql.connector.connect(**database)
    df = pd.read_sql(query, conn)
    df = df[df['code'] == 'S']
    holidays = set(df['day'])
    return holidays


def getExpiryByShift(start_date,shft,kind = 'T',holidays = []):
    if shft != 0:
        dire = shft / np.abs(shft)
    else:
        dire = 0
        
    shft = np.abs(shft)     
    if kind == 'T':
        exp = start_date
        while (shft > 0):
            exp = exp + dt.timedelta(days=1) * dire
            shft = shft - 1
            while (exp.weekday() == 5 or exp.weekday() == 6 or exp in holidays):
                exp = exp + dire * dt.timedelta(1)
        pass
    elif kind == 'C':
        exp = start_date + dt.timedelta(days=shft) * dire
        while (exp.weekday() == 5 or exp.weekday() == 6 or exp in holidays):
            exp = exp +  dire * dt.timedelta(1)
    else:
        print "kind should be T or C"
    return exp

def generate_quote_exp_list(days_to_expiry_group,start_date):
    date_list = []
    for k,v in days_to_expiry_group.iteritems():
        exps = {}
        exps['exp'] = getExpiryByShift(start_date,k,v,getHolidays())
        exps['Type'] = str(k) + v
        date_list.append(exps)
    date_list.sort(key=lambda x : x['exp'])
    return date_list


def dates(start_date):
    date_5t = getExpiryByShift(start_date, 5, 'T', getHolidays()).strftime("%Y-%m-%d")
    date_10t = getExpiryByShift(start_date, 10, 'T', getHolidays()).strftime("%Y-%m-%d")
    date_30c = getExpiryByShift(start_date, 30, 'C', getHolidays()).strftime("%Y-%m-%d")
    return date_5t, date_10t, date_30c


def lastdate_of_nextmonth():
    d = dt.datetime.now()

    year = d.year
    next_month = d.month+1

    if d.month == 12:
        next_month = 1
        year += 1

    days = calendar.monthrange(year, next_month)[1]
    return dt.datetime(year, next_month, days)


def getBdaysToExpiry(start_date,end_date):
    '''返回输入两个数据之间的交易日天数'''
    from commands import tdate
    dates = tdate(start_date, end_date, mkt='MKT_SHFE', as_python_date=True)   
    bdays = len(dates)
    if len(dates)>0 and dates[0]==start_date:
        bdays = bdays - 1
    return max(bdays, 0)


class quoteConvertor():
    def __init__(self,fetch_quote,fetch_symbols):
        self.fetch_quote = fetch_quote
        self.fetch_symbols = fetch_symbols

    def stand_inst_code(self,row):
        MarketInformation = ["SHFE","DCE","CZCE","INE","SSE","SZSE","CFFEX","SGE"]
        future_code = row['symbol']
        exchange = row['exchange']
        exchange = [x for x in MarketInformation if row['exchange'] in x][0]
        matchObj = re.search(r'([a-zA-Z]*)([0-9]*)',future_code)
        instr = 'FUT_' + exchange + '_' +  matchObj.group(1).upper() + ":20" + matchObj.group(2)
        return instr

    def covert_to_stand_df(self,quote_table,counterparty_id,strike_ratio,search_date):
        #quote_table.loc[:,'T'] = quote_table['expiry_date'].apply(lambda x:getBdaysToExpiry(search_date,x))
        #print search_date,quote_table.loc[0,'expiry_date']
        #print type(search_date),type(dt.datetime.strftime(quote_table.loc[0,'expiry_date'],'%Y-%m-%d'))
        def date_cal(start_date,end_date_str):
            #print search_date,end_date_str
            end_date = dt.datetime.strptime(end_date_str,'%Y-%m-%d').date()
            #days = (end_date - start_date).days
            days = getBdaysToExpiry(search_date,end_date)
            #print days
            return days

        quote_table.loc[:,'T'] = quote_table['expiry_date'].apply(lambda x:date_cal(search_date,x))
        quote_table['counterparty_id'] = counterparty_id
        quote_table['strike_ratio'] = strike_ratio
        quote_table['quote_date'] = search_date
        quote_table['period_comment'] = quote_table['note']
        quote_table.loc[:,'bid_vol'] = quote_table.apply(lambda x:self.vol_calculator(x['call_bid'],x['put_bid'],x['T'],x['opt_price']),axis = 1)
        quote_table.loc[:,'ask_vol'] = quote_table.apply(lambda x:self.vol_calculator(x['call_ask'],x['put_ask'],x['T'],x['opt_price']),axis = 1)

        quote_table.loc[:,'instrument_name'] = quote_table.apply(lambda x:self.stand_inst_code(x),axis = 1)
        return quote_table

    def vol_calculator(self,call_prc,put_prc,T,underlyingPrc):
        #print call_prc,put_prc,T,underlyingPrc
        mean_prc = (call_prc + put_prc) / 2.0
        if np.isnan(mean_prc):
            return np.nan
        else:
            model_option_type = black.OptionType.CALL
            maturity =  T/244.0
            discount = np.exp(-0.06 * maturity)
            model_impiledVol = black.BlackImpliedCalculator(model_option_type,underlyingPrc,underlyingPrc, maturity,discount)
            imp_vol = model_impiledVol.obtainImpliedVol(mean_prc)
            
            return imp_vol

    def store_to_db(self,conn,quote_table):
        cursor = conn.cursor()
        for idx, row in quote_table.iterrows():
            stmt = (
                "REPLACE INTO marketdata.market_impvol_info " +
                "( updatetime,instrument_name, expiry_date, bid_vol, ask_vol,counterparty_id,period_comment,strike_price_ratio) VALUES " +
                "('{}'".format(dt.datetime.now()) +
                ", '{}'".format(row['instrument_name']) + 
                ", '{}'".format(row['expiry_date'] ) +
                ", {}".format(row['bid_vol']  if not np.isnan(row['bid_vol']) else 'null') +
                ", {}".format(row['ask_vol']   if not np.isnan(row['ask_vol']) else 'null')+
                ", {}".format(row['counterparty_id'] ) +
                ", '{}'".format(row['period_comment'] ) + 
                ", {})".format(row['strike_ratio'] )
            )
            print stmt
            cursor.execute(stmt)
            conn.commit()
        cursor.close()


    def update_vol_info(self,counterparty_id,is_store=False):
        counterparty_id = counterparty_id
        strike_ratio = 1.0
        search_date = dt.datetime.now().date()

        # database prepared
        database = {
        'host': 'instance0.mysql.rds.aliyuncs.com',
        'user': 'longbeach',
        'password': 'L0n9beach',
        }
        conn = mysql.connector.connect(**database)

        ok, symbols = self.fetch_symbols()
        #print symbols
        quote_table = None
        stand_quote_table = None
        if ok:
            quote_table = self.fetch_quote(symbols)
            stand_quote_table = self.covert_to_stand_df(quote_table,counterparty_id,strike_ratio,search_date)
        #print stand_quote_table
        if is_store:
            self.store_to_db(conn,stand_quote_table)
        return stand_quote_table


if __name__ == '__main__':
    '''test'''
    # test calenday
    start_date = dt.datetime.now().date()
    print generate_quote_exp_list( g_days_to_expiry_group,start_date)
    
    # test date
    start_date = dt.date.today()
    print '-1T',getExpiryByShift(start_date,-1,'T',getHolidays())
    print '-2T',getExpiryByShift(start_date,-2,'T',getHolidays())
    print '-3T',getExpiryByShift(start_date,-3,'T',getHolidays())
    print '-4T',getExpiryByShift(start_date,-4,'T',getHolidays())


    print '-1C',getExpiryByShift(start_date,-1,'C',getHolidays())
    print '-2C',getExpiryByShift(start_date,-2,'C',getHolidays())
    print '-3C',getExpiryByShift(start_date,-3,'C',getHolidays())
    print '-4C',getExpiryByShift(start_date,-4,'C',getHolidays())

    print '1T',getExpiryByShift(start_date,1,'T',getHolidays())
    print '2T',getExpiryByShift(start_date,2,'T',getHolidays())
    print '3T',getExpiryByShift(start_date,3,'T',getHolidays())
    print '4T',getExpiryByShift(start_date,4,'T',getHolidays())
    print '5T',getExpiryByShift(start_date,5,'T',getHolidays())
    print dates(start_date)

    print '1C',getExpiryByShift(start_date,1,'C',getHolidays())
    print '2C',getExpiryByShift(start_date,2,'C',getHolidays())
    print '3C',getExpiryByShift(start_date,3,'C',getHolidays())
    print '4C',getExpiryByShift(start_date,4,'C',getHolidays())

    print getBdaysToExpiry(dt.date.today(),dt.date(2019,3,22))
    