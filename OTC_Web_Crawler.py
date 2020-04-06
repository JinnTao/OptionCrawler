#!/usr/bin/env python
# coding=utf-8

from market_quote  import DzOTCQuoteSource
from market_quote import nhOTCQuoteSource # check
from market_quote import htOTCQuoteSource # check
from market_quote import XhOTCQuoteSource
from market_quote import GtjaOTCQuoteSource
from market_quote import LzOTCQuoteSource
from market_quote import BhrsQuoteSource
from market_quote import MyOTCQuoteSource
from market_quote import common
from market_quote import ZsOTCQuoteSource
from market_quote import ShzqOTCQuoteSource
from market_quote import JhOTCQuoteSource
import mysql.connector

import datetime as dt
import pandas as pd
import numpy as np
import openpyxl
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.utils import get_column_letter
from commands import tdate
from longbeach import black

def fresh_otc_quote():
    is_store = True
    # 东征润和
    print '东征润和'
    convertor = common.quoteConvertor(DzOTCQuoteSource.fetch_quote,DzOTCQuoteSource.fetch_symbols)
    convertor.update_vol_info(DzOTCQuoteSource.counterparty_id,is_store)
    #渤海荣盛
    print '渤海荣盛'
    convertor = common.quoteConvertor(BhrsQuoteSource.fetch_quote,BhrsQuoteSource.fetch_symbols)
    convertor.update_vol_info(BhrsQuoteSource.counterparty_id,is_store)
    # 国泰君安
    print '国泰君安'
    convertor = common.quoteConvertor(GtjaOTCQuoteSource.fetch_quote,GtjaOTCQuoteSource.fetch_symbols)
    convertor.update_vol_info(GtjaOTCQuoteSource.counterparty_id,is_store)
    # 华泰期货
    print '华泰期货'
    htOTCQuoteSource.update_vol_info(htOTCQuoteSource.counterparty_id,is_store)
    # 鲁证期货
    print '鲁证期货'
    convertor = commo    # print '济海贸发场外期权实时报价'
    # convertor = common.quoteConvertor(JhOTCQuoteSource.fetch_quote,JhOTCQuoteSource.fetch_symbols)
    # convertor.update_vol_info(JhOTCQuoteSource.counterparty_id,is_store)ource.fetch_quote,LzOTCQuoteSource.fetch_symbols)
    # 南华期货
    #print '南华期货'    # print '济海贸发场外期权实时报价'
    # convertor = common.quoteConvertor(JhOTCQuoteSource.fetch_quote,JhOTCQuoteSource.fetch_symbols)
    # convertor.update_vol_info(JhOTCQuoteSource.counterparty_id,is_store)
#nhOTCQuoteSource.upd    # print '济海贸发场外期权实时报价'
    # convertor = common.quoteConvertor(JhOTCQuoteSource.fetch_quote,JhOTCQuoteSource.fetch_symbols)
    # convertor.update_vol_info(JhOTCQuoteSource.counterparty_id,is_store)e.counterparty_id,is_store)

    # 新湖期货
    #print '新湖期货'    # print '济海贸发场外期权实时报价'
    # convertor = common.quoteConvertor(JhOTCQuoteSource.fetch_quote,JhOTCQuoteSource.fetch_symbols)
    # convertor.update_vol_info(JhOTCQuoteSource.counterparty_id,is_store)
    #convertor = common.quoteConvertor(XhOTCQuoteSource.fetch_quote,XhOTCQuoteSource.fetch_symbols)
    #convertor.update_vol_info(XhOTCQuoteSource.counterparty_id,is_store)
    # 
    #print '茂源私募'
    #convertor = common.quoteConvertor(MyOTCQuoteSource.fetch_quote,MyOTCQuoteSource.fetch_symbols)
    #convertor.update_vol_info(MyOTCQuoteSource.counterparty_id,is_store)


    print '浙商期货'
    convertor = common.quoteConvertor(ZsOTCQuoteSource.fetch_quote,ZsOTCQuoteSource.fetch_symbols)
    convertor.update_vol_info(ZsOTCQuoteSource.counterparty_id,is_store)
    
    # print '济海贸发场外期权实时报价'
    # convertor = common.quoteConvertor(JhOTCQuoteSource.fetch_quote,JhOTCQuoteSource.fetch_symbols)
    # convertor.update_vol_info(JhOTCQuoteSource.counterparty_id,is_store)

    print '上海中期期货'
    convertor = common.quoteConvertor(ShzqOTCQuoteSource.fetch_quote,ShzqOTCQuoteSource.fetch_symbols)
    convertor.update_vol_info(ShzqOTCQuoteSource.counterparty_id,is_store)
    # 合牧场外

    # 最终报价
    print 'Done！'

def getBdaysToExpiry(start_date,end_date):
    '''返回输入两个数据之间的交易日天数'''
    
    dates = tdate(start_date, end_date, mkt='MKT_SHFE', as_python_date=True)   
    bdays = len(dates)
    if len(dates)>0 and dates[0]==start_date:
        bdays = bdays - 1
    return max(bdays, 0)

def getHolidays(conn):
    query = "SELECT * FROM core.holidays"
    holiday_list = pd.read_sql(query, conn)
    holiday_list = holiday_list[holiday_list['code'] == 'S']
    holiday_set = set(holiday_list['day'])
    return holiday_set
def getTradeDayByShift(shft,holidays,shift_start_date,ascending = False):
    #IF exp is not a trading day, then shift to the following trading day.
    exp = shift_start_date + dt.timedelta(days=shft)
    mul = 1
    if ascending == True:
        mul = -1
    while (exp.weekday() == 5 or exp.weekday() == 6 or exp in holidays):
        exp = exp +  mul * dt.timedelta(1)
    return exp

def obtain_vol(vol_info,quote_date,instrument_name,category = 'Month'):
    #print quote_date,instrument_name
    
    c1 = vol_info['trade_date'] == quote_date
    code = instrument_name.split(':')[0].split('_')[2] + instrument_name.split(':')[1][-4:]
    #print code
    if category == 'Month':
        c2 = vol_info['instrument_name'] == instrument_name
    elif category == 'Active':
        code = instrument_name.split(':')[0].split('_')[2] + str('0')
        #print code
        c2 = vol_info['code'] == code
    result = vol_info[c1 & c2]
    #print result
    #print len(result.index)
    if result.empty:
        return np.nan
    else:
        return float((result[result['code']== code]['theo_vol'].values)[0])


def re_order(df,ascending):
    result = df.rank(axis = 1,method='max',ascending=ascending)
    rr = result.apply(pd.value_counts)
    new_order = []
    new_col = zip(rr.loc[1,:],rr.columns.tolist())
    for v in sorted(new_col):
        #print v[0],v[1]
        new_order.append(v[1])
    return df.reindex(columns=new_order)

def calcu_prc(start_date,end_date,vol,spread,risk_spread = np.nan):
    result = np.nan
    days = np.nan
    if not np.isnan(vol):
        optionType = black.OptionType.CALL
        days = getBdaysToExpiry(start_date=start_date,end_date=end_date) 
        T = days / 244.0
        risk_free_rate = 0.06
        discount = np.exp(-risk_free_rate*T)
        stdev = vol * np.sqrt(T)
        price = black.BlackCalc(optionType, 1.0, 1.0, stdev, discount)
        #print stdev,discount,T,risk_free_rate
        result = price.value()
        if not np.isnan(risk_spread) and spread < risk_spread:
            result =  np.nan
    else:
        result = np.nan
    #print start_date,end_date,vol,spread,risk_spread,days,result
    return result
def update_to_excel(periods,vol_info,dfqts,dfcp,today,yesterday):
    dfbid_list = []
    dfask_list = []
    dftheovol_list = []
    dfjudge_list = []


    for T,period in sorted(periods.iteritems()):
        dfqt = dfqts[dfqts['period_comment']==period]
        # Convert id to short name.
        counterparty_id_to_name = {int(r['id']):r['short_name'] for i,r in dfcp.iterrows()}
        dfqt.loc[:,'counterparty']=dfqt.loc[:,'counterparty_id'].apply(lambda x: counterparty_id_to_name[x])
        
        dforder = dfqt.groupby(['instrument_name','counterparty']).sum().unstack(level=1)
        dforder = dforder[['bid_vol','ask_vol']]
        dfbid = dforder['bid_vol']
        dfask = dforder['ask_vol']
        
        dfbid = re_order(dfbid,False)
        dfask = re_order(dfask,False)
    
        
        # write theovol
        dftheory = pd.DataFrame(index=dfbid.index)
        dftheory.reset_index(inplace=True)
        dftheory.loc[:,'T_Vol'] =  dftheory.apply(lambda x : obtain_vol(vol_info,today,x['instrument_name']),axis = 1)
        dftheory.loc[:,'Y_Vol'] =  dftheory.apply(lambda x : obtain_vol(vol_info,yesterday,x['instrument_name']),axis = 1)
        T_nums = (~np.isnan(dftheory.loc[:,'T_Vol'])).sum()
        Y_nums = (~np.isnan(dftheory.loc[:,'Y_Vol'])).sum()
        failVol = None
#         print T_nums
        if T_nums > 0:
            dftheory.loc[:,'fairVol'] = dftheory.loc[:,'T_Vol']
            failVol = dftheory.loc[:,'T_Vol']
        else:
            failVol = dftheory.loc[:,'Y_Vol']
            dftheory.loc[:,'fairVol'] = dftheory.loc[:,'Y_Vol']
        
        dfjudge = pd.DataFrame(index=dfbid.index)
        #dfjudge['quote_date'] = today
        dfjudge.loc[:,'bidSource'] =dfbid.idxmax(axis=1)
        dfjudge.loc[:,'maxbid'] = np.max(dfbid,axis=1)
        dfjudge.loc[:,'minask'] = np.min(dfask,axis=1)
        dfjudge.loc[:,'askSource'] =dfask.idxmin(axis=1)

        dfjudge['Spread']= dfjudge['minask'] - dfjudge['maxbid']
        dfjudge.reset_index(inplace=True)
        dfjudge.loc[:,'Arbitrage'] = dfjudge['Spread'].apply(lambda x : 'True' if x < 0 else 'False')
        dfjudge.loc[:,'T_M_Vol'] =  dfjudge.apply(lambda x : obtain_vol(vol_info,today,x['instrument_name']),axis = 1)
        dfjudge.loc[:,'Y_M_Vol'] =  dfjudge.apply(lambda x : obtain_vol(vol_info,yesterday,x['instrument_name']),axis = 1)

        dfjudge.loc[:,'T_A_Vol'] =  dfjudge.apply(lambda x : obtain_vol(vol_info,today,x['instrument_name'],'Active'),axis = 1)
        dfjudge.loc[:,'Y_A_Vol'] =  dfjudge.apply(lambda x : obtain_vol(vol_info,yesterday,x['instrument_name'],'Active'),axis = 1)

        dfbid_list.append(dfbid)
        dfask_list.append(dfask)
        dftheovol_list.append(dftheory)
        dfjudge_list.append(dfjudge)
    return dfbid_list,dfask_list,dftheovol_list,dfjudge_list
def generator_web_source():

    import mysql.connector
    database = {
        'host': 'instance0.mysql.rds.aliyuncs.com',
        'user': 'longbeach',
        'password': 'L0n9beach',
    }
    conn = mysql.connector.connect(**database) 
    cursor = conn.cursor()
    today = dt.date.today()
    print(today)

    yesterday = getTradeDayByShift(-1,getHolidays(conn),today,ascending=True)
    print yesterday
    stmt = 'SELECT * FROM marketdata.commodity_volinfo where trade_date in(DATE(\'{}\'),DATE(\'{}\')) order by trade_date desc;'.format(yesterday.strftime('%Y-%m-%d'),today.strftime('%Y-%m-%d'))
    vol_info = pd.read_sql(con=conn,sql=stmt)

    print obtain_vol(vol_info,today,'FUT_SHFE_HC:201905')

    query = "SELECT * FROM marketdata.market_impvol_info " + \
        "where quote_date='{}'".format(today.strftime("%Y-%m-%d")) + \
        ";"
    dfqts = pd.read_sql(query, conn)


    query = "SELECT * FROM marketdata.counterparty;"
    dfcp = pd.read_sql(query, conn)

   
    periods = dict(zip(dfqts['expiry_date'].unique().tolist(),dfqts['period_comment'].unique().tolist()))
    print (periods)
    print dfqts['expiry_date'].unique().tolist()

    return update_to_excel(periods,vol_info,dfqts,dfcp,today,yesterday)

    
if __name__ == "__main__":

    #DzOTCQuoteSource.update_vol_info(12)
    #nhOTCQuoteSource.update_vol_info(1)
    #htOTCQuoteSource.update_vol_info(2)
    #XhOTCQuoteSource.update_vol_info(5)
    #GtjaOTCQuoteSource.update_vol_info(4)
    #LzOTCQuoteSource.update_vol_info(7)
    #BhrsQuoteSource.update_vol_info(14)

    #print '茂源私募'
    #convertor = common.quoteConvertor(MyOTCQuoteSource.fetch_quote,MyOTCQuoteSource.fetch_symbols)
    #print convertor.update_vol_info(MyOTCQuoteSource.counterparty_id,False )
    #convertor.update_vol_info(MyOTCQuoteSource.counterparty_id,True)
    # 合牧场外
    # First step
    fresh_otc_quote()
    # Sencond step
    #print generator_web_source()

    # import time
    # import threading

    # while True:

    #     rnd = pd.date_range(start=dt.datetime(dt.date.today().year,
    #                     dt.date.today().month,
    #                     dt.date.today().day,
    #                     9,0,0),periods=8,freq='1H')
    #     nowTime = dt.datetime.now().replace(microsecond=0)
    #     print rnd
    #     if rnd.contains(nowTime):
    #         try:
    #             fresh_otc_quote()
    #         except:
    #             time.sleep(60*5)
    #             continue
    #     else:
    #         print nowTime
    #     time.sleep(1)
