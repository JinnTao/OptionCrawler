#!/usr/bin/env python
# coding=utf-8
import mysql.connector
import numpy as np
import pandas as pd
import requests
import datetime as dt
import json
import grequests
import re
import time

def exeTime(func):
    def newFunc(*args, **args2):
        #print args,args2
        t0 = time.time()
        print "@%s, {%s} start" % (time.strftime("%X", time.localtime()), func.__name__)
        back = func(*args, **args2)
        print "@%s, {%s} end" % (time.strftime("%X", time.localtime()), func.__name__)
        print "@%.3fs taken for {%s}" % (time.time() - t0, func.__name__)
        return back
    return newFunc
# --end of exeTime

class commdityVolFresher(object):
    @exeTime
    def __init__(self,split_date,is_store = False,no_fresh_list = []):
        self.split_date = split_date
        self.is_store = is_store
        self.no_fresh_list = no_fresh_list
        
        database = {
        'host': 'instance0.mysql.rds.aliyuncs.com',
        'user': 'longbeach',
        'password': 'L0n9beach',
        }
        self.conn = mysql.connector.connect(**database)
        sql = 'SELECT * FROM core.commodity_future_details where product = \'FUT\'\
        and market in (\'CFFEX\',\'CZCE\',\'DCE\',\'INE\',\'SHFE\')order by market;'
        contract_map = pd.read_sql(sql,self.conn)
  
        contract_map_quote = contract_map[contract_map['instrument_code'].apply(lambda x : x not  in no_fresh_list)]
        self.contract_map_quote = contract_map_quote

    def obtain_code_list(self,start = 15,step_len = 5):
        inst_month = []
        for i in range(step_len):
            for j in range(12):
                instr_mon = str(start) + str(j+1).zfill(2)
                inst_month.append(instr_mon)
            start = start + 1

        month_list = pd.DataFrame(columns=['Exchange','code'])
        active_list = pd.DataFrame(columns=['Exchange','code'])

        for idx,row in self.contract_map_quote.iterrows():
            active_list = active_list.append(pd.DataFrame(data=[[row['market'],row['instrument_code'] + '0']],\
                                            columns=['Exchange','code']),ignore_index=True)
            for i in inst_month:
                month_list = month_list.append(pd.DataFrame(data=[[row['market'],row['instrument_code'] + i]],\
                                            columns=['Exchange','code']),ignore_index=True)
        return active_list,month_list

    def on_error(self,request, exception):
        print "request failed [request] %s [error] %s" % (request, exception)
        pass


    def obtain_history_data(self,code_list):
        columns_r = ['trade_date','open','high','low','close','volume']
        history_database = []
        urls = []
        for idx,row in code_list.iterrows():
            exchange = row['Exchange']
            future_code = row['code']
            if(exchange == 'CFFEX'):
                url_str = ('http://stock2.finance.sina.com.cn/futures/api/json.php/CffexFuturesService.getCffexFuturesDailyKLine?symbol='+future_code )
                #print url_str

            else:
            #    continue
                url_str = ('http://stock2.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesDailyKLine?symbol=' +future_code)
            urls.append(url_str)
        http_requests = (grequests.get(url) for url in urls)
        http_responses = grequests.map(http_requests,
                                    size=18,
                                    exception_handler=self.on_error)
        for i,r in enumerate(http_responses):
            future_code = code_list.loc[i,'code']
            exchange = code_list.loc[i,'Exchange']
            matchObj = re.search(r'([a-zA-Z]*)([0-9]*)',future_code)
            instr = 'FUT_' + exchange + '_' +  matchObj.group(1).upper() + ":20" + matchObj.group(2)
            if r is not None and r.ok:
                data_json = r.json()
                #print instr,future_code,exchange
                if data_json is not None:
                    data_lists = list(data_json)
                    frame = pd.DataFrame(data_lists,index = range(0,len(data_lists)),columns = columns_r)
                    frame['trade_date']= np.array([dt.datetime.strptime(i,'%Y-%m-%d').date() for i in frame['trade_date']])
                    frame['instrument_name'] = instr
                    frame['code'] = future_code
                    frame['category'] = instr.split(':')[0]
                    frame['open'] = np.array([  float(i)  if i is not None else i for i in frame['open']])
                    frame['high'] = np.array([  float(i)  if i is not None else i for i in frame['high']])
                    frame['low'] = np.array([  float(i)  if i is not None else i for i in frame['low']])
                    frame['close'] = np.array([  float(i)  if i is not None else i for i in frame['close']])
                    frame['volume'] = np.array([float(i) for i in frame['volume']])
                    if len(future_code) > 4:
                        frame.sort_values(by=['trade_date'],ascending=True,inplace =True)
                        #print frame['close'] ,frame['close'] 
                        frame.loc[:,"close_prev"] = frame['close'].shift(1)
                        frame.loc[:,'logrt'] = np.log(frame['close'] / frame['close_prev'])
                        logrt = np.array(frame['logrt'])
                        c2c_5 = np.zeros(len(logrt))
                        c2c_10 = np.zeros(len(logrt))
                        c2c_21 = np.zeros(len(logrt))
                        mean_21 = np.zeros(len(logrt))
                        std_21 = np.zeros(len(logrt))
                        mutipler = np.sqrt(244.0)

                        N = 21
                        for i in range(len(logrt)):
                            if(i < N-1):
                                c2c_5[i] = np.nan
                                c2c_10[i] = np.nan
                                c2c_21[i] = np.nan
                            else:
                                c2c_5[i] =  np.std(logrt[(i-5+1):i+1],ddof = 1)*mutipler
                                c2c_10[i] =  np.std(logrt[(i-10+1):i+1],ddof = 1)*mutipler
                                c2c_21[i] =  np.std(logrt[(i-21+1):i+1],ddof = 1)*mutipler
                                if(i > N + 21 - 1):
                                    std_21[i] = np.std(c2c_21[(i-21+1):i+1],ddof = 1)
                                    mean_21[i] = np.mean(c2c_21[(i-21+1):i+1])
                        frame['theo_vol'] = c2c_21

                        frame['theo_vol_5'] = c2c_5
                        frame['theo_vol_10'] = c2c_10
                        frame['theo_vol_21'] = c2c_21
                        frame['theo_vol_42'] = np.zeros(len(logrt))
                        frame['theo_vol_63'] = np.zeros(len(logrt))
                        frame['vol_21_mean'] = mean_21
                        frame['vol_21_std'] = std_21
                    else:
                    
                        frame['close_prev'] = np.nan
                        #frame['volume'] = np.array([float(i) for i in frame['volume']])
                        frame['theo_vol'] = np.nan
                        frame['theo_vol_5'] = np.nan
                        frame['theo_vol_10'] = np.nan
                        frame['theo_vol_21'] = np.nan
                        frame['theo_vol_42'] = np.nan
                        frame['theo_vol_63'] = np.nan
                        frame['vol_21_mean'] = np.nan
                        frame['vol_21_std'] = np.nan
                    history_database.append(frame)
        data = pd.concat(history_database,ignore_index=True)
        return data



    @exeTime
    def fresh(self,start = 15,step = 5):
        active_list,month_list = self.obtain_code_list(start=start,step_len=step)
        #print active_list,month_list
        
        month_data = self.obtain_history_data(month_list)
        
        if self.is_store:
            self.store_to_db(month_data[month_data['trade_date'] >= self.split_date])

        active_data = self.obtain_history_data(active_list)
        active_data_continue = pd.merge(left=active_data,right=month_data,on=['trade_date','open','high','low','close','volume','category'],suffixes=['_m',''])
        active_data_continue_st = active_data_continue.groupby(by=['code_m']).apply(lambda x : self.generate_vol_info(x))
        active_data_continue_st= active_data_continue_st.loc[:,['trade_date','open','high','low','close','volume','instrument_name','code_m',
                                    'close_prev','logrt','theo_vol','theo_vol_5','theo_vol_10','theo_vol_21',
                                    'theo_vol_42','theo_vol_63','vol_21_mean','vol_21_std','category']]
        active_data_continue_st.loc[:,'code'] = active_data_continue_st.loc[:,'code_m']
        active_data_continue_st = active_data_continue_st[active_data_continue_st['trade_date'] >= self.split_date]

        if self.is_store:
            print 'store to db',active_data_continue_st['instrument_name'].unique().tolist()
            self.store_to_db(active_data_continue_st)
            

    def generate_vol_info(self,x):
        logrt = np.array(x['logrt'])
        c2c_5 = np.zeros(len(logrt))
        c2c_10 = np.zeros(len(logrt))
        c2c_21 = np.zeros(len(logrt))
        c2c_42 = np.zeros(len(logrt))
        c2c_63 = np.zeros(len(logrt))
        mean_21 = np.zeros(len(logrt))
        std_21 = np.zeros(len(logrt))
        mutipler = np.sqrt(244.0)

        N = 21
        for i in range(len(logrt)):
            if(i < N-1):
                c2c_5[i] = np.nan
                c2c_10[i] = np.nan
                c2c_21[i] = np.nan
            else:
                c2c_5[i] =  np.std(logrt[(i-5+1):i+1],ddof = 1)*mutipler
                c2c_10[i] =  np.std(logrt[(i-10+1):i+1],ddof = 1)*mutipler
                c2c_21[i] =  np.std(logrt[(i-21+1):i+1],ddof = 1)*mutipler
                if(i > N + 21 - 1):
                    std_21[i] = np.std(c2c_21[(i-21+1):i+1],ddof = 1)
                    mean_21[i] = np.mean(c2c_21[(i-21+1):i+1])
                if(i > 63):
                    c2c_42[i] =  np.std(logrt[(i-42+1):i+1],ddof = 1)*mutipler
                    c2c_63[i] =  np.std(logrt[(i-63+1):i+1],ddof = 1)*mutipler
        x['theo_vol'] = c2c_21

        x['theo_vol_5'] = c2c_5
        x['theo_vol_10'] = c2c_10
        x['theo_vol_21'] = c2c_21
        x['theo_vol_42'] = c2c_42
        x['theo_vol_63'] = c2c_63
        x['vol_21_mean'] = mean_21
        x['vol_21_std'] = std_21
        return x
    def store_to_db(self,df):
        cursor = self.conn.cursor()
        for idx, row in df.iterrows():
            stmt = (
                "REPLACE INTO marketdata.commodity_volinfo " +
                "(trade_date, instrument_name, open, high, low, close, close_prev, volume, theo_vol,\
        theo_vol_5,theo_vol_10,theo_vol_21,theo_vol_42,theo_vol_63,logrt,vol_21_mean,vol_21_std,code,category) VALUES " +
                "( '{}'".format(row['trade_date']) +
                ", '{}'".format(row['instrument_name']) + 
                ", {}".format(row['open'] ) +
                ", {}".format(row['high']  ) +
                ", {}".format(row['low'] ) +
                ", {}".format(row['close'] ) +
                ", {}".format(row['close_prev'] if not np.isnan(row['close_prev']) else 'null') +
                ", {}".format(row['volume'] ) +
                ", {}".format(row['theo_vol'] if not np.isnan(row['theo_vol']) else 'null')  +
                ", {}".format(row['theo_vol_5'] if not np.isnan(row['theo_vol_5']) else 'null')  +
                ", {}".format(row['theo_vol_10'] if not np.isnan(row['theo_vol_10']) else 'null')  +
                ", {}".format(row['theo_vol_21'] if not np.isnan(row['theo_vol_21']) else 'null') +
                ", {}".format(row['theo_vol_42'] if not np.isnan(row['theo_vol_42']) else 'null') +
                ", {}".format(row['theo_vol_63'] if not np.isnan(row['theo_vol_63']) else 'null') +
                ", {}".format(row['logrt'] if not np.isnan(row['logrt']) and not np.isinf(row['logrt']) else 'null') +
                ", {}".format(row['vol_21_mean'] if not np.isnan(row['vol_21_mean']) else 'null') +
                ", {}".format(row['vol_21_std'] if not np.isnan(row['vol_21_std']) else 'null') + 
                ", '{}'".format(row['code']) + 
                ", '{}');".format(row['category'])
            )
            cursor.execute(stmt)
            self.conn.commit()
        cursor.close()
        
if __name__ == '__main__':

    # 不取价的品种 没有流动性同时不适用商品 -- JinnTao 2018-12-26
    not_quote_list = ['T','TF','WR','BB','FB','B','ME']
    
    # 只更新从split_date开始的数据
    #save_date = dt.date(2019,2,15)
    save_date = dt.date.today()
    fresher = commdityVolFresher(  split_date=save_date,is_store=True,no_fresh_list=not_quote_list)
    
    # 从15年开始，刷新到20年所有品种合约数据，并生成连续主力合约数据
    fresher.fresh(start = 18, step = 3)

