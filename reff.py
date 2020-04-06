#!/usr/bin/env python
# coding=utf-8

import datetime

from db import aliyun_rds_conn


def parse_instrument_list_to_db(file_path):
    conn = aliyun_rds_conn()
    cursor = conn.cursor()

    for line in open(file_path):
        sql = '''
                    INSERT INTO marketdata.instrument_list 
                    (date, text) VALUES 
                    ('%s', '%s') 
                    ''' % (datetime.date.today(), line)
        cursor.execute(sql)

    conn.commit()
    cursor.close()


def instrument_list(trading_date):
    conn = aliyun_rds_conn()
    cursor = conn.cursor()

    sql = '''
        select text from marketdata.instrument_list where date = '%s'
        ''' % (trading_date)
    cursor.execute(sql)

    return [text[0] for text in cursor]


def parse_option_info_to_db(file_path):
    conn = aliyun_rds_conn()
    cursor = conn.cursor()

    for line in open(file_path):
        if 'OPT' in line:
            exchange, securityid, contractid, _ = line.split(',')
            exec_price = float(contractid[-4:]) / 100
            sql = '''
            INSERT INTO marketdata.option_info 
            (date, securityid, contractid, exec_price) VALUES 
            ('%s', '%s', '%s', %s) 
            ''' % (datetime.date.today(), securityid, contractid, exec_price)

            #print sql

            cursor.execute(sql)

    conn.commit()
    cursor.close()


def option_info(trading_date):
    conn = aliyun_rds_conn()
    cursor = conn.cursor()

    sql='''
    select * from marketdata.option_info where date = '%s'
    ''' % (trading_date)
    cursor.execute(sql)

    for _, securityid, contractid, exec_price in cursor:
        print securityid, contractid, exec_price


if __name__ == "__main__":
    # parse_option_info_to_db('/home/weizeguo/lbpy/instrument.list')
    # option_info(datetime.date.today())
    #parse_instrument_list_to_db('/home/weizeguo/lbpy/instrument.list')
    insts = instrument_list(datetime.date.today())
    print insts

    with open('./test_list', 'w') as fd:
        fd.writelines(insts)