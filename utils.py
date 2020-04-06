import re
import datetime as dt

import pandas as pd

from contextlib import contextmanager
import db
from common import getExpiryByShift, getHolidays

#################################################################
# Utilities
@contextmanager
def pushd(newDir):
    import os
    previousDir = os.getcwd()
    os.chdir(newDir)
    try:
        yield
    finally:
        os.chdir(previousDir)

def mkdir_p(path):
    import os
    import os.path
    try:
        if not os.path.isdir(path):
            os.makedirs(path)
    except OSError:
        if not os.path.isdir(path):
            raise
mkdir_f = mkdir_p

# computes weeknum with offset days, 0 is monday
def weeknum( d, offset = 0 ):
    ''' 
    Returns the weeknum that 'd' falls in, offset is the
    the starting weekday that begins the week, so the 2nd Monday is weeknum 2
    if offset is 0 (0 is Monday, 1 is Tues, etc)
    '''
    from datetime import timedelta
    from datetime import datetime
    from datetime import date
    
    s = date( d.year, d.month, 1 )
    adj = (s.weekday() + offset + 1) % 7
    diffdays = d - s
    return ((diffdays.days + adj)/7) + 1

def date_range(sdate,edate):
    '''produces a list of dates between sdate and edate inclusive'''
    import pandas as pd
    from pandas.tseries.offsets import BDay
    return [d.to_datetime().date() for d in pd.date_range(sdate,edate,freq=BDay())]

class switch(object):
    '''helper for doing a switch-like statement in python'''
    def __init__(self, value):
        self.value = value
        self.fall = False

    def __iter__(self):
        """Return the match method once, then stop"""
        yield self.match
        raise StopIteration
    
    def match(self, args):
        """Indicate whether or not to enter a case suite"""
        if self.fall or not args:
            return True
        elif self.value in args:
            self.fall = True
            return True
        else:
            return False

class dotdict(dict):
     """dot.notation access to dictionary attributes"""
     def __getattr__(self, attr):
         rv=self.get(attr)
         return dotdict(rv) if isinstance(rv,dict) else rv
     def __getstate__(self): return self.__dict__
     def __setstate__(self, d): self.__dict__.update(d)
     __setattr__= dict.__setitem__
     __delattr__= dict.__delitem__
     __dir__= dict.keys

import concurrent.futures
class ProcessPoolExecutor(concurrent.futures.ThreadPoolExecutor):
    """concurrent.futures compatible Executor for multiprocess-based process pool
    Gracefully handles lambda expressions for passing to process pools.  Basically
    backed by a ThreadPool where each thread handles a single spawned process
    """
    def submit(self, f, *args):
        def process_proxy(f_, *args0):
            import multiprocess as mp
            def f1(f,q,*args):
                try:
                    r=f(*args)
                    q.put(r)
                    q.close()
                except Exception as e:
                    import traceback
                    q.put((e,traceback.format_exc()))
            q=mp.Queue()
            p=mp.Process(target=f1, args=(f_,q)+args0)
            p.start()
            rv=q.get()
            q.close()
            p.terminate()
            p.join()
            q.join_thread()
            return rv
        return super(ProcessPoolExecutor,self).submit(process_proxy, f, *args)



symbol_re = re.compile('([a-z A-Z]+)(\d+)')
def replace_expiry_month(symbol):
    m = symbol_re.search(symbol)
    if not m:
        print 'error symbol format! [symbol] %s' % symbol
        assert False

    product, expiry_month = m.groups()
    if expiry_month[0] == '9':
        expiry_month = '1' + expiry_month
    elif expiry_month[0] == '0':
        expiry_month = '2' + expiry_month

    return product + expiry_month


def last_settlement_date():
    curr_day = dt.date.today()

    curr_time = dt.datetime.now()
    if curr_time.hour > 15:
        return curr_day

    return getExpiryByShift(curr_day, -1, 'T', getHolidays())

_close_price_df = pd.DataFrame()
def close_price(symbol, last_settlement_date):
    global _close_price_df
    if _close_price_df.empty:
        sql = '''
                select trade_date, code as symbol, close
                from marketdata.commodity_volinfo
                where trade_date = '%s'
                  and length(code) > 3
                ''' % (last_settlement_date)
        print sql
        _close_price_df = pd.read_sql(sql, con=db.aliyun_rds_conn())

    close = _close_price_df[_close_price_df['symbol'] == symbol.upper()]
    #print close
    if close.empty:
        return None
    else:
        return close['close'].values[0]
