import datetime as _dt
def report_detail(account, date=_dt.date.today(), conn='production'):
    '''
    Returns: DataFrame of list of orders with columns like
    ['open_time', 'done_time', 'trade_id', 'instr', 'dir', 'price', 'size']
    '''
    import longbeach.db
    import pandas as pd
    import datetime as dt
    conn=longbeach.db.get_conn(conn)
    query = ('select * from attribution.order_transactions '
             'where type in ("order_issue","transit_to_open","order_done") and date="{1}" '
    #           'and instr like "FUT_SHFE_RU%"'
    #          ' and symid=21005'
             ' and account="{0}"'
    #          ' and ident like "%RU2%"'
             ' ').format(account,date)

    data = pd.read_sql(query,con=longbeach.db.get_conn('production'))
    data.loc[:,'msg_time']=data.msg_time.apply(lambda x: dt.datetime.fromtimestamp(x))    

    aa=data[['msg_time', 'type', 'client_id','order_id','trade_id', 'dir','price','size','instr']]

    iss=aa[aa.type=='order_issue']
    opn=aa[aa.type.isin(['transit_to_open'])]
    cls=aa[aa.type.isin(['order_done'])]

    dd=pd.merge(opn,iss,on=['client_id','order_id'],how='inner')
    ee=dd[['msg_time_x','type_x', 'trade_id_x','instr_y','dir_y', 'price_y', 'size_y']]
    ee.columns = ['open_time','type_x', 'trade_id','instr','dir', 'price', 'size']
    rr = pd.merge(ee,cls[['msg_time','type','trade_id']],on=['trade_id'], how='inner')
    rr = rr[['open_time', 'msg_time', 'trade_id', 'instr', 'dir', 'price', 'size']]
    return rr.rename(columns={'msg_time': 'done_time'})

def report(account, date=_dt.date.today(), conn='production'):
    '''Quoting Times report
    Returns: (bid summary, ask summary) pandas dataframes
    '''
    ff = report_detail(account, date)
    ff.loc[:,'duration'] = ff.loc[:,'done_time'] - ff.loc[:,'open_time']
    buy=ff[ff.dir==0]
    sel=ff[ff.dir==1]
    import numpy as np
    keys = ['open_time','trade_id', 'instr','dir', 'price', 'size', 'duration']
    buy,sell=buy[keys],sel[keys]
    gbuy,gsel=buy.groupby('instr'),sel.groupby('instr')
    
    agg_func = {'sum': np.sum,'mean': lambda x: np.sum(x)/len(x)}
    dfbuy,dfsel=gbuy['duration'].agg(agg_func),gsel['duration'].agg(agg_func)
    dfbuy.index.name=None
    dfsel.index.name=None
    return dfbuy,dfsel
