def get_fills(conn, tids, date):
    import pandas as pd
    import longbeach.db
    if not tids:
        return pd.DataFrame()
    if isinstance(tids, basestring): tids = [tids]
    return pd.read_sql_query(('select * from attribution.fills'
                   ' where tradeid in {} and date="{}"'
                   ''.format(str(tuple([str(t) for t in tids])), date)
                   ), con=longbeach.db.get_conn(conn))

def get_fillrate(conn, ident, date,extra_info=False):
    import longbeach.orderstats
    rpt=longbeach.orderstats.compute_orderstatsv('attribution', ['md_to_order_delay', 'exch_order_confirm_delay'],
                                           {'ident': ident,
                                            'date': date,
                                           },
                                           None, False, verbose=False,
                                      )
    tids = [o['trade_id'] for o in rpt['exch_order_confirm_delay']['data'].itervalues()]
    fills = get_fills(conn, tids, date)
    import numpy as np
    fill_tbl = {tid: fills[fills.tradeid==tid] for tid in tids}
    fill_tbl = {tid: df.set_index(np.arange(0, len(df))) for tid,df in fill_tbl.iteritems()}
#     fill_tbl = {tid: get_fill(tid,date) for tid in orders}
    nfill=len([1 for f in fill_tbl.itervalues() if len(f)>0])
    nord=len(fill_tbl)
    if extra_info:
        return (nfill/float(nord) if nord > 0 else 0),nfill,nord,fill_tbl,rpt
    return nfill/float(nord) if nord>0 else 0,nfill,nord

def details(conn, ident, date):
    fill_rate, nfills, nords, fill_tbl, rpt = get_fillrate(conn, ident, date, extra_info=True)
    filled=[k for k,v in fill_tbl.iteritems() if not v.empty]
    stats=[(o,fill_tbl[o['trade_id']])
           for k,o in rpt['exch_order_confirm_delay']['data'].iteritems() if o['trade_id'] in filled]
    import numpy as np
    return dict(
        fill_rate=fill_rate,
        num_fills=nfills,
        num_orders = nords,
        med_latency_on_fills = (lambda a: np.median(a) if len(a) > 0 else 0 )([o['delay']*1000 for o,f in stats]),
        avg_latency = rpt['exch_order_confirm_delay']['avg']
    ), rpt

def __report( conn, ident, dates ):
    import pandas as pd
    import longbeach.db
    data = [(d,) + details(conn, ident, d) for d in dates ]

    def extract(rpt,k):
        if rpt[k]['count']==0:
            return [0,0,0,0]
        pct=rpt[k]['percentiles']
        d=dict(pct)
        return [rpt[k]['avg'],d[5],d[50],d[95]]

    def combine_pnl(conn, ident,df):
        import longbeach.pnl_daily
        import datetime as dt
        accts=longbeach.db.get_ident_accounts(conn,ident)
        dates=df.date.astype(dt.date).tolist()
        df2 = reduce(lambda a,b: a.append(b), (longbeach.pnl_daily.report(conn, accts, d) for d in dates))
        df2['npc'] = (df2.net/df2.contracts).fillna(0)
        return df2[['net','npc']].join(df.set_index('date')).sort_index(ascending=False)

    df=pd.DataFrame([[d, a['fill_rate'],a['med_latency_on_fills'],]
                     + extract(rpt,'exch_order_confirm_delay')
                     + extract(rpt,'md_to_order_delay')      
                     for d,a,rpt in data],
                    columns=['date', 'fill_rate',
                             'lat*',
                             'lat', 'lat_5', 'lat_50','lat_95',
                             'our', 'our_5', 'our_50','our_95']
    )
    return combine_pnl(conn, ident, df)

def report( conn, tag, dates ):
    '''return array of (ident, result_df) of latency results'''
    import longbeach.db
    
    idents=reduce(lambda a,b: a+b,
                  (longbeach.db.get_account_idents(conn,dates[-1],a)
                   for a in longbeach.db.get_accounts_by_tag(conn, tag)))
    results = [(ident, __report(conn, ident, dates)) for ident in idents]
    return results

def html(results):
    '''turns results from report() into html'''
    from longbeach.html import format_df
    txt=''.join(['<h2>{}</h2>{}'.format(i,format_df(df.set_index(df.index.date))) for i,df in sorted(results)])
    return txt
