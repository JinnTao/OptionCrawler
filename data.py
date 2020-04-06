def load( filename ):
    import pandas as pd
    import datetime as dt
    d1 = pd.read_table( filename, sep=' ', header=None )
    d1['tv'] = (pd.to_datetime(d1.iloc[:,0], unit='s', utc=False) + dt.timedelta(hours=8))
    del d1[0]
    d1.set_index( ['tv'], inplace=True )
    return d1

def load_csv( filename, header=0, timecol=0 ):
    import pandas as pd
    import datetime as dt
    d1 = pd.read_table( filename, sep=',', header=header )
    d1.iloc[:,timecol] = d1.iloc[:,timecol].apply(lambda t: dt.datetime.fromtimestamp(t))
    d1.set_index( [timecol], inplace=True )
    return d1

def load_book( filename ):
    import datetime as dt
    import re
    
    y = [ dict([i.split(':') for i in l.split(' ') if len(i.split(':'))==2])
          for l in open(filename)]

    def parse(i):
        d = dict( time=dt.datetime.fromtimestamp(float(i['time'])),
              mid_px=float(i['mid_px']),
              ask_sz=int(i['ask_sz']),
              ask_px=float(i['ask_px']),
              bid_px=float(i['bid_px']),
              bid_sz=int(i['bid_sz']),
              )
        for n_ in range(5):
            n = n_+1
            if str(n) in i:
                b = re.split('@|x', i[str(n)])
                d["bid_px" + str(n)] = float(b[0])
                d["bid_sz" + str(n)] = int(b[1])
                d["ask_px" + str(n)] = float(b[2])
                d["ask_sz" + str(n)] = int(b[3])
        return d
    z = [ parse(i) for i in y ]
    return z

def load_signalinfo( filename, timecols=['entry_tm','exit_tm'] ):
    import pandas as pd
    import datetime as dt
    d1 = pd.read_table( filename, sep=',',
                        index_col = 'entry_tm', 
                        parse_dates = timecols, 
                        date_parser = lambda x: dt.datetime.fromtimestamp(float(x)), 
                        header=0,
    )
    d1.rename( columns={d1.columns[0]:'id'}, inplace=True )
    return d1

def __format_timevals( data ):
    '''recursively format time values on a json object converted from a bson log'''
    import datetime as dt
    if type(data) == type([]):
        for d in data: __format_timevals( d )
    elif type(data) == type({}):
        ts_keys = { u'ts', u'send_time', u'lm', u'llm', u'exch_time',
                    u'startup_tv', u'time', u'recv_time', u'open_time',
                    u'mean_fill_time', }
        match_keys = set(data.keys()) & ts_keys
        for k in match_keys:
            if type(data[k]) != dt.datetime:
                data[k] = dt.datetime.fromtimestamp(data[k]/1000000.0)
        for k in data: __format_timevals( data[k] )

def load_ordertracker( filename, post=None ):
    '''load BSON ordertracker file, pre-converts some known timestamps, depends on b2json'''
    import subprocess
    import json
    p = subprocess.Popen(['b2json', filename], stdout=subprocess.PIPE)
    data = [json.loads(s.replace('-nan','NaN')) for s in p.stdout]
    if post: post( data )
    return data

def load_ordertracker_gen( filename, post=None, convert_tv=False ):
    '''load BSON ordertracker file, pre-converts some known timestamps, depends on b2json'''
    import subprocess
    import json
    p = subprocess.Popen(['b2json', filename], stdout=subprocess.PIPE)
    post_process = lambda e: e
    if convert_tv:
        def proc(e):
            __format_timevals(e.values()[0])
            return e
        post_process = proc
    return (proc(json.loads(s.replace('-nan','NaN'))) for s in p.stdout)

def bson_stream(fname):
    import subprocess
    import json
    p = subprocess.Popen(['b2json', fname], stdout=subprocess.PIPE)
    return (json.loads(s.replace('-nan','NaN')) for s in p.stdout)

def fills_from_bson(fname):
    import subprocess
    import json
    p = subprocess.Popen(['b2json', fname], stdout=subprocess.PIPE)
    return (j for j in bson_stream(fname) if j.keys()[0]=='fill')

def __fmt_tid( jobj ):
    return "{0}#{1}".format( jobj['account'], jobj['id'] )

def collect_stats_from_bson_files( bson_files ):
    '''
    reads in a list of bson files, produces a structure as:
      { account : { date : { 'df' : DataFrame, ... }, ... }, ... }

    each DataFrame contains the following columns:
      'tid',
      'msg_send_time'
      'msg_recv_time'
      'order_send_time'
      'order_transit_time'
      'order_confirm_recv_time'
      'order_confirm_time'
      'cancel_send_time'
      'cancel_transit_recv_time'
      'cancel_transit_time'
      'cancel_done_time'

    'tid' is a unique identifier serving as index, it's the uniquely assigned trade id for each order.
    Note that not all cells have valid values, some cells may only contain a 'None' value as each order's
    lifecycle is different.

    Known Issues:
      * Onlys works per bson file context, so order id in each bson must be unique, otherwise stats may be off the chart.
      * Cannot look across different bson files to match up order id, there are cases that traders may be restarted
        multiple times intra-day, so its orders may be scattered across different bson files. We can't handle that for now.
    '''
    import datetime as dt
    import longbeach.db
    import pandas as pd
    datamap = {}
    dbconn = longbeach.db.get_conn( "production" )

    for file_name in bson_files:
        json_data = load_ordertracker( file_name )
        order_time_sequence = {}
        account_name = None
        curdate = None

        for bmsg in json_data:
            if u'startup' == bmsg.keys()[0]:
                account_name = bmsg['startup']['account']
                curdate =  dt.datetime.fromtimestamp(bmsg['startup']['startup_tv']/1000000.0).strftime( "%Y%m%d" )
                if account_name not in datamap:
                    datamap[account_name] = {}
                if curdate not in datamap[account_name]:
                    datamap[account_name][curdate] = { 'pnl': 0, }
            elif u'fill' == bmsg.keys()[0]:
                acct = bmsg['fill']['account']
                curd =  dt.datetime.fromtimestamp(bmsg['fill']['ts']/1000000.0).strftime( "%Y%m%d" )
                instr = bmsg['fill']['instr']
                mult = longbeach.db.get_multiplier( dbconn, instr, curdate )

                datamap[acct][curd]['pnl'] -= ( float(mult) * float(bmsg['fill']['fill_sz']) * float(bmsg['fill']['fill_px']) )
                datamap[acct][curd]['pnl'] -= ( float(bmsg['fill']['fees']['reg']) + float(bmsg['fill']['fees']['exch']) + float(bmsg['fill']['fees']['clear']) )
            elif u'issue' == bmsg.keys()[0]:
                oid = bmsg['issue']['oid']
                if oid in order_time_sequence:
                    # this happens when there are two 'issue' messages right next to each other with exactly
                    # the same order id within the same bson file. this is super-bad, I have no way to distinguish
                    # those two orders at all within the bson file context.
                    continue
                order_time_sequence[oid] = {
                    'msg_send_time': bmsg['issue']['msg']['header']['time'],
                    'msg_recv_time': bmsg['issue']['msg']['recv_time'],
                    'order_send_time': bmsg['issue']['send_time'],
                }
            elif u'cancel' == bmsg.keys()[0]:
                oid = bmsg['cancel']['oid']
                if oid not in order_time_sequence:
                    continue
                order_time_sequence[oid].update(
                    {
                        'cancel_send_time': bmsg['cancel']['send_time'],
                    }
                )
            elif u'done' == bmsg.keys()[0]:
                oid = bmsg['done']['oid']
                if oid not in order_time_sequence:
                    continue
                tid = __fmt_tid( bmsg['done']['tradeid'] )
                if tid not in order_time_sequence:
                    order_time_sequence[tid] = order_time_sequence[oid]
                    del order_time_sequence[oid]
                if bmsg['done']['cxlsts2'] == u'CXLSTATUS_CONFIRMED':
                    order_time_sequence[tid].update(
                        {
                            'cancel_done_time': bmsg['done']['ts'],
                        }
                    )
            elif u'statchange' == bmsg.keys()[0]:
                oid = bmsg['statchange']['oid']
                if oid not in order_time_sequence:
                    continue
                tid = __fmt_tid( bmsg['statchange']['tradeid'] )
                if tid not in order_time_sequence:
                    order_time_sequence[tid] = order_time_sequence[oid]
                if bmsg['statchange']['cxlsts2'] == u'CXLSTATUS_TRANSIT':
                    order_time_sequence[tid].update(
                        {
                            'cancel_transit_recv_time': bmsg['statchange']['ts'],
                            'cancel_transit_time': bmsg['statchange']['open_time'],
                        }
                    )
                elif bmsg['statchange']['lsts2'] == u'STAT_NEW' and bmsg['statchange']['sts2'] == 'STAT_TRANSIT':
                    order_time_sequence[tid].update(
                        {
                            'order_transit_time': bmsg['statchange']['ts'],
                        }
                    )
                elif bmsg['statchange']['lsts2'] == u'STAT_TRANSIT' and bmsg['statchange']['sts2'] == 'STAT_OPEN':
                    order_time_sequence[tid].update(
                        {
                            'order_confirm_recv_time': bmsg['statchange']['ts'],
                            'order_confirm_time': bmsg['statchange']['open_time'],
                        }
                    )

        df = pd.DataFrame(columns=['msg_send_time', 'msg_recv_time',
                                   'order_send_time', 'order_transit_time', 'order_confirm_recv_time', 'order_confirm_time',
                                   'cancel_send_time', 'cancel_transit_recv_time', 'cancel_transit_time', 'cancel_done_time',
                                  ])

        for i in order_time_sequence:
            if type(i) == type(1):
                # ignore numeric oid stats
                continue
            for k in order_time_sequence[i]:
                df.loc[i, k] = dt.datetime.fromtimestamp(order_time_sequence[i][k]/1000000.0) if order_time_sequence[i][k] else None

        if account_name and curdate:
            datamap[account_name][curdate]['df'] = df

    return datamap
