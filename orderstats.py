#!/usr/bin/python

import mysql.connector
from datetime import date,datetime
 
class MySQLCursorDict(mysql.connector.cursor.MySQLCursor):
    def fetchone(self):
        row = self._fetch_row()
        if row:
            return dict(zip(self.column_names, self._row_to_python(row)))
        return None


def compute_orderstats_tags( dbprofile, conditions, start_tag, end_tag,
                             query=None, simulate=False, verbose=False ):
    import longbeach.db
    conn = longbeach.db.get_conn(dbprofile)

    cursor = conn.cursor(cursor_class=MySQLCursorDict)
    filters = []
    if query:
        filters.append(query)

    for k,v in conditions.items():
        if isinstance( v, list ):
            filters.append( "{0} {1} '{2}'".format( k, v[0], v[1] ) )
        else:
            filters.append( "{0}='{1}'".format( k, str(v) ) )
    query=("select * from order_transactions "
           "where "
           "{filter}"
           "(type='{s_tag}' or type='{e_tag}') ").format(
               filter = (' and '.join(filters) + ' and ') if len(filters) > 0 else '',
               s_tag = start_tag,
               e_tag = end_tag
           )
    if simulate:
        print(query)
        return None

    cursor.execute(query)

    from collections import defaultdict

    times = defaultdict(dict)
    for r in cursor:
        key = '{date}_{ident}.{client_id}.{order_id}'.format(**r)
        time = r['msg_time']
        entry = times[key]
        entry['start_time'] = min( time, entry['start_time'] if 'start_time' in entry else time )
        entry['end_time'] = min( time, entry['end_time'] if 'end_time' in entry else time )
        if 'trade_id' in r:
            entry['trade_id'] = r['trade_id']
        times[key][r['type']] = time

    conn.close()

    valid_times = {}
    for k,v in sorted(times.items()):
        if start_tag in v and end_tag in v:
          diff = v[end_tag] - v[start_tag]
          v['delay'] = diff
          valid_times[k] = v
          if verbose:
              t = datetime.fromtimestamp(v['start_time'])
              print( "{ts} {key} {tid} {diff:.3f}".format(
                  ts=t, key=k, tid=v['trade_id'], diff=diff*1000
              ))

    import numpy
    data = [ v['delay'] for k,v in valid_times.items() ]
    m = numpy.mean(data) if len(data) > 0 else 0
    pct = [0, 5, 25, 50, 75, 95, 100]
    p = numpy.percentile( data, pct ) if len(data) > 0 else []
    result = dict(
        avg = round(m*1000,3),
        median = p[3]*1000 if len(data) > 0 else 0,
        count = len(data),
        percentiles = zip(pct,[round(1000*i,3) for i in p]),
        data = valid_times,
    )
    return result

delays_table = dict(
    exch_order_confirm_delay = ('new_to_transit', 'transit_to_open')
    exch_cancel_delay = ('cancel_transit', 'order_done'),
    td_cancel_delay = ('cancel_issue', 'cancel_transit'),
    td_send_delay = ('order_issue', 'new_to_transit'),
    md_to_order_delay = ('client_order_issue', 'order_issue'),
    md_to_cancel_transit_delay = ('client_cancel_issue', 'cancel_transit'),
    md_to_cancel_delay = ('client_cancel_issue', 'cancel_issue'),
)

for k in delays_table.keys():
    globals()[k.upper()]=k

def compute_orderstats( dbprofile,
                        delay_tag,
                        conditions,
                        query = None,
                        simulate = False,
                        verbose = False,
                        ):
    tags = delays_table[delay_tag]
    return compute_orderstats_tags( dbprofile, conditions,
                                    start_tag = tags[0],
                                    end_tag = tags[1],
                                    query = query,
                                    simulate = simulate,
                                    verbose = verbose,
                                )
                        
def compute_orderstatsv( dbprofile, tags_list, conditions, query, simulate, verbose ):
    # this can be made made more efficient
    results = {}
    for i in tags_list:
        r = compute_orderstats( dbprofile, i, conditions, query, simulate, verbose )
        results[i] = r
    import longbeach.table
    t = longbeach.table.Table()
    for k,v in results.items():
        t.set( k, 'avg', v['avg'] )
        t.set( k, 'count', v['count'] )
        t.set_row( k, dict(v['percentiles']) )
    c = t.cols
    c.remove('avg')
    c.remove('count')
    t.cols = ['avg','count'] + sorted(t.cols)

    if verbose:
        print(t.str())
    return results

def run( profile, conditions, args ):
    result = compute_orderstatsv(
        dbprofile,
        delays_table.keys(),
        conditions,
        args.query,
        args.dryrun,
        args.verbose,
    )
    result = compute_orderstats(
        dbprofile,
        args.delay_type,
        conditions,
        args.query,
        args.dryrun,
    )
    if not args.dryrun:
#        m = result['avg']
#        print( '{0} avg: {1:.3f} med: {3:.3f} cnt: {2}'.format(
#            args.delay_type, m, result['count'], result['median'] ) )
#        print(str(result['percentiles']))
        from longbeach.table import Table
        t = Table()
        t.set( args.delay_type, 'avg', result['avg'] )
        t.set_row( args.delay_type, dict(result['percentiles']) )
        t.cols = ['avg'] + sorted(dict(result['percentiles']).keys())
        print(t.str())

if __name__== '__main__':
    import argparse
    parser = argparse.ArgumentParser( description="Compute order stats from transactions in database" )
    parser.add_argument( "-d", "--date", type=str, help='date' )
    parser.add_argument( "--account", type=str, help='account' )
    parser.add_argument( "-q", "--query", type=str, help="query string" )
    parser.add_argument( "-p", "--profile", default='production', help='database profile' )
    parser.add_argument( "-v", "--verbose", action='store_true', default=False, help='Turn on verbose output.' )
    parser.add_argument( "-n", "--dryrun",  action='store_true', default=False, help='Only print out commands, not executing them.' )
    parser.add_argument( 'delay_type', nargs='?', default='exch_order_confirm_delay',
                         choices=delays_table.keys(),
                         help='type of delay to evaluate' )
    args = parser.parse_args()
    
    dbprofile = dict(
        user= 'longbeach',
        password='Lon9beach',
        host='instance0.mysql.rds.aliyuncs.com',
        database='sandbox'
    )
    cond = {}
    if args.date:
        cond['date'] = datetime.strptime(args.date, '%Y%m%d').date()
    if args.account:
        cond['account'] = args.account
    run( dbprofile, cond, args )
