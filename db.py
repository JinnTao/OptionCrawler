"""Database access functions"""

import logging
import mysql.connector

import pandas as pd

def get_dbparams(r, type='mysqldb'):
    if type=='mysqldb':
        r=r.copy()
        r.pop('adapter', None)
        r['db'] = r.pop('database', r.get('db', None))
        r['passwd'] = r.pop('password', r.get('passwd', None))
        r['user'] = r.pop('username', r.get('user', None))
    return r

def profile( name, file='/with/longbeach/conf/core/database.lua', as_type=None ):
    code = """
function run(scriptfile)
    local env = setmetatable({}, {__index=_G})
    assert(pcall(setfenv(assert(loadfile(scriptfile)), env)))
    setmetatable(env, nil)
    return env
end
    """
    import lua
    lua.execute(code)
    env = lua.eval( "run('{0}')".format(file) )
    databases = env.databases
    if name in databases:
        t = databases[name]
        r = {}
        for i in t:
            r[i] = t[i]

        if as_type=='mysqldb':
            return get_dbparams(r)
        elif as_type=='url':
            return "{adapter}://{username}:{password}@{host}/{database}".format(**r)
        return r

class ConnectionPool:
    '''
    SQLAlchemy connection pool wrapper around MySQLdb connections
    by definition only works for MySQL connections
    '''
    def __init__(self, **conn_args):
        self.conn_args=conn_args.copy()
        import MySQLdb
        import sqlalchemy
        self.sqldbp = sqlalchemy.pool.manage(MySQLdb, recycle=60, pool_size=10, max_overflow=100)
        
    def connect(self):
        return self.sqldbp.connect(**self.conn_args)

def connect( dbprofile, buffered=False ):
    """
    Returns a connection from a longbeach database profile
    dbprofile can be a profile table or a string naming the profile
    """
    if type(dbprofile) == str:
        dbprofile = profile(dbprofile)

    if dbprofile['adapter'] == 'mysql':
        import mysql.connector as mc
        mysql_profile = dict(
            host = dbprofile['host'],
            database = dbprofile['database'],
            user = dbprofile['username'],
            password = dbprofile['password'],
            buffered = buffered,
        )
        return mc.connect(**mysql_profile)
    return None

def get_pool( conn_or_config ):
    '''
    get connection pool to the db, using a config, or a profile name
    '''
    if isinstance(conn_or_config, dict):
        # if 'adapter' in conn_or_config
        return ConnectionPool(**conn_or_config)
    elif isinstance(conn_or_config, basestring):
        return ConnectionPool(**profile(conn_or_config, as_type='mysqldb'))
    elif isinstance(conn_or_config, ConnectionPool):
        return conn_or_config
    else:
        raise TypeError("conn must be a profile name or ConnectionPool")

def get_conn( conn_or_config, buffered=False ):
    '''
    get connection to the db, using a config, or a profile name
    '''
    if type(conn_or_config) == dict:
        import mysql.connector
        args = conn_or_config.copy()
        args['buffered']=buffered
        conn = mysql.connector.connect(**args)
    elif type(conn_or_config) == str:
        conn = connect(conn_or_config, buffered=buffered)
    elif isinstance(conn_or_config, ConnectionPool):
        return conn_or_config.connect()
    else:
        conn = conn_or_config
        try:
            if not conn.is_connected():
                print("reconnect")
                conn.reconnect()
        except:
            pass
    return conn

def get_currency_rate( conn, target, base='USD', date=None ):
    """Gets the multiplier for an instrument on a given date from the db"""
    if not hasattr( get_currency_rate, "cache" ):
        get_currency_rate.cache = {}
    
    import datetime as dt
    if not date: date = dt.date.today()
    key = target + ":" + base + ':' + str(date)
    try:
        return get_currency_rate.cache[key]
    except KeyError:
        conn=get_conn(conn)
        #cursor = conn.cursor(buffered=True)
        cursor = conn.cursor()
        cursor.execute( "SELECT rate from core.fx_rates" +
                        " where currency='{}' and base='{}'".format(target, base, str(date)) + 
                        " and update_date<='{}'".format(str(date)) +
                        " order by update_date desc limit 1"
                    )
        for r in cursor:
            rate = float(r[0])
            get_currency_rate.cache[key] = rate
        cursor.close()
        return rate

def get_currency_mult(target, source, date=None, conn='production'):
    '''
    get currency multiplier m for target currency from source
    target_currency = m * source_currency
    '''
    import longbeach.db
    import datetime as dt
    if target==source: return 1.0
    date=date or dt.date.today()
    pool=longbeach.db.get_pool(conn)
    conn=pool.connect()
    usd_to_tgt=longbeach.db.get_currency_rate(conn, target, base='USD', date=date)
    usd_to_src=longbeach.db.get_currency_rate(conn, source, base='USD', date=date)
    conn.close()
    return usd_to_tgt / usd_to_src

def get_multiplier( conn, instr, date=None ):
    """Gets the multiplier for an instrument on a given date from the db"""
    import datetime as dt
    if not hasattr( get_multiplier, "cache" ):
        get_multiplier.cache = {}

    date = date or dt.date.today()
    try:
        parts = instr.split(':')
        return get_multiplier.cache[parts[0]]
    except KeyError:
        conn=get_conn(conn)
        cursor = conn.cursor()
        symbol = parts[0].split('_')
        if len(symbol) == 3:
            # future or option instrument
            mkt = symbol[1]
            ic = symbol[2]
        elif len(symbol) == 2:
            # stock
            mkt = symbol[0]
            ic = symbol[1]
        else:
            raise "Cannot get market code from symbol {0}".format( parts[0] )
        expmo = parts[1].split('w')[0] if len(parts)>1 else ''
        expiry = ( '{0}01'.format(expmo) if len(parts) > 1 else dt.datetime.today().strftime( "%Y-%m-%d" ) )
        stmt = ( "SELECT symid,market,instrument_code,contract_size from core.commodity_future_details" +
                    " where market='{0}'".format(mkt) + 
                    " and instrument_code='{0}'".format(ic) +
                    " and start_expiry <= '{0}' and end_expiry >= '{0}' ".format(expiry) +
                    " limit 1"
                    )
        logging.debug( stmt )
        cursor.execute( stmt )
        for r in cursor:
            mult = int(r[3])
            get_multiplier.cache[parts[0]] = mult
        cursor.close()
        return mult

def get_acctid( conn, acct_name ):
    """ Returns the acctid associated with the account_name from core.accts"""
    if not hasattr( get_acctid, "cache" ):
        get_acctid.cache = {}

    try:
        return get_acctid.cache[acct_name]
    except KeyError:
        conn = get_conn(conn)
        cursor = conn.cursor()
        stmt = "SELECT account_id from core.accounts where name='{0}'".format(acct_name)
        logging.debug( stmt )
        cursor.execute( stmt )
        acctid=None
        for r in cursor:
            acctid = int(r[0])
            get_acctid.cache[acct_name] = acctid
        cursor.close()
        return acctid

def get_accounts_regexp( conn, acct_regexp ):
    """ Returns a list of accounts matching the regular exression """
    conn = get_conn(conn)

    cursor = conn.cursor()
    stmt = "select name from core.accounts where name regexp '{}'".format(acct_regexp)
    logging.info(stmt)
    cursor.execute(stmt)
    result = [ r[0] for r in cursor ]
    return result

def get_ident_accounts( conn, ident_re, date=None ):
    """ Returns a list of accounts matching the ident regular exression """
    import longbeach.db
    conn = longbeach.db.get_conn(conn)
    cursor = conn.cursor()
    cond = { 'date': date } if date else {}
    condstr = ''
    if cond:
        condstr = 'and ' + (' and '.join('{}="{}"'.format(k,v) for k,v in cond.iteritems()))
    stmt = ("select distinct(a.name) from attribution.fills f "
            "join core.accounts a on "
            "a.account_id=f.acctid "
            "where ident regexp '{}' {}").format(ident_re, condstr)
    cursor.execute(stmt)
    result =[ r[0] for r in cursor ]
    cursor.close()
    return result

def get_symid( conn, symstr ):
    conn = get_conn(conn)
    cursor = conn.cursor()
    stmt = ("select symid from core.Tickers "
            "where representation='{}'"
            ).format(symstr)
    cursor.execute(stmt)
    result = [ r[0] for r in cursor ]
    cursor.close()
    return result[0] if result else None

def select_trades( config, strategy_id, sdate, edate, query='' ):
    '''
    Select a list of trades between sdate and edate inclusive, with strategy_id regexp, ordered by tv
    '''
    import mysql.connector
    from collections import namedtuple
    conn = get_conn(config)
    cursor = conn.cursor()
    keys = "time instrument dir signed_qty price size pos fees acct id tradeid ident notional"
    Trade = namedtuple('Trade', keys )
    stmt = ("SELECT from_unixtime(timeval) as tv,instrument,dir,if(dir='buy',size,-size)"
            ",price,size,pos,exchange_fee+regulator_fee+clearing_fee,a.name,f.id"
            ",tradeid,ident"
            " from attribution.fills f" +
            " join core.accounts a on f.acctid=a.account_id" +
            " where ident regexp '{0}' ".format(strategy_id) + 
            ("and ({})".format(query) if len(query) else "") +
            " and date between '{0}' and '{1}'".format(sdate.strftime("%Y-%m-%d"), edate.strftime("%Y-%m-%d")) +
            " order by tv")

    logging.debug(stmt)
    cursor.execute(stmt)

    tmp_results = [trades for trades in cursor]
    # for trades in cursor:
    #     tmp_results.append(trades)
        
    cursor.close()
    results = []
    for t in tmp_results:
        tlist = list(t) + [-int(t[3])*float(t[4])*get_multiplier(conn, t[1],edate)]
        results.append(Trade(*tlist))

    return results

def get_account_trades_v2( account, sdate, edate=None, query = '', ident=None, mutable=False, conn='production' ):
    if edate is None:
        edate=sdate
    rr=get_account_trades(**locals())
    if mutable:
        from longbeach.utils import dotdict
        return [ dotdict(x._asdict()) for x in rr ]
    return rr

def get_account_trades( conn, account, sdate, edate, query = '', ident=None, **kwargs ):
    '''
    Select a list of trades between sdate and edate inclusive, with strategy_id regexp
    '''
    import mysql.connector
    from collections import namedtuple
    if isinstance(account, basestring):
        acctid = get_acctid(conn,account)
    else:
        acctid=account
    conn = get_conn(conn)
    cursor = conn.cursor()
    keys = "time instrument dir signed_qty price size pos fees acct id tradeid ident notional"
    Trade = namedtuple('Trade', keys )

    where_clauses = ["acctid={}".format(acctid)]
    if query and len(query):
        where_clauses.append("({})".format(query))
    where_clauses.append("date between '{0}' and '{1}'".format(sdate.strftime("%Y-%m-%d"),
                                                               edate.strftime("%Y-%m-%d")))
    if ident and ident != '.*':
        where_clauses.append("ident regexp '{0}' ".format(ident))

    stmt = ("SELECT from_unixtime(timeval),instrument,dir,if(dir='buy',size,-size)"
            ",price,size,pos,exchange_fee+regulator_fee+clearing_fee,"
            "a.name,f.id,tradeid,ident"
            " from attribution.fills f" +
            " join core.accounts a on f.acctid=a.account_id" +
            " where {}".format(" and ".join(where_clauses)))

    logging.debug(stmt)
    cursor.execute(stmt)

    tmp_results = []
    for trades in cursor:
        tmp_results.append(trades)
        
    cursor.close()
    results = []
    for t in tmp_results:
        tlist = list(t) + [-int(t[3])*float(t[4])*get_multiplier(conn, t[1],edate)]
        results.append(Trade(*tlist))

    return results

def get_account_positions( conn, account, date, eod = True, query = '', ident='.*' ):
    from datetime import timedelta
    import longbeach.pnl
    conn=get_conn(conn)
    if not eod:
        date = date - timedelta(days=1)
    if query and len(query)==0:
        query = None
    if isinstance(account,basestring):
        acctid = get_acctid(conn,account)
    else:
        acctid=account

    htrades = select_trades(
        conn,
        ident,
        date-timedelta(days=365), # heuristic
        date,
        'acctid={}{}'.format(acctid, ' and ({})'.format(query) if query else '' ) )
    pos1={ t.instrument:t.pos for t in sorted(htrades, key=lambda o:o.time) }
    pos1={ k:v for k,v in pos1.iteritems() if v!=0 and not longbeach.pnl.expired(k,date) }
    return pos1

def get_account_positions2( conn, account, date, eod=True, query=None, ident=None, lookback_days=365, simulate=False ):
    '''Reads account positions - this is most efficient yet, doing most of the work on db side'''
    from datetime import timedelta
    import longbeach.pnl
    conn=get_conn(conn)
    if not eod:
        date = date - timedelta(days=1)
    if query and len(query)==0:
        query = None
    if isinstance(account,basestring):
        acctid = get_acctid(conn,account)
    else:
        acctid=account

    sdate,edate= date-timedelta(days=lookback_days),date
    cursor=conn.cursor()
    where_clauses=["acctid={}".format(acctid),
                   "date between '{}' and '{}'".format(sdate.strftime('%Y-%m-%d'),
                                                       edate.strftime('%Y-%m-%d'))]
    if ident and ident != '.*':
        where_clauses.append("ident regexp '{}'".format(ident))
    if query and len(query>0):
        where_clauses.append("({})".format(query))

    query=("""select 
        p1.instrument, id,from_unixtime(p1.timeval),id,pos,ident
    from
        attribution.fills as p1
            inner join
        (select 
            instrument,
                max(id) as max_id,
                from_unixtime(max(timeval)),
                max(timeval) as max_tv,
                from_unixtime(timeval)
        FROM
            attribution.fills
        where {0}
        group by instrument desc) as p2
    on (p1.instrument=p2.instrument) and (p1.timeval=p2.max_tv)
    where pos!=0 and {0}
    order by instrument
    """).format(' and '.join(where_clauses))
    if simulate:
        return query
    cursor.execute(query)
    r=[c for c in cursor]
    b={i[0]: i[4] for i in r if not longbeach.pnl.expired(i[0], edate)}
    return b

def get_positions_with_ident( conn, date, account=None, ident=None, eod=True, query=None, lookback_days=365, simulate=False ):
    '''Reads account positions - this is most efficient yet, doing most of the work on db side'''
    from datetime import timedelta
    import longbeach.pnl
    conn=get_conn(conn)
    if not eod:
        date = date - timedelta(days=1)
    if query and len(query)==0:
        query = None

    sdate,edate= date-timedelta(days=lookback_days),date
    cursor=conn.cursor()
    where_clauses=["date between '{}' and '{}'".format(sdate.strftime('%Y-%m-%d'),
    
                                                   edate.strftime('%Y-%m-%d'))]
    if account:
        if isinstance(account,basestring):
            acctid = get_acctid(conn,account)
        else:
            acctid=account
        where_clauses.append("acctid={}".format(acctid))
    if ident and ident != '.*':
        where_clauses.append("ident regexp '{}'".format(ident))
    if query and len(query>0):
        where_clauses.append("({})".format(query))

    query=("""select 
        p1.instrument, id,from_unixtime(p1.timeval),id,pos,ident
    from
        attribution.fills as p1
            inner join
        (select 
            instrument,
                max(id) as max_id,
                from_unixtime(max(timeval)),
                max(timeval) as max_tv,
                from_unixtime(timeval)
        FROM
            attribution.fills
        where {0}
        group by instrument desc) as p2
    on (p1.instrument=p2.instrument) and (p1.timeval=p2.max_tv)
    where pos!=0 and {0}
    order by instrument
    """).format(' and '.join(where_clauses))
    if simulate:
        return query
    cursor.execute(query)
    r=[c for c in cursor]
    b={i[0]: (i[4],i[5]) for i in r if not longbeach.pnl.expired(i[0], edate)}
    return b

def get_positions( conn, date, **kwargs ):
    '''
    Reads account positions - this is most efficient yet, doing most of the work on db side
    see get_positions_with_ident
    '''
    return { k: v[0] for k,v in get_positions_with_ident( conn, date, **kwargs ).iteritems() }

def get_ident_positions( conn, ident, date, **kwargs ):
    return get_positions( conn, date, ident=ident, **kwargs )

def get_overnight_trades(acct, date, cutoff_tm="08:45", conn='production'):
    import datetime as dt
    import longbeach.db
    query="time(from_unixtime(timeval))<'{}'".format(cutoff_tm)
    return longbeach.db.get_account_trades_v2(acct, date, query=query )

#
# retrieve the ith-most traded contract during the period from sdate to edate (inclusive on
# both ends) for symbol (ie FUT_CFFEX_IF)
#
# @conn     - connection to db, should be a connection obtained from get_conn.
# @symbol   - integer symbol id, can be contructed from Pytrion.str2sym( 'FUT_CFFEX_IF' ).id()
# @sdate    - starting date in format of 'YYYY-MM-DD'
# @edate    - ending date in format of 'YYYY-MM-DD'
# @position - 0 means the most traded one, 1 means the second most traded one and so on
#
# return - a string representing the ith-most traded contract name
def get_instr_by_vol( conn, symbol, sdate, edate, position ):
    dbconn = get_conn( conn )
    stmt = ('''select instrument_name, sum(total_volume) as vol from marketdata.instrument_dayinfo
        where symid = {sid} and trade_date >= '{sdate}' and trade_date <= '{edate}'
        group by instrument_name
        order by vol desc'''.format( sid=symbol, sdate=sdate, edate=edate) )
    cursor = conn.cursor()
    cursor.execute( stmt )
    instrs = [ r[0] for r in cursor ]
    cursor.close()
    return instrs[position]

#
# retrieve the list of the ith-most traded contract on each day during the period
# from sdate to edate (inclusive on  both ends) for symbol (ie FUT_CFFEX_IF)
#
# @conn     - connection to db, should be a connection obtained from get_conn.
# @symbol   - integer symbol id, can be contructed from Pytrion.str2sym( 'FUT_CFFEX_IF' ).id()
# @sdate    - starting date in format of 'YYYY-MM-DD'
# @edate    - ending date in format of 'YYYY-MM-DD'
# @position - 0 means the most traded one, 1 means the second most traded one and so on
#
# return - a list of (datetime.date, {'instr', 'pos', 'vol'}) for the i-th most traded contract on each day
def get_instr_list_by_vol( conn, symbol, sdate, edate, position ):
    dbconn = get_conn( conn )
    stmt = ('''select trade_date, instrument_name, total_volume as vol from marketdata.instrument_dayinfo
        where symid = {sid} and trade_date >= '{sdate}' and trade_date <= '{edate}'
        order by trade_date desc, vol desc'''.format( sid=symbol, sdate=sdate, edate=edate) )
    cursor = conn.cursor()
    cursor.execute( stmt )
    result = {}
    for r in cursor:
        if r[0] not in result:
            result[ r[0] ] = { 'instr': r[1], 'vol': r[2], 'pos': 0 }
        else:
            if result[ r[0] ]['pos'] < position:
                result[ r[0] ]['instr'] = r[1]
                result[ r[0] ]['vol']   = r[2]
                result[ r[0] ]['pos']  += 1
    cursor.close()
    return result

def account_tag_id(conn, name):
    '''Returns the tag id for the given tag'''
    conn = get_conn(conn)
    cursor = conn.cursor(buffered=True)
    query = 'select tag_id from core.account_tags_master where '\
            'tag_name="{}" order by tag_id desc limit 1'.format(name)
    cursor.execute(query)    
    return (next(cursor)[0] if cursor.rowcount==1 else None)

def get_accounts_by_tag(conn, tag_name, by_name=True):
    '''return accounts by tag'''
    result_field= 'name' if by_name else 'account_id'
    conn = get_conn(conn)
    tag_id = account_tag_id(conn,tag_name)
    if not tag_id:
        return []
    cursor = conn.cursor(buffered=True)
    query = 'select core.accounts.{} from core.account_tags '\
            'join core.accounts on core.account_tags.account_id=core.accounts.account_id '\
            'where tag_id={}'.format(result_field,tag_id)
    cursor.execute(query)    
    return [d[0] for d in cursor]

def get_account_idents(conn, date, account ):
    conn = get_conn(conn)
    cursor = conn.cursor(buffered=True)
    acctid = get_acctid(conn, account) if isinstance(account, basestring) else account
    cursor.execute('select distinct(ident) from attribution.orders where acctid={} and date="{}"'\
                   .format(acctid,str(date)))
    tidents=set(d[0] for d in cursor)
    cursor.execute('select distinct(ident) from attribution.fills where acctid={} and date="{}"'\
                   .format(acctid,str(date)))
    tidents=tidents | set(d[0] for d in cursor)
    pos = get_positions_with_ident(conn, date, account=account)
    if pos:
        return list(set(v[1] for k,v in pos.iteritems()) | set(tidents))
    return list(tidents)

def get_daily_pnl(conn, where):
    '''Queries the test.pnl_daily_v2 database'''
    import pandas as pd
    conn=get_conn(conn)
    cursor=conn.cursor()
    query=('SELECT date,a.name as acct,net,gross,position,trade,fees,contracts,currency '
           'FROM test.pnl_daily_v2 as pnl '
           'join core.accounts as a on pnl.account_id=a.account_id '
           'where {}'.format(where)
          )
    cursor.execute(query)
    def convert(a):
        import decimal
        if type(a) is decimal.Decimal: return float(a)
        return a
    z=[(a[0],[convert(x) for x in a[1:]]) for a in cursor]
    return pd.DataFrame([i[1] for i in z],
                        columns=cursor.column_names[1:],
                        index=pd.DatetimeIndex([i[0] for i in z]))

def populate_daily_pnl(conn, p):
    '''requires DataFrame with columns [u'acct', u'net', u'gross', u'position', u'trade', u'fees',
       u'contracts'], with date index'''
    p1 = p.copy()
    p1.loc[:,'account_id'] = p['acct'].apply(lambda a: get_acctid(conn,a))
    p1 = p1[['account_id'] + p.columns.tolist()[1:]]
    conn=get_conn(conn)
    cursor=conn.cursor()
    keys= 'date,'+','.join(p1.columns.tolist())
    values = [ '"'+ '","'.join([a[0].strftime('%Y-%m-%d')] + [str(i) for i in a[1]]) + '"'
     for a in p1.iterrows()]
    queries = ['replace into test.pnl_daily_v2 ({}) values ({})'.format(keys,v) for v in values]
    for q in queries:
        cursor.execute(q)
    conn.commit()

def get_cffex_extra_fees( dbconfig, idents, sdate, edate ):
    import longbeach.db

    conn = longbeach.db.get_conn( dbconfig )
    cursor = conn.cursor()
    stmt = '''select f.ident as ident, f.date as trade_date, sum(c.order_count) as order_sum, sum(c.cancel_count) as cancel_sum
              from attribution.cffex_order_cancel_counts c
                   join (select t.ident, t.date, t.acctid
                         from attribution.fills t where t.date >='{sdate}' and t.date <= '{edate}'
                         and t.ident in ({names})
                         group by t.ident, t.date, t.acctid) f
                   on c.acctid = f.acctid and c.trade_date = f.date group by f.ident,f.date;
            '''.format( sdate=sdate.strftime("%Y-%m-%d"), edate=edate.strftime("%Y-%m-%d"),
                        names=",".join( [ "'{0}'".format(i) for i in idents ] ) )

    # returns something like:
    # +------------------------------------+------------+-----------+------------+
    # | ident                              | trade_date | order_sum | cancel_sum |
    # +------------------------------------+------------+-----------+------------+
    # | pluto1_if2_0714_SHX_0.7_HTF_SHX022 | 2015-08-03 |       564 |        375 |
    # | pluto1_if2_0723_LMQ_1.0_HTF_LMQ024 | 2015-08-03 |       531 |        262 |
    # | pluto1_if2_0723_LMQ_1.2_HTF_LMQ022 | 2015-08-03 |        33 |         22 |
    # | pluto1_if2_0723_TJF_1.0_HTF_TJF014 | 2015-08-03 |       628 |        375 |
    # | pluto1_if2_0730_HDY_0.8_GMF_HDY003 | 2015-08-03 |       114 |         82 |
    # +------------------------------------+------------+-----------+------------+

    cursor.execute(stmt)
    cols = [x[0] for x in cursor.description]

    result = [ dict(zip(cols, r)) for r in cursor ]

    # print(result)

    cursor.close()
    conn.close()

    return result


def get_dayinfo(instr, date=None, conn='production'):
    '''Queries dayinfo database'''
    import longbeach.db
    import pandas as pd
    import datetime as dt
    date=date or dt.datetime.today()
    pool=longbeach.db.get_pool(conn)
    conn=pool.connect()
    df=pd.read_sql("select * from marketdata.instrument_dayinfo where instrument_name='{}' and trade_date='{}'".format(
            instr, date.strftime('%Y-%m-%d')
        ), con=conn)
    conn.close()
    return df


__conn_pool = dict()
def _conn(dbconfig):
    assert dbconfig is not None
    global __conn_pool
    dbconfig_str = ' '.join(['%s=%s' % (key, val) for key, val in dbconfig.items()])
    if dbconfig_str not in __conn_pool.keys():
        __conn_pool[dbconfig_str] = mysql.connector.connect(**dbconfig)

    return __conn_pool[dbconfig_str]


def aliyun_rds_conn():
    return _conn(dbconfig={
        'host': 'instance0.mysql.rds.aliyuncs.com',
        'user': 'longbeach',
        'password': 'L0n9beach',
    })


if __name__=='__main__':
    #print(profile('attribution_w'))
#    print(get_conn('production'))
    import datetime as dt

    sql = '''
            select trade_date, code as symbol, close
            from marketdata.commodity_volinfo
            where trade_date = '%s'
              and length(code) > 3
            ''' % (dt.date.today())
    #df = sql_query_as_df(sql, user='longbeach', passwd='L0n9beach')



    df = pd.read_sql(sql, con=aliyun_rds_conn())
    print df[df['symbol'] == 'IC1901']['close'][0]
    # conn = get_conn('production')
    # print( get_multiplier( conn, 'FUT_SHFE_RU:201601', "" ) )
    # print( get_multiplier( conn, 'FUT_SHFE_RU', "" ) )
