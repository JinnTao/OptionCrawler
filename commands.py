def tdate( start, end, mkt="MKT_SHFE", as_python_date=False ):
    """
    Return a list of trading days between "start" and "end" for the given market
    start and end should be of the format YYYYMMDD
    """
    from datetime import date, datetime
    import subprocess
    import os
    import sys

    if isinstance( start, date ): start = start.strftime('%Y%m%d')
    if isinstance( end, date ): end = end.strftime('%Y%m%d')
    cmd = [ "tdate", str(start), str(end), mkt ]

    try:
        results = subprocess.check_output( " ".join(cmd), shell=True ).strip().split(os.linesep)
        # empty results
        if len(results)==1 and results[0]=='':
            return []

        if as_python_date:
            return map( lambda s: datetime.strptime(s,'%Y%m%d').date(), results )
        else:
            return map( int, results )
    except subprocess.CalledProcessError as e:
        # print >>sys.stderr, " ".join(cmd), "returned", e.returncode, e.output
        return []

def book_printer( instr, sdate, edate=None, source=None, ignore_missing=False, simulate=False, depth=True, numLevels=5 ):
    """
    Book Printer
    runs book_printer
    """

    from datetime import date
    if isinstance( sdate, date ): sdate = sdate.strftime('%Y%m%d')
    if isinstance( edate, date ): edate = edate.strftime('%Y%m%d')
    if not edate: edate=sdate

    args=[]
    if depth:
        args.append('--depth')
    if ignore_missing:
        args.append('--ignore_missing')
    if source:
        args.append('-S {}'.format(source))

    cmd = "book_printer -i {} -s '{}' -e '{}' {}".format(instr, sdate, edate, ' '.join(args))

    if simulate:
        return cmd

    import subprocess as sp
    import datetime as dt
    import re
    def parse(l):
        i = dict([i.split(':') for i in l.split(' ') if len(i.split(':'))==2])
        d = dict( time=dt.datetime.fromtimestamp(float(i['time'])),
                  mid_px=float(i['mid_px']),
                  ask_sz=int(i['ask_sz']),
                  ask_px=float(i['ask_px']),
                  bid_px=float(i['bid_px']),
                  bid_sz=int(i['bid_sz']),
        )
        for n_ in range(numLevels):
            n = n_+1
            if str(n) in i:
                b = re.split('@|x', i[str(n)])
                d["bid_px" + str(n)] = float(b[0])
                d["bid_sz" + str(n)] = int(b[1])
                d["ask_px" + str(n)] = float(b[2])
                d["ask_sz" + str(n)] = int(b[3])
        return d
    p = sp.Popen(cmd, shell=True, universal_newlines=True, stdout=sp.PIPE)
    rows = [ parse(l) for l in p.stdout if re.match('^time',l)]
    if rows:
        import pandas as pd
        df = pd.DataFrame(rows).set_index('time')
        return df
    return None

def tick_printer( instr, sdate, edate=None, source=None, ignore_missing=False, simulate=False, with_book=True ):
    """
    Tick Printer
    runs tick_printer
    """
    from datetime import date
    if isinstance( sdate, date ): sdate = sdate.strftime('%Y%m%d')
    if isinstance( edate, date ): edate = edate.strftime('%Y%m%d')
    if not edate: edate=sdate
    args=[]
    if source: args.append('-S {}'.format(source))
    if ignore_missing: args.append('--ignore-missing')
    if with_book: args.append('--book')

    cmd = "tick_printer -i {} -s '{}' -e '{}' {}".format(instr, sdate, edate, ' '.join(args))

    if simulate:
        return cmd

    import subprocess as sp
    import datetime as dt
    import re
    typemap = dict(
        time=lambda x: dt.datetime.fromtimestamp(float(x)),
        ex_time=lambda x: dt.datetime.fromtimestamp(float(x)),
        sz=int,
        px=float,
        bid_px=float,
        bid_sz=int,
        ask_px=float,
        ask_sz=int,
    )
    def parse(l):
        i = dict([i.split(':') for i in l.split(' ') if len(i.split(':'))==2])
        for k,v in i.iteritems():
            if k in typemap:
                i[k] = typemap[k](v)
        return i
    p = sp.Popen(cmd, shell=True, universal_newlines=True, stdout=sp.PIPE)
    rows = [ parse(l) for l in p.stdout if re.match('^time:.+', l) ]
    import pandas as pd
    df = pd.DataFrame(rows)
    if not df.empty:
        df.set_index('time', inplace=True)
    return df

def signalprint( config, sdate, edate ):
    """ Runs signalprint on the given config file on the given dates
    Output: DataFrame
    """
    import subprocess
    import longbeach.data
    from StringIO import StringIO
    from datetime import date, datetime

    if isinstance( sdate, date ): sdate = sdate.strftime('%Y%m%d')
    if isinstance( edate, date ): edate = edate.strftime('%Y%m%d')

    cmd = 'signalprint -c {} -s {} -e {}'.format(config, sdate, edate)
    output = subprocess.check_output(cmd.split())
    return longbeach.data.load(StringIO(output))

def candlesticks( instrument, sdate, edate, period, zeros=False, book=False, source=None ):
    import subprocess as sp
    from datetime import date, timedelta, datetime
    from StringIO import StringIO
    import longbeach.data
    import pandas as pd

    instrument = str(instrument)
    if isinstance( sdate, date ): sdate = sdate.strftime('%Y%m%d')
    if isinstance( edate, date ): edate = edate.strftime('%Y%m%d')

    add_args=''
    if zeros:
        add_args += ' --zeros'
    if book:
        add_args += ' --book'
    if source:
        add_args += ' --source {}'.format(source)

    args = '--instrument {} -s {} -e {} -p {} {}'.format(
        instrument, sdate, edate, period, add_args )
    cmd = ['candlesticks'] + args.split()
    p = sp.Popen(cmd, stdout=sp.PIPE, universal_newlines=True)
    data = longbeach.data.load_csv(p.stdout, timecol=1, header=None)
    # convert column 1 to timeval
    data.ix[:,0] = data.ix[:,0].apply(lambda x: datetime.fromtimestamp(x))
    headings = {0:'start_time',2: 'open', 3:'high', 4:'low', 5:'close', 6:'volume'}
    data.rename( columns = headings, inplace=True )
    data.index.name='tv'
    return data

def dfstream( instr, sdate, edate, source, label = 'SHFE' ):
    '''
for SHFE/DCE L1: label = 'shfe'/'dce'
for DCE L2: label = 'dcel2'
for sgx_omni_order: label = 'sgx_order'
for sgx_omni_book: label = 'sgx_book'
for sgx_omni_trade: label = 'sgx_trade'
    '''
    import subprocess as sp
    from StringIO import StringIO
    from longbeach.utils import switch
    import sys
    import pandas as pd
    import datetime as dt
    try:
        from datetime import date
        if isinstance( sdate, date ): sdate = sdate.strftime('%Y%m%d')
        if isinstance( edate, date ): edate = edate.strftime('%Y%m%d')
        sdate=str(sdate)
        edate=str(edate)

        for case in switch( label.lower() ):
            if case(['shfe','dce']):
                cmd = ['dfstream',
                       '-s', sdate,
                       '-e', edate,
                       source + ':' + instr.split(':')[0], 
                       '--fixed', '2',
                       '|', 'grep', instr ]  
                break
            if case('dcel2'):
                cmd = ['dfstream',
                       '-s', sdate,
                       '-e', edate,
                       source + ':' + instr.split(':')[0],
                       '--fixed', '2',
                       '|', 'egrep', '"dce_l2_best_and_deep.+' + instr + '"']            
                break
            if case('sgx_order'):
                cmd = ['dfstream',
                       '-s', sdate,
                       '-e', edate,
                       source + ':' + instr.split(':')[0],
                       '--fixed', '2',
                       '|', 'egrep', '"sgx_omni_order.+' + instr + '"']
                break
            if case('sgx_trade'):
                cmd = ['dfstream',
                       '-s', sdate,
                       '-e', edate,
                       source + ':' + instr.split(':')[0],
                       '--fixed', '2',
                       '|', 'egrep', '"sgx_omni_trade.+' + instr + '"']
                break
            if case('sgx_book'):
                cmd = ['dfstream',
                       '-s', sdate,
                       '-e', edate,
                       source + ':' + instr.split(':')[0],
                       '--fixed', '2',
                       '|', 'egrep', '"sgx_omni_book.+' + instr + '"']
                break
            if case():
                print "Please select the market you want to run dfstream. SHFE/DCE is the default."
                cmd = ['dfstream',
                       '-s', sdate,
                       '-e', edate,
                       source + ':' + instr.split(':')[0], 
                       '--fixed', '2',
                       '|', 'grep', instr ]
                break
            
        a = sp.check_output( " ".join( cmd ), shell = True )
        tag  = [ item.split(':')[0] for item in StringIO(a).readline().split() ]
        a = pd.read_csv(StringIO( a ), header = False,
                        sep = " ",
                        index_col = 0,
                        parse_dates = filter( lambda x : 'time' in x, tag ),
                        names = tag,
                        date_parser = lambda x : dt.datetime.fromtimestamp( float( x.split(':')[1])) )
        for item in filter( lambda x : 'time' not in item, tag ) :
            if 'time' not in item:
                a[item] = a[item].apply( lambda x: x.split(':',1)[1] )
        return a
    except sp.CalledProcessError as e:
        print >> sys.stderr, " ".join( cmd ), "returned", e.returncode, e.output

