import pandas as pd
from longbeach.utils import *

def dates(ident):
    import os
    import datetime as dt
    d = sorted([ dt.datetime.strptime( i, '%Y%m%d' ).date()
                 for i in os.listdir(ident) if os.path.isdir(os.path.join(ident,i)) ])
    return d
    
def load_signalinfo(ident, date=None):
    import longbeach.data
    import os
    import datetime
    if date == None:
        full = pd.concat(
            [ longbeach.data.load_signalinfo(('{0}/'+i+'/{0}.signalinfo-'+i+'.csv').format(ident))
              for i in os.listdir(ident) ] )
        full.sort_index(inplace=True)
        return full
    else:
        if type(date)==datetime.date:
            date=date.strftime('%Y%m%d')
        data = longbeach.data.load_signalinfo(('{0}/'+date+'/{0}.signalinfo-'+date+'.csv').format(ident))
        return data

def fills(ident, date):
    import bson
    import os.path
    import datetime as dt

    if type(date)==dt.date:
        date=date.strftime('%Y%m%d')

    f=open(os.path.join(
            ident,
            "{date}/{ident}.ordertracker.TEST0-{date}.bson".format(
                ident=ident, date=date)))
    data = bson.decode_all(f.read())
    fills = [i['fill'] for i in data if i.keys()[0] == 'fill']
    for f in fills:
        f['ts'] = dt.datetime.fromtimestamp(f['ts']/1000000.0)
        f['fees_all'] = sum([ v for _,v in f['fees'].iteritems() ])
    df_fills = pd.DataFrame(fills)
    return df_fills

def summarize(ident):
    '''
    Summarize backtest daily PNL
    Returns: longbeach.table.Table
    '''
    import longbeach.table
    import os
    import os.path
    t = longbeach.table.Table()
    dates = sorted([ i for i in os.listdir(ident) if os.path.isdir(os.path.join(ident,i)) ])
    for d in dates:
        outdir=os.path.join(ident,d)
        with pushd(outdir):
            r = [float(i.split()[-1]) for i in open('pnl') if 'Net PNL:' in i]
            if len(r): t.set( d, 'Net', r[0] )
            r = [float(i.split()[-1]) for i in open('pnl') if 'Gross PNL:' in i]
            if len(r): t.set( d, 'Gross', r[0] )
    t.sum_rows()
    return t

def run_on_date( ident, date, cmd ):
    import subprocess as sp
    dir="{}/{}".format(ident,date)
    with pushd(dir):
        if type(cmd) == str:
            return sp.check_call( cmd, shell=True )
        else:
            return cmd

def run_worker(args):
    import sys
    f=args['configfile'] #os.path.abspath(arg_configfile)
    ident=args['ident']
    outdir=args['dir']
    d=args['date']
    executable=args['executable']
    account='TEST0'
    logfile='{}.{}.stdout'.format(ident,d.strftime('%Y%m%d'))
    cmdfmt = '{executable} -s {date} -e {date} -n {ident} -c {configfile} -a {account} -M {simfile} > {logfile}'

    import shutil
    shutil.rmtree(outdir, ignore_errors=True)
    mkdir_f(outdir)
    with pushd(outdir):
        cmd = cmdfmt.format(
            executable = executable,
            date=d,
            ident=ident,
            configfile=f,
            account=account,
            logfile=logfile,
            simfile='{}.{}.sim'.format(ident,d.strftime('%Y%m%d')),
        )
        # print(cmd)
        import subprocess as sp
        try: 
            a = sp.check_output( cmd, shell = True )
            a = sp.check_output( 'ipnl.rb *.bson > pnl', shell = True )
        except sp.CalledProcessError as e:
            print >> sys.stderr, cmd, "returned", e.returncode, e.output
    
def simulate( ident, configfile, dates, njobs, executable='multimarketmaker' ):
    '''Run simulation in parallel'''
    import os.path
    import multiprocessing
    import concurrent.futures as fut
    arg_list = [ dict( dir="{}/{}".format(ident,d.strftime('%Y%m%d')),
                       configfile=os.path.abspath(configfile),
                       date=d,
                       ident=ident,
                       executable=executable,
                   )
                 for d in dates ]
    pool = multiprocessing.Pool(njobs)
    pool.map_async(run_worker, arg_list).get(9999)
    
def simulate_async( ident, configfile, dates, njobs=10, executable='multimarketmaker' ):
    '''Run simulation in async mode
    compatible with concurrent.futures, use like this:
 
    jobs = simulate_async(...)
    concurrent.futures.wait(jobs)
    
    '''
    import concurrent.futures as fut
    import os.path
    import longbeach.utils as utils
  
    pool = utils.ProcessPoolExecutor(njobs)
    arg_list = [ dict( dir="{}/{}".format(ident,d.strftime('%Y%m%d')),
                       configfile=os.path.abspath(configfile),
                       date=d,
                       ident=ident,
                       executable=executable,
                   )
                 for d in dates ]
    handles=[pool.submit(run_worker, a) for a in arg_list]
    pool.shutdown(wait=False)
    return handles

def trade_pnl(ident, date):
    import longbeach.db
    trades2 = fills(ident,date)
    a=trades2[['ts','instr', 'fees_all','fill_px', 'fill_pos', 'fill_sz']].copy()
    a.loc[:,'mult'] = a.loc[:,'instr'].apply(lambda x: longbeach.db.get_multiplier('production',x,date))
    a.loc[:,'ppos'] = (a.loc[:,'fill_pos'].shift(1)).fillna(0)
    a.loc[:,'delta'] = (a.ppos * a.fill_px.diff() * a['mult']).fillna(0)
    a.loc[:,'gross'] = a['delta'].cumsum()
    a.loc[:,'net'] = a.gross - a.fees_all.cumsum()
    return a[['ts','instr', 'fill_px', 'fill_sz', 'fees_all', 'gross','net']]

def plot_pnl( ident, date, fees=True ):
    df = load_signalinfo(ident,date)

    df.fillna( 0, inplace=True )
    df['entry_tm']=df.index
    df = df[df['status']=='OK']
    entries = df[['entry_tm','esig','dir','f0']].copy()
    entries.columns = ['tm','sig','dir','trdpx']
    entries.loc[:,'oc'] = 'open'
    exits = df[['exit_tm', 'xsig','dir','f1']].copy()
    exits.columns   = ['tm','sig','dir','trdpx']
    exits.loc[:,'oc'] = 'close'
    exits.loc[:,'dir'] = exits['dir'].apply(lambda x: 'buy' if x=='sell' else 'sell') #reverse dir for exits

    trades = pd.concat( [entries,exits], ignore_index=False )
    trades.loc[:,'trdpx'] = trades['trdpx'].apply(lambda x: float((((x.split(" "))[1]).split("@"))[0])) 
    trades.sort('tm', ascending=True, inplace=True)
    trades.index = range(len(trades))
    trades.loc[:,'signed_qty'] = trades['dir'].apply(lambda x: 1 if x=='buy' else -1)
    trades.loc[:,'pos'] = trades['signed_qty'].cumsum()
    trades.loc[:,'pnl'] = 0 #initialize to 0
    for i,r in trades[1:].iterrows():
        dpx = float(trades.loc[i,'trdpx']) - float(trades.loc[i-1,'trdpx'])
        trades.loc[i,'pnl'] = trades.loc[i-1,'pnl'] + int(trades.loc[i-1,'pos'])*dpx

    pnl_df = trade_pnl(ident, date)

    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(nrows=3, ncols=1, figsize=(15,20))
    pnl_df[['ts', 'fill_px', 'gross', 'net']].plot( x='ts', secondary_y=['gross', 'net'], ax=axes[0])

    plt.grid(True)
    trades.plot(x='tm', y='pos', ax=axes[1], ylim=(-1.5,1.5), drawstyle='steps-post')
    trades[trades['oc']=='open'].plot(x='tm', y='sig', ax=axes[2], ylim=(-3,3), style='ro')
    trades[trades['oc']=='close'].plot(x='tm', y='sig', ax=axes[2], ylim=(-5,5), style='go')

    return fig,axes

def get_maxmin(ident):
    import longbeach.table
    t = longbeach.table.Table()
    for r_ in longbeach.backtest.dates(ident):
        r=r_.strftime('%Y%m%d')
        f = trade_pnl( ident, r )
        t.set( r, 'max', f.pnl.max() )
        t.set( r, 'min', f.pnl.min() )
        t.set( r, 'qty', f.fill_sz.abs().sum() )
    return t.to_DataFrame()    
