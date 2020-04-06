from longbeach_optioncore import OptionScenarioEvaluator
from longbeach_optioncore import OptionValues
import longbeach_optioncore
import longbeach.db

def spot_weekly(d):
    '''Returns the current TAIFEX weekly future on the given day 'd' '''
    import datetime as dt
    import dateutil.rrule as rr
    ed = list(rr.rrule(rr.MONTHLY, count=1,byweekday=rr.WE,
                       dtstart=d))[0]
    dates = list(rr.rrule(rr.MONTHLY, count=6,byweekday=rr.WE,
                             dtstart=dt.date(ed.year,ed.month,1)))
    n = len([dd for dd in dates if d > dd.date()])
    return '{}w{}'.format(d.strftime('%Y%m'),n+1)

def spot_month(d):
    '''
    Returns the current TAIFEX monthly future on the given day 'd'.
    On expiration day, the next month becomes the spot-month
    '''
    import datetime as dt
    from longbeach.utils import weeknum
    import dateutil.rrule as rr
    dates = list(rr.rrule(rr.MONTHLY, count=3, byweekday=rr.WE(3),
                          dtstart=dt.date(d.year,d.month,1)))
    ref = dates[0]
    if d < ref.date():
        return '{:04d}{:02d}'.format(d.year,d.month)
    return '{:04d}{:02d}'.format(dates[1].year,dates[1].month)

class OptionValuesProvider(longbeach_optioncore.DummyValuesProvider):
    def __init__(self, conn, today ):
        import Pytrion as pt
        import datetime
        super(OptionValuesProvider,self).__init__()
        if type(today) == datetime.date:
            today = pt.Date(today.year, today.month, today.day, 0, 0, 0)
        self.today = today
        import datetime as dt
        self.pydate = dt.date(today.year(), today.month(), today.day())
        self.conn = longbeach.db.get_conn(conn)
        self.values = {}
        self.forward = {}
        self.maturity = {}
        self.discount = {}
        
    def getContractSize(self, instr):
        return longbeach.db.get_multiplier( self.conn, str(instr), self.today )

    def get(self, instr):
        instr = str(instr)
        if instr not in self.values:
            import longbeach.pnl
            import datetime as dt
            pydate = dt.date(self.today.year(),
                             self.today.month(),
                             self.today.day())
            v = longbeach.pnl.get_prev_optionvalues( self.conn,
                                                   [instr],
                                                   pydate + dt.timedelta(days=1))
            v = v[instr]
            ov = OptionValues()
            ov.greeks().setDelta(v['delta'])
            ov.greeks().setGamma(v['gamma'])
            ov.greeks().setTheta(v['theta'])
            ov.greeks().setVega(v['vega'])
            ov.greeks().setVegaTw(v['vega_tw'])
            ov.setForward(v['forward'])
            ov.setTheoVol(v['theo_vol']*0.01)
            ov.setTheo(v['value'])
            self.values[instr] = ov
            return ov
        return self.values[instr]

    def getAtmForward(self, instr):
        instr = str(instr)
        if instr in self.forward:
            return self.forward[instr]
            
        import datetime as dt
        v = longbeach.pnl.get_prev_optionvalues( self.conn, [instr], self.pydate + dt.timedelta(days=1))
        if instr not in v:
            print('missing instrument in optionvalues {}'.format(instr))
            return 0
        v = v[instr]
        fwd = v['forward']
        self.forward[instr] = fwd
        return fwd

    def getMaturity(self, instr):
        instr = str(instr)
        if instr in self.maturity:
            return self.maturity[instr]
            
        import datetime as dt
        v = longbeach.pnl.get_prev_optionvalues( self.conn, [instr], self.pydate + dt.timedelta(days=1))
        v = v[instr]
        mat = v['maturity']
        self.maturity[instr] = mat
        return mat

    def getDiscountFactor(self, instr):
        instr = str(instr)
        if instr in self.discount:
            return self.discount[instr]
        
        import datetime as dt
        v = longbeach.pnl.get_prev_optionvalues( self.conn, [instr], self.pydate + dt.timedelta(days=1))
        v = v[instr]
        df = v['discount']
        self.discount[instr] = df
        return df

def read_cpview(file):
    '''
    return last_time, exp_fwd, cpview[columns]
    '''
    import pandas as pd
    import os
    if os.stat(file).st_size == 0:
        return 0, {}, pd.DataFrame()

    df = pd.read_csv(file, header=0)
    last_time = df['time'].values[-1]
    last_df = df[df['time']==last_time]
#    last_df.loc[:,'expiration'] = last_df['expiration'].astype(str)

    exp_fwd = {}
    expiries = sorted(set(last_df['expiration'].values))
    for exp in expiries:
        exp_fwd[exp] = last_df[last_df['expiration']==exp]['forward'].values[0]
    
    call_rows = last_df[last_df['callput']=='RT_CALL']
    # call_rows.loc[:,'strike'] = call_rows['strike'].astype(float)
    put_rows = last_df[last_df['callput']=='RT_PUT']
    # put_rows.loc[:,'strike'] = put_rows['strike'].astype(float)
    cpview = pd.merge(call_rows,put_rows, on=['expiration','strike'], suffixes=('_c', '_p'))
    
    columns = [
    'mkt_bid_size_c','mkt_bid_c','our_bid_c','our_bid_size_c','our_ask_c','our_ask_size_c','mkt_ask_c','mkt_ask_size_c',
    'expiration','strike',
    'mkt_bid_size_p','mkt_bid_p','our_bid_p','our_bid_size_p','our_ask_p','our_ask_size_p','mkt_ask_p','mkt_ask_size_p',
    ]  
    return last_time, exp_fwd, cpview[columns]

