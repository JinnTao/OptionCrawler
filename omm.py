def make_vol_dataset(file):
    '''Load a GvvFit output as a DataFrame'''
    import datetime as dt
    import pandas as pd
    df = pd.read_csv(file, sep=' ', header=None)
    df.columns = ['tm','volcurve','sym','exp','forward','atmv','rho','volvol','alpha','unamed']
    df['dt'] = df['tm'].apply(lambda x: dt.datetime.fromtimestamp(x))
    df.set_index(df['dt'],inplace=True)
    return df

def load(date):
    '''Returns: fills dataframe, dict of ref values by expiry'''
    import longbeach.data
    import pandas as pd
    import json
    
    filename='/nfs/public_data/optiontrading/trade_analysis/omm_txo_987_FUBON_TAIFEX003.ordertracker.FUBON_TAIFEX003-{0}.bson'.format(date.strftime('%Y%m%d'))
    import subprocess
    p = subprocess.Popen(['b2json', filename], stdout=subprocess.PIPE)
    atmv = make_vol_dataset('data/GvvFit_{}.log'.format(date.strftime('%Y%m%d')))
    atmv_e = {e: atmv[atmv['exp']==e] for e in atmv['exp'].unique()}

    atmv_e = {k: v.groupby(v.index).last() for k,v in atmv_e.iteritems()}

    d = [json.loads(s.replace('-nan', 'NaN')) for s in p.stdout]

    fills = [t.items()[0][1] for t in d if t.keys()[0]=='fill']
    fills = [f for f in fills if not f['instr'].startswith('FUT')]
    longbeach.data.__format_timevals(fills)

    # this is a fix for bad data, remove in future
    for f in fills:
        optinfo = f['optinfo']
        for k in ['delta_shift', 'vega_shift', 'phd_distortion']:
            if k not in optinfo.keys() and k+':' in optinfo.keys():
                optinfo[k] = optinfo[k+':']

    df = pd.DataFrame([(f['instr'], f['instr'].split(':')[1],f['dir'],f['fill_px'],f['fill_sz'],
                        f['optinfo']['vega']*f['fill_sz'],
                        f['optinfo']['delta_shift:'],
                        f['optinfo']['vega_shift:'],
                        f['latefill']
                       ) for f in fills],
                      columns=['instr', 'exp', 'dir','px', 'sz','vega','dshift','vshift','latefill'],
                      index=[f['ts'] for f in fills])

    def get_atmv(x):
        b = atmv_e[x.exp]['atmv']
        i = b.index.asof(x.name)
        return b[i] if not pd.isnull(i) else 0

    def get_fwd(x):
        b = atmv_e[x.exp]['forward']
        return b[b.index.asof(x.name)] if not pd.isnull(b.index.asof(x.name)) else 0

    df.loc[:,'atmv'] = df[['exp']].apply(get_atmv, axis=1)
    df.loc[:,'fwd'] = df[['exp']].apply(get_fwd, axis=1)
    df=df[df['atmv'] != 0]

    return df,atmv_e

def plot_buysell(v,n,df,atmv_e):
    import matplotlib.pyplot as plt
    b = v[v['dir']=='buy']
    s = v[v['dir']=='sell']
    lf = v[v['latefill']]
    d = v[(abs(v['dshift'])>0.5) & ((v['dshift']+v['vshift']) > 0.5)]
    vs = v[(abs(v['vshift']>0.5)) & ((v['dshift']+v['vshift']) > 0.5)]
    plt.scatter(b.index,b.atmv,marker='^',c='r',edgecolor='face',s=40, alpha=0.5)
    plt.scatter(s.index,s.atmv,marker='v',c='g',edgecolor='face',s=40, alpha=0.5)
    plt.scatter(d.index,d.atmv,marker='o',s=80,facecolors='none',label='dshift')
    plt.scatter(lf.index,lf.atmv,marker='x',c='b',s=100, label='latefill')
    plt.scatter(vs.index,vs.atmv,marker='D',c='b',s=100,facecolors='none', label='vshift')
    atmv_e[n]['atmv'].plot(drawstyle='steps')
    plt.legend(title=n).get_frame().set_alpha(0.5)
    atmv_e[n]['forward'].plot(secondary_y=True,color='grey')
    plt.xlim(df.index.min()-dt.timedelta(minutes=10),df.index.max()+dt.timedelta(minutes=10))
    plt.grid(True)
