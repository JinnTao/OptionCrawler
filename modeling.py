class Project(object):
    def __init__(self, name, model, root=None):
        super(Project,self).__init__()
        import os.path
        self.name = name
        self.root = os.path.abspath('.' if root == None else root) 
        self.model = os.path.abspath(model)
        self.projpath = os.path.join(self.root, name)
        self.__ivar_file = os.path.join(self.projpath, name)+'.sqlite'
        
    def get_signals_raw(self):
        import sqlite3
        import pandas as pd
        conn = sqlite3.connect(self.__ivar_file)
        df = pd.read_sql('select * from data order by scheduled_timeval', conn,
                         index_col='scheduled_timeval')
        return df
    
    @property
    def ivar_file(self): return self.__ivar_file
    
    def generate_data(self, dates, njobs=10):
        from longbeach.utils import pushd, mkdir_p
        mkdir_p(self.projpath)
        import pathos.multiprocessing as mp
        pool = mp.ProcessingPool(njobs)
        
        def process(fname, dbfile):
            import os.path
            with pushd(p.projpath):
                df = da.getSignalDataFrame(fname, 'signalgen.dat.desc')
                conn = sqlite3.connect(dbfile)
                df.to_sql('data', conn, if_exists='append')

        with pushd(self.projpath):
            da.getSignalOnDateList(dates,self.model, njobs=njobs)
            import glob
            a=[f for f in glob.iglob('signalgen.*.trigger.dat') if 'ov' not in f]
            conn=sqlite3.connect(self.__ivar_file)
            c=conn.cursor()
            c.execute('drop table if exists data')
            conn.commit()
    
            pool.map(process, a, [self.__ivar_file]*len(a))
        
        return None
    
    def store(name, df):
        conn = sqlite3.connect(self.__ivar_file)
        df.to_sql('df.'+name, conn, if_exists='replace')
        
    def load(name, index_col='scheduled_timeval'):
        conn = sqlite3.connect(self.__ivar_file)
        return pd.read_sql('select * from {}'.format('df.'+name), conn, index_col=index_col)
