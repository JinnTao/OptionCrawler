from pandas import DataFrame


class Table:
    '''
    Implementation of a self formatting table
    add rows and cols and when you print, it'll be a pretty table
    '''
    def __init__( self, title='' ):
        self.rows = []
        self.cols = []
        self.data = {}
        self.title = title

    def empty(self):
        return len(self.rows) == 0 and len(self.cols) == 0

    def set( self, row_id, col_id, v ):
        if row_id not in self.rows:
            self.rows.append(row_id)
            self.data[row_id] = {}
        if col_id not in self.cols:
            self.cols.append(col_id)
        self.data[row_id][col_id] = v

    def set_row( self, row_id, v ):
        for k in v:
            if k not in self.cols:
                self.cols.append(k)
        if row_id not in self.rows:
            self.rows.append(row_id)
            self.data[row_id] = {}
        self.data[row_id].update(v)

    def get_row( self, row_id, none_val = '' ):
        d = self.data[row_id]
        if d:
            return [ d[col] if col in d else none_val for col in self.cols ]
        else:
            return none_val

    def get( self, row_id, col_id ):
        return self.data.get(row_id, {}).get(col_id,None)

    def __repr__(self):
        return self.str()

    def str(self):
#        s = ',' + ','.join(self.cols) + '\n'
#        for i in self.rows:
#            s += str(i) + ','
#            for j in self.cols:
#                s += (str(self.data[i][j]) if j in self.data[i] else '-') + ','
#            s += "\n"
#        return s
        import tabulate
        rows = []
        for rid in self.rows:
            row = self.get_row(rid)
            row.insert(0, rid)
            rows.append(row)
        return tabulate.tabulate(rows,
                                 headers = [self.title] + self.cols,
                                 floatfmt=",.2f",
                                 numalign='right',
                             )
    def csv(self, header=True):
        import csv
        import StringIO
        output = StringIO.StringIO()
        c = csv.writer(output)
        c.writerow( [self.title] + self.cols ) if header else None
        rows = []
        for rid in self.rows:
            row = self.get_row(rid)
            row.insert(0, rid)
            c.writerow(row)

        return output.getvalue()

    def sum_rows( self, label='Total', skip=[] ):
        """Sum down the rows, adds an additional row"""
        for c in self.cols:
            if c in skip:
                continue
            data = [ self.get(r,c) for r in self.rows]
            total = sum([ x for x in data if x])
            self.set( label, c, total )

    def sum_rows2( self, label='Total', include=[] ):
        """Sum down the rows, adds an additional row"""
        for c in self.cols:
            if c not in include:
                continue
            data = [ self.get(r,c) for r in self.rows]
            total = sum([ x for x in data if x])
            self.set( label, c, total )

    def sum_cols( self, label='Total' ):
        """Sum across columns, adds an additional column"""
        for r in self.rows:
            data = [ self.get(r,c) for c in self.cols]
            total = sum([ x for x in data if x])
            self.set( r, label, total )
    
    def to_DataFrame( self, none_val = None ):
        df = DataFrame(columns=self.cols)
        for i in self.rows:
            df.loc[i] = self.get_row(i, none_val)
        return df

def from_DataFrame( df ):
    tbl = Table()
    for i,r in df.iterrows():
        for c in r.index:
            tbl.set( i, c, r[c] )
    return tbl
    

if __name__ == '__main__':
    a = Table()
    a.set( 'a0', 'b', 123 )
    a.set( 'a1', 'd', 555 )
    a.set( 'a1', 'b', 525 )
    a.set_row( 'a3', { 'z': 6, 'y': 7, 'm': 8 } )
    print(a.str())

    a = Table('mytitle')
    a.set( 'a0', 'b', 144 )
    a.set( 'a1', 'd', 555 )
    a.set( 'a1', 'b', 525 )
    a.set_row( 'a3', { 'z': 6, 'y': 7, 'm': 8 } )
    print(a.str())
    print(a.csv())
    print(a.csv(header=False))

    a.sum_cols()
    print(a.str())
    print("DataFrame")
    print(a.to_DataFrame(none_val=0))
