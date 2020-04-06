def split_tradelogs( date_str, tdarchive, tradelog_dir=None, dryrun=False ):
    import datetime as dt
    import os.path
    import os
    import tarfile
    import longbeach.utils
    import re

    if not tradelog_dir:
        if 'LONGBEACH_TRADELOGS_ROOT' in os.environ:
            tradelog_dir=os.environ['LONGBEACH_TRADELOGS_ROOT'] + '/'
        else:
            tradelog_dir = ''

    d = dt.datetime.strptime( str(date_str), '%Y%m%d' ).date()
    path = d.strftime('%Y/%m/%d/')
    longbeach.utils.mkdir_f(tradelog_dir + path)
    f = tarfile.open(tdarchive)
    m = f.getmembers()
    def make_name(fn):
        return tradelog_dir + path + os.path.basename(fn)
    files = [ (i, make_name(i.name))
              for i in m if re.match('.+client.+log$', i.name ) ]
    if not dryrun:
        for i in files:
            with open( i[1] ,'w' ) as outf:
                infile = f.extractfile(i[0])
                if not infile:
                    print('Error:' + i[1])
                for line in infile:
                    outf.write(line)
    else:
        for i in files:
            print(i[1])

