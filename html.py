TABLECSS = '''
<style media='all' type='text/css'>
     table,
     .dataframe {
       width: 100%;
       margin-bottom: 20px;
       border: 1px solid #dddddd;
       border-collapse: collapse;
     }
     th, td {
       padding: 0.25em;
       line-height: 20px;
       vertical-align: top;
       text-align: right;
       border: 1px solid #dddddd;
     }
     .tr-odd { background-color: #eeeeee; }
     .tr-even { background-color: transparent; }
</style>
'''

def _default_cellstyle(value, row, col):
    import numbers
    style = 'text-align: right; ' 
    if isinstance(value,numbers.Number):
        style += ('color:red;' if value < 0 else 'color:black;')
    return style

def format_df( summary, cellstyle = _default_cellstyle, classes = [] ):
    import cgi
    if type(cellstyle)==str:
        cellstyle_str = cellstyle
        cellstyle = lambda x: cellstyle_str

    def format_value(v):
        import numpy as np
        if type(v) == float or isinstance(v,np.float):
            return "{:,.02f}".format(v)
        elif type(v) == int or isinstance(v,np.integer):
            return "{:,}".format(v)
        else:
            return str(v)

    _classes = ["dataframe"] + classes
    html = '<table class="{}">'.format(' '.join(_classes))
    html += "<tr><th>{}</th>".format(summary.columns.name if summary.columns.name else '')
    for value in list(summary.columns):
        html += "<th>{}</th>".format(value)
    html += "</tr>"
    if summary.index.name:
        html += "<tr><th>{}</th>{}</tr>".format(summary.index.name, "<td></td>" * len(summary.columns))
    cols=summary.columns
    il=1
    label = ['tr-even', 'tr-odd'] if len(summary)>2 else ['','']
    for row in summary.itertuples():
        html += '<tr class="{}"><th>{}</th>'.format(label[il], row[0])
        values = row[1:]
        for i in range(len(cols)):
            v = values[i]
            html += '<td style="{}">{}</td>'.format(
                cellstyle(v, row[0], cols[i]),
                cgi.escape(format_value(v)))
        html += "</tr>\n"
        il = (il+1)%2
    html += "</table>"
    return html

def inline_css(html, handler='inlinestyler'):
    if handler=='inlinestyler':
        from inlinestyler.utils import inline_css as f
    elif handler == 'premailer':
        from premailer import transform as f
    return f(html)

def format(objs, **kwargs):
    '''HTML format a list of objects
    Returns an HTML string
    '''
    s = ''
    for o in objs:
        import pandas as pd
        if isinstance(o, pd.DataFrame):
            s += format_df(o, cellstyle=_default_cellstyle,**kwargs)
        else:
            f = getattr(o, '_repr_html_', None)
            s += (f() if f else str(o))
    return s
