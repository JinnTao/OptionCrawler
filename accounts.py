class tags(object):
    '''Gets the set of accounts given a tag'''
    def __init__(self, tags_file='/with/longbeach/conf/core/tags.lua'):
        import lupa
        self.L=lupa.LuaRuntime()
        self.t=self.L.execute(open(tags_file).read())

    def get_account_tags(self, acct):
        import re
        m=[(k,{x for x in v.values()}) for k,v in self.t.regex.items() if re.match(k,acct)]
        tags=reduce(lambda a,b: a | b[1], m,set())
        tags |= set(self.t.accounts[acct].values() if acct in self.t.accounts else [])
        tags |= {'all'}
        return tags

def account_entries(acct_table_file='/with/longbeach/conf/core/acct.table'):
    '''Generator for account, acctid tuples'''
    import re
    f=open(acct_table_file,'r')
    for l in f.readlines():
        l=re.sub('\s*#.*','',l).strip()
        if len(l)==0: continue
#         if l.startswith('#'): continue
        yield re.split(r'\s+',l)

class AccountTagMap(object):
    '''Holds a mapping of accounts to tags and vice-versa'''
    def __init__(self,
                 accts_file='/with/longbeach/conf/core/acct.table',
                 tags_file='/with/longbeach/conf/core/tags.lua' ):
        t=tags(tags_file)
        self.acct_to_tags = {}
        self.tag_to_accts = {}

        for k,_ in account_entries(accts_file):
            acct_tags = t.get_account_tags(k)
            self.acct_to_tags[k] = acct_tags
            map(lambda t:
                self.tag_to_accts.setdefault(t,set()).update({k}),
                acct_tags)

    def get_tags(self, acct):
        return self.acct_to_tags[acct]

    def get_accounts(self, tag):
        return self.tag_to_accts[tag]
    
class TagsRuntime(AccountTagMap):
    '''Evaluates a tags-expression and returns the set of matched accounts'''
    def __init__(self,**kwargs):
        super(TagsRuntime,self).__init__(**kwargs)
        from collections import defaultdict
        self.context = defaultdict(set,self.tag_to_accts)

    def eval(self, expr):
        try:
            return eval(expr, {}, self.context)
        except:
            return {}

def eval_tags(expr, runtime=None):
    '''convenience function for evaluating a single tags expression'''
    runtime = TagsRuntime() if runtime is None else runtime
    return runtime.eval(expr)
