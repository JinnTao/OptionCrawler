SPECIAL_DELIM = [("[{}[".format("="*n), "]{}]".format("="*n)) for n in range(10)]

def type_of(v, *types):
    return any(isinstance(v, t) for t in types)

def indent(s, level, prefix="  "):
    return "\n".join("{}{}".format(prefix*level, l).rstrip()
                     for l in s.split("\n"))

def to_lua(v):
    if type_of(v, str):
        temp = '"{}"'.format(v)
    elif type_of(v, float, int):
        temp = "{}".format(v)
    elif type_of(v, dict):
        kvs = []
        for k, v in v.iteritems():
            vs = '{}'.format(to_lua(v))
            kvs.append("{} = {}".format(k, vs))
        temp = "{{\n{}\n}}".format(indent(",\n".join(kvs), 1))
    elif type_of(v, list, tuple, set):
        kvs = []
        for v in v:
            vs = to_lua(v)
            kvs.append('{}'.format(vs))
        temp = "{{\n{}\n}}".format(indent(",\n".join(kvs), 1))
    else:
        temp = v

    return temp

