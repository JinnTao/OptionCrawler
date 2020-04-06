def get_expiry_date(instr):
    '''returns the expiration date of the given instrument'''
    import longbeach.core
    if isinstance(instr,basestring):
        instr = longbeach.core.Instrument.fromString( str(instr) )
    import Pytrion
    return Pytrion.get_expiry_date(instr).to_datetime().date()
