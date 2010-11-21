'''
Filter class for sql queries.
'''    

class Filter(object):
    '''
    Provides the ability to create queries using >,<, !=, etc.
    Functions are defined as staticmethods, so no need to instatiate.
    Usage:
    say the model MyModel includes a numeric field 'amount'
    >>> q = Query(model=MyModel).filter(amount = Filter.gt(100))

    returns the records where amount > 100
    '''
    @staticmethod
    def _output(op, value=None, column=None):
        if not value and column:
            return (op % escape(column),)
        if value:
            return (op, value)
        else:
            return (op,)
    
    @staticmethod
    def isnull():
        return Filter._output(" is Null", None)
    
    @staticmethod
    def notnull():
        return Filter._output(" is NOT Null", None)

    @staticmethod
    def gt(value=None, column=None): # Have class Column inherit from Filter, to just get op and % the value?
        return Filter._output(">%s", value, column)

    @staticmethod
    def ge(value=None, column=None):
        return Filter._output(">=%s", value, column)

    @staticmethod
    def lt(value=None, column=None):
        return Filter._output("<%s", value, column)

    @staticmethod
    def le(value=None, column=None):
        return Filter._output("<=%s", value, column)

    @staticmethod
    def ne(value=None, column=None):
        return Filter._output("!=%s", value, column)

    @classmethod
    def eq(cls, value=None, column=None):
        if not column and not value:
            return cls.isnull()
        return Filter._output("=%s", value, column)

    @staticmethod
    def between(value1, value2, NOTflag=None):
        op = " NOT BETWEEN %s AND %s" if NOTflag else " BETWEEN %s AND %s"
        lval = min(value1, value2)
        uval = max(value1, value2)
        return (op, lval, uval)
    
    @staticmethod
    def isin(value):
        if hasattr(value, "__iter__"):
            if len(value) > 1:
                return Filter._output(" IN %s" % str(tuple(value)),)
            else:
                value = value[0]
        return Filter.eq(value)

    @staticmethod
    def islike(value):
        return Filter._output(" LIKE %s", value)

    @staticmethod
    def _getvalue(value):
        if isinstance(value,tuple):
            try:
                return value[1]
            except IndexError:
                return value
        else:
            return value

    @staticmethod
    def _extractvalue(value):
        if isinstance(value,tuple) and len(value)==1:
            return False
        return True
    
    @staticmethod
    def _extractvalues(itervals):
        ll = []
        for x in itervals:
            if hasattr(x, "__iter__"):
                ll.extend(x[1:])
            else:
                ll.append(x)
        return ll
