'''
Query class(es) for sql

'''

from autumn.db.filter import Filter
from autumn.Ipy.dbconnection import escape, unescape, sqlPlaceholder

class Query(object):
    '''
    Creates a query object:
    qry = Query(model=Author,
    '''
    def __init__(self, query_type='SELECT *',  model=None, order_by=None,
                       **kwargs):

        self.cache = None

        self.model = model()
        self.modelClass = model        
        self.type = query_type
        self.conditions = self.where(**kwargs)
        if order_by:
            self.order_by(order_by)
        else:
            self.order = " "
        self.limit = ()
         
    def where(self, **kwargs):
        '''
        Performs validation that fields are in table before returning a
        dict object that can be used for Query.filter
        '''
        cond = {}
        cond.update(kwargs)
        for k in kwargs:
            if k not in self.model.fields:
                del cond[k]

        return cond
        
    def __getitem__(self, k):
        if self.cache != None:
            return self.cache[k]
        
        if isinstance(k, (int, long)):
            self.limit = (k,1)
            lst = self.get_data()
            if not lst:
                return None
            return lst[0]
        elif isinstance(k, slice):
            if k.start is not None:
                assert k.stop is not None, "Limit must be set when an offset is present"
                assert k.stop >= k.start, "Limit must be greater than or equal to offset"
                self.limit = k.start, (k.stop - k.start)
            elif k.stop is not None:
                self.limit = 0, k.stop
        
        return self.get_data()
        
    def __len__(self):
        return len(self.get_data())
        
    def __iter__(self):
        return iter(self.get_data())
        
    def __repr__(self):
        return repr(self.get_data())
        
    def count(self):
        # TODO: Logic changes query, so further calls do not return model objects.
        # I assume behaviour is supposed return table count if no query
        # conditions are entered
        self.get_data()
        if self.cache is None:
            # self.type = 'SELECT COUNT(*)'
            # return self.execute_query().fetchone()[0]
            return 0
        else:
            return len(self.cache)
        
    def filter(self, **kwargs):
        # get_data() needs to know if query has been executed since
        # conditions was last updated--
        # so I'm changing filter is set self.cache to None
        self.conditions.update(kwargs)
        self.cache = None
        return self
        
    def order_by(self, field=None, direction='ASC', **kwargs):
        '''
        Specify sort criteria: 'column', 'direction', or
        multiple column sorts with key words, e.g.:
        colum_name1='asc', column_name2='desc'
        '''
        
        if field:
            kwargs.update({field: direction})
            
        joinstr = ' ORDER BY'
            
        for field in kwargs:
            direction = kwargs[field].upper()
            field = escape(field)
            self.order = joinstr.join([self.order,
                                       " %s %s" % (field, direction)])
            joinstr = ","
        
        return self

    def mult_order_by(self, **kwargs):
        '''
        sorts by multiple columns, e.g.:
        colum_name1='asc', column_name2='desc'
        '''
        joinstr = 'ORDER BY'
        for kfield in kwargs:
            field = escape(kfield)
            direction = kwargs[kfield].upper()
            self.order = joinstr.join([self.order,
                                       " %s %s" % (field, direction)])
            joinstr = ","
        return self

    def getconditionkey(self, key, value):
        if "(" in key and ")" in key:
            # allows passing functions, e.g., year(author_birthday)
            # note these must be passed to self.conditions manually
            # example:
            # byear = 'year(author_birthday)'
            # qry = Query(model=MyModel)
            # qry.conditions[byear] = 1922
            # looks for authors born in 1922
            n0 = key.find("(")
            n1 = key.find(")")
            fieldname = key[n0+1:n1]
            key = key.replace(fieldname, escape(fieldname))
        else:
            fieldname = key
            key = escape(key)

        datatype = self.model.columnDescriptions[fieldname][0]
        placeholder = sqlPlaceholder(datatype) if value is not None else "%s"
        
        if isinstance(value, tuple):
            return key + value[0].replace("%s", placeholder)
        else:
            return key + "=%s" % placeholder

    def extract_condition_keys(self):
        if len(self.conditions):
            return 'WHERE %s' % ' AND '.join("%s" % (self.getconditionkey(k.strip(),
                                                                          self.conditions[k]), )
                                                for k in self.conditions)

    def extract_condition_values(self):
        it = self.conditions.itervalues()
        return Filter._extractvalues(it)
        
    def query_template(self):
        return '%s FROM %s %s %s %s' % (
            self.type,
            self.model.Meta.table_safe,
            self.extract_condition_keys() or '',
            self.order,
            self.extract_limit() or '',
        )
        
    def extract_limit(self):
        if len(self.limit):
            return 'LIMIT %s' % ', '.join(str(l) for l in self.limit)
        
        
    def extract_sql(self):
        values = self.extract_condition_values()
        sqltext = self.query_template()
        
        if values:
            sqltext = sqltext % tuple(values)
            
        return sqltext
        
    def get_data(self):
        # needs to know if query has been executed since conditions last updated
        # so I'm changing filter is set self.cache to None
        if self.cache is None:
            self.cache = list(self.iterator())
        return self.cache
        
    def iterator(self):
        for row in self.execute_query():
            obj = self.modelClass(*row)
            obj._new_record = False
            yield obj
                    
    def execute_query(self, sqltext=None):
        if not sqltext:
            sqltext = self.extract_sql()
        
        return self.model.dbconn.executeDBQuery(sqltext)
        