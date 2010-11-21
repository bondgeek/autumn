'''
Atumn ORM for IronPython--inspired by, and stealing freely from, autumn

Adapts MySQLdb database calls to .NET 

'''

from autumn.Ipy.dbconnection import DBConnection, escape, unescape, sqlPlaceholder
from autumn.Ipy.dbconnection import OdbcConnection, OdbcDataAdapter, OdbcCommand
from autumn.Ipy.dbconnection import DataSet, ConnectionState

from autumn.Ipy.validators import ValidatorChain
from autumn.Ipy.query import Query

class ModelCache(object):
    models = {}
    
    def add(self, model):
        self.models[model.__name__] = model
        
    def get(self, model_name):
        return self.models[model_name]
   
cache = ModelCache()

class Empty(object):
    pass

class ModelBase(type):
    '''
    Metaclass for Model
    Sets up default table name and primary key
    Adds fields from table as attributes
    Creates ValidatorChains as necessary
    '''
    def __new__(cls, name, bases, attrs):
        if name == 'Model':
            return super(ModelBase, cls).__new__(cls, name, bases, attrs)

        new_class = type.__new__(cls, name, bases, attrs)
        
        if not getattr(new_class, "dbconn", None):
            raise StandardError, "no database connection for %s" % name.lower()
        new_class.connection = new_class.dbconn.connection
        
        if not getattr(new_class, 'Meta', None):
            new_class.Meta = Empty()
        
        if getattr(new_class.Meta, 'table', None):
            name = new_class.Meta.table
        else:
            name = name.lower()
        new_class.Meta.table_safe = escape(name)
        
        # Assume id is the default
        if not getattr(new_class.Meta, 'pk', None):
            new_class.Meta.pk = 'id'
        
        # Create function to loop over iterable validations
        for k, v in getattr(new_class.Meta, 'validations', {}).iteritems():
            if isinstance(v, (list, tuple)):
                new_class.Meta.validations[k] = ValidatorChain(*v)
        
        # Get field names and data types for columns in table
        if getattr(new_class, "connection", None):
            qry = new_class.Meta.table_safe.join(["SELECT * FROM ", 
                                                  " LIMIT 1;"])

            adaptor = OdbcDataAdapter(qry, new_class.connection)
            fields = DataSet()
            
            new_class.connection.Open()
            adaptor.Fill(fields)
            new_class._fields = [c.ColumnName for c in fields.Tables[0].Columns]
            new_class._colDescription = dict([(c.ColumnName,
                                              (c.DataType.FullName, 
                                               c.AllowDBNull, c.Unique)) 
                                              for c in fields.Tables[0].Columns])
            
            assert new_class.Meta.pk in new_class._colDescription, "\nPrimary Key: %s not in database\n" % new_class.Meta.pk
            
            new_class.connection.Close()

        cache.add(new_class)
        return new_class

class Model(object):
    '''
    Base class for a given table. 
    
    class Author(Model):
        dbconn = DBConnection( someGoodConnectionString )
    
    ModelBase creates some attributes that are defined as properties in Model:
    - columnDescriptions:  a dictionary object {fieldName: (datatype, allowNulls, Unique
    '''
    __metaclass__ = ModelBase
    connection = None
    
    def __init__(self, *args, **kwargs):
        '''
        Allows setting of fields using kwargs
        '''
        self.__dict__[self.Meta.pk] = None
        
        self._new_record = True
        
        self._changed = set()        
        [setattr(self, k, v) for k, v in kwargs.iteritems()]
        [setattr(self, self._fields[i], arg) for i, arg in enumerate(args)]

                
    def close(self):
        self._connection.Close()

    @classmethod
    def tablename(cls):
        return unescape(cls.Meta.table_safe)
    
    @property
    def fields(self):
        return self._fields 
        
    @property
    def columnDescriptions(self):
        return self._colDescription
    
    def getPlaceholder(self, field):
        '''
        .NET Data framework requires explicit placing of quotes around 
        field values in query statements--but that doesn't work if
        the value is NULL.
        
        '''
        placeholder = sqlPlaceholder( self.columnDescriptions[field][0] )
        nullplaceholder = self.dbconn.placeholder
        v = getattr(self, field, None)
        
        return placeholder if v is not None else nullplaceholder
        
    def __setattr__(self, name, value):
        '''Records when fields have changed
        Lets you add fields, which obviously don't affect database.
        
        '''
        if name != '_changed' and name in self._fields and hasattr(self, '_changed'):
            self._changed.add(name)
        self.__dict__[name] = value
        
    def _get_pk(self):
        'Sets the current value of the primary key'
        return getattr(self, self.Meta.pk, None)

    def _set_pk(self, value):
        'Sets the primary key'
        return setattr(self, self.Meta.pk, value)
    
    def _update(self):
        '''
        Uses SQL UPDATE to update record
        
        '''
        
        values = [getattr(self, f, None) for f in self._changed]
        values.append(self._get_pk())
        
        query = 'UPDATE %s SET ' % self.Meta.table_safe
        query += ', '.join(['%s = %s' % (escape(f), self.getPlaceholder(f)) 
                            for f in self._changed])
                            
        query += ' WHERE %s = %s ' % (escape(self.Meta.pk), 
                                      self.getPlaceholder(self.Meta.pk))
        
        query = query % tuple(values)

        rc = self.dbconn.executeDBNonQuery(query)
        return rc
    
    def _new_save(self, duplicateKey="IGNORE"):
        'Uses SQL INSERT to create new record'
        # if pk field is set, we want to insert it too
        # if pk field is None, we want to auto-create it from lastrowid
        auto_pk = 1 and (self._get_pk() is None) or 0
        
        fields = []
        values = []
        placeholders = []
        for f in self._fields:
            if f != self.Meta.pk or not auto_pk:
                v = getattr(self, f, None)
                
                values.append(v)
                fields.append(escape(f))
                placeholders.append(self.getPlaceholder(f))
        
        # get sql syntax for "REPLACE", "IGNORE", "FAIL" 
        # treatment of duplicate keys
        insertsql = self.dbconn.insert.get(duplicateKey,
                                           self.dbconn.insert['FAIL'])
                                       
        query = insertsql % (
               self.Meta.table_safe,
               ', '.join(fields),
               ', '.join(placeholders)
        )
        
        query = query % tuple([x if x is not None else "NULL" for x in values])
        
        try:
            self.dbconn.executeDBNonQuery(query, CloseIt=False)
        except:
            print("\nInsert Error for query: %s\n" % query)
            return False
            
        else:
            if self._get_pk() is None:
                lastrowid = self.dbconn.executeDBQuery("SELECT LAST_INSERT_ID();")[0][0]
                self._set_pk(lastrowid)
        
        return True    
        
    def _get_defaults(self):
        'Sets attribute defaults based on ``defaults`` dict'
        for k, v in getattr(self.Meta, 'defaults', {}).iteritems():
            if not getattr(self, k, None):
                if callable(v):
                    v = v()
                setattr(self, k, v)
        
    def delete(self):
        'Deletes record from database'
        query = 'DELETE FROM %s WHERE %s = %s' % (self.Meta.table_safe, 
                                                  self.Meta.pk, 
                                                  self.getPlaceholder(self.Meta.pk))
        values = [getattr(self, self.Meta.pk)]
        query = query % tuple(values)

        try:
            self.dbconn.executeDBNonQuery(query)
        except:
            return False        
        
        return True
        
    def is_valid(self):
        'Returns boolean on whether all ``validations`` pass'
        try:
            self._validate()
            return True
        except Model.ValidationError:
            return False
    
    def _validate(self):
        '''Tests all validations, raises Model.ValidationError
        
        '''
        for k, v in getattr(self.Meta, 'validations', {}).iteritems():
            assert callable(v), 'The validator must be callable'
            value = getattr(self, k)
            if not v(value):
                raise Model.ValidationError, 'Improper value "%s" for "%s"' % (value, k)
        
    def save(self, duplicateKey="IGNORE"):
        'Sets defaults, validates and inserts into or updates database'
        self._get_defaults()
        self._validate()
        
        if self._new_record:
            rc = self._new_save(duplicateKey)
            self._new_record = False
            return rc
            
        else:
            return self._update()
    
    def merge(self, *field_args, **condition_kwgs):
        '''
        Update if match, else insert--match by field names or by conditions.
        Returns number of rows affected.
        '''
        self._get_defaults()
        self._validate()
        for arg in field_args:
            if arg in self.fields:
                condition_kwgs[arg] = getattr(self, arg, None)
                
        if not condition_kwgs:
            qry = [self.get(self.Meta.pk)] if self.id else None
        else:
            qry = self.get(**condition_kwgs)
        
        if not qry:
            self._new_save("REPLACE")
            self._new_record = False
            rcount = 1
        else:
            rcount = 0
            for row in qry:
                for f in self.fields:
                    if f != self.Meta.pk:
                        setattr(row, f, getattr(self, f, None))
                if row._update():
                    rcount += 1
        return rcount

    @classmethod
    def get(cls, _obj_pk=None, **kwargs):
        'Returns Query object'
        if _obj_pk is not None:
            return cls.get(**{cls.Meta.pk: _obj_pk})[0]

        return Query(model=cls, **kwargs)  
        