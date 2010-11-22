'''
Database and connection string configurations
'''
try:
    from System.Data.Odbc import OdbcConnection, OdbcDataAdapter, OdbcCommand
except:
    from resolverlib import *
    from System.Data.Odbc import OdbcConnection, OdbcDataAdapter, OdbcCommand
finally:
    from System.Data import DataSet, ConnectionState

def escape(string):
    '''
    Use to make sure table names are sql safe.
    '''
    return '`%s`' % string

def unescape(string):
    return string.strip('`')
    
def sqlPlaceholder(datatype):
    if (datatype == 'System.String' or
       datatype == 'System.DateTime' or 
       datatype == 'System.TimeSpan' or
       datatype == str):
        return '"%s"'
    
    return '%s'
  
class DBConnection(object):
    def __init__(self, **kwargs):
        defaultKeys = {'UID': None,
                       'PWD': None,
                       'DSN': None}
        defaultErrorString = "DBConnection:  Value for %s must be provided"
        
        # if parameters are provided, use them,
        # if one of the default keys is not provided, use the default
        self._connectionKeys = kwargs
        for k in defaultKeys:
            val = defaultKeys.get(k, None)
            if not val:
                assert k in kwargs, defaultErrorString % k
            self._connectionKeys[k] = kwargs.get(k, val)
        
        self._setConnectionString()
        self._connection = OdbcConnection(self.connectionString)
        self.placeholder = '%s'
        self.insert = {"REPLACE": "REPLACE INTO %s (%s) VALUES (%s)",
                       "IGNORE": "INSERT IGNORE INTO %s (%s) VALUES (%s)",
                        "FAIL": "INSERT INTO %s (%s) VALUES (%s)"}
        
    def _setConnectionString(self):
        self._connectionString = ";".join(["=".join((k, self._connectionKeys[k])) 
                                          for k in self._connectionKeys])
    def _getConnectionString(self):
        '''
        TODO: mask the password. 
        '''
        return self._connectionString

    def _getConnection(self):
        return self._connection
        
    def executeDBNonQuery(self, sql, CloseIt=True):
        '''
        Executes a non-query command against the database
        '''
        command = OdbcCommand(sql, self.connection)
        if not (self.connection.State == ConnectionState.Open):
            self.connection.Open()
        rc = command.ExecuteNonQuery()
        
        if CloseIt:
            self.connection.Close()
        return rc
    
                        
    def executeDBQuery(self, sql, rowsAsDict=None):
        '''
        Executes a query against the database, return the results in a list,
        mimicking the fetchall feature of MySQLdb.
        
        TODO: could use "GetDataTypeName" methods to return formating strings.
        '''
        reader = self.getReader(sql)
        
        fetchall = []
        while reader.Read():
            row = tuple([reader.GetValue(n) for n in range(reader.FieldCount)])
            if rowsAsDict:
                rowNames = tuple([reader.GetName(n) for n in range(reader.FieldCount)])
                row = dict(zip(rowNames, row))
            fetchall.append(row)
        
        #clean-up connections
        reader.Close()
        self.connection.Close()
        
        return fetchall

    def getReader(self, sql):
        '''
        Executes a non-query command against the database.  User must close the reader
        when finished.
        '''
        command = OdbcCommand(sql, self.connection)
        if not (self.connection.State == ConnectionState.Open):
            self.connection.Open()
        reader = command.ExecuteReader()

        return reader
        
    connectionString = property(_getConnectionString, _setConnectionString)    
    connection = property(_getConnection)
