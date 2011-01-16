'''
Connection class
 
  This class keeps track of what kind of connection you have (sqlite,mysql, pyodbc)
  and has a 'close()' method that is easily accessible.
  
*dbtype* 
    Values 'sqlite3', 'mysql', 'pyodbc'.  'mysql' uses the MySQLdb package to connect.  
    'pyodbc' supports the use of connection strings and
    provides more cross platform interoperability.         

Example
    
    mycfg = {'DSN': myODBC_DSN, 'UID': myUID, 'PWD': myPWD}

    class marketsdb(object):
        db = DBConnection('pyodbc', db='mydatabase', **mycfg) 
        
    class Table(marketsdb, Model):
        "This is now an autumnORM object representing mydatabase.Table" 
        pass

N.B.:  If you use this to create a connection, any Query.raw_sql call MUST specify
the db connection.  Using the above example:
    
    Query.raw_sql("select * from mytable", db=marketsdb.db)
   
'''

from connection import Database, DBConn, autumn_db

class DBConnector(Database):
    
    def getconnection(self, dbtype=None, db=None, dbfile=None, **kwcfg):
        '''return a Database object connected to the database'''
        self.dbtype = dbtype
        if dbtype == 'sqlite3':
            import os
            if db is None:
                db = os.path.split(dbfile)[1]
            elif os.path.isdir(dbfile):
                dbfile = os.path.join(dbfile,db)
            else:
                pass # because dbfile better be a good file name
        self.dbname = db
        if 'user' in kwcfg:
            self.user = kwcfg['user']
        else:
            self.user = None
        
        if db:
            kwcfg.update({'DATABASE': db})
        
        self.connect(dbtype, dbfile, **kwcfg) 

    def close(self):
        self.connection.close()

class DBConnection(DBConn):
    def __init__(self, dbtype=None, db=None, dbfile=None, **kwcfg):
        global autumn_db
        
        DBConn.__init__(self, DBConnector())
        
        self.conn.getconnection(dbtype, db, dbfile, **kwcfg)
        
        autumn_db = self
        
        
