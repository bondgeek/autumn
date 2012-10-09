'''
Connection class
 
  This class keeps track of what kind of connection you have (sqlite,mysql, pyodbc)
  and has a 'close()' method that is easily accessible.
  
  Using this class helps deal with multiple database connections.
  
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
import os
from threading import local as threading_local

from connection import Database, DBConn, autumn_db
from autumn.util import AutoConn

class DBConnector(Database):
    
    def getconnection(self, dbtype=None, db=None, dbfile=None, **kwcfg):
        '''return a Database object connected to the database'''
        self.dbtype = dbtype
        if dbtype == 'sqlite3':
            if db is None:
                db = os.path.split(dbfile)[1]
            elif os.path.isdir(dbfile):
                dbfile = os.path.join(dbfile,db)
            else:
                pass # because dbfile better be a good file name
                
        else:
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
        
        

class SQLITE3_Connection(object):
    """
    A container that will automatically create a database connection object
    for each thread that accesses it.  Useful with SQLite, because the Python
    modules for SQLite require a different connection object for each thread.
    
    """
    def __init__(self, db_filename, db_path=None):
        self.b_debug = False
        self.b_commit = True
        self.db_name = os.path.join(db_path, db_filename)
        self.container = threading_local()
        
    def __getattr__(self, name):
        try:
            if "conn" == name:
                return self.container.conn
                
        except BaseException:
            self.container.conn = DBConnector()
            self.container.conn.getconnection('sqlite3', 
                                              dbfile=self.db_name)
            return self.container.conn
            
        raise AttributeError
