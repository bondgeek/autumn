'''
  Connection class
 
  To be invoked from a dbconnection class that contains db=DBConn()
  This class keeps track of what kind of connection you have (sqlite,mysql)
  and has a 'close()' method that is easily accessible.
  
  
    class marketsdb(object):
        db.conn = DBConnection('mysql', db='mydatabase', **mycfg) 
        close = db.conn.close

    class Table(marketsdb, Model):
        "This is now an autumnORM object representing mydatabase.Table" 
        pass

   
'''

from connection import Database, DBConn

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
            kwcfg.update({'db': db})
        
        self.connect(dbtype, dbfile, **kwcfg) 

    def close(self):
        self.connection.close()

class DBConnection(DBConn):
    def __init__(self, dbtype=None, db=None, dbfile=None, **kwcfg):
        DBConn.__init__(self, DBConnector())
        
        self.conn.getconnection(dbtype, db, dbfile, **kwcfg)
        
