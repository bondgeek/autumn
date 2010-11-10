        
class Database(object):
    placeholder = '?'
    
    def connect(self, dbtype, *args, **kwargs):
        if dbtype == 'sqlite3':
            import sqlite3
            self.connection = sqlite3.connect(*args)
            self.lastrowid_ = "LAST_INSERT_ROWID()"
        elif dbtype == 'mysql':
            import MySQLdb
            self.connection = MySQLdb.connect(**kwargs)
            self.lastrowid_ = "LAST_INSERT_ID()"
            self.placeholder = '%s'
        elif dbtype == 'pyodbc':
            import pyodbc
            self.connection = pyodbc.connect(**kwargs)
            self.connection.autocommit = True
            self.lastrowid_ = "LAST_INSERT_ID()"
            self.placeholder = '?'
    
    def lastrowid(self):
        idCursor = self.connection.cursor()
        qry = "SELECT %s AS last_row_id" % self.lastrowid_
        idCursor.execute(qry)
        
        rc = idCursor.fetchone()
        
        return rc[0] if rc else None
    
class DBConn(object):
    def __init__(self):
        self.b_debug = False
        self.b_commit = True
        self.conn = None

autumn_db = DBConn()
autumn_db.conn = Database()
