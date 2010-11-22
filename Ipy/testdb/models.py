from autumn.Ipy import Model, DBConnection, Query, Filter, OneToMany, ForeignKey

from alprion import AlprionCfg_

cfg = AlprionCfg_.read('database', ("UID", "PWD"))
cfg['DSN'] = 'AutumnTest'

autumnTestConn = DBConnection(**cfg)

autumnTestConn.executeDBNonQuery("truncate books;") # must drop books first, due to foreign key
autumnTestConn.executeDBNonQuery("truncate author;")
autumnTestConn.executeDBNonQuery("truncate transaction;")
print("truncated")

class Author(Model):
    dbconn = DBConnection(**cfg)

    books = OneToMany('Book')
    
class Book(Model):
    dbconn = DBConnection(**cfg)
    
    author = ForeignKey(Author)

    class Meta:
        table = 'books'

class Transaction(Model):
    dbconn = DBConnection(**cfg)
