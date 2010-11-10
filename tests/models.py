from ..db.connection import autumn_db
from ..model import Model
from ..db.relations import ForeignKey, OneToMany
from .. import validators
import datetime

#autumn_db.conn.connect('sqlite3', '/tmp/example.db')
#autumn_db.conn.connect('mysql', user='root', db='autumn_test')
autumn_db.conn.connect('pyodbc', driver='{MySQL ODBC 5.1 Driver}', server='localhost', database='autumn_test', user='root')

    
class Author(Model):
    books = OneToMany('Book')
    
    class Meta:
        defaults = {'bio': 'No bio available'}
        validations = {'first_name': validators.Length(),
                       'last_name': (validators.Length(), lambda x: x != 'BadGuy!')}
    
class Book(Model):
    author = ForeignKey(Author)
    
    class Meta:
        table = 'books'
