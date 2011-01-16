'''
Fork of the autumn ORM

'''

__copyright__ = 'Copyright (c) 2008 Jared Kuolt'

version = (0,5,1)
version_string = "Autumn ORM version %d.%d.%d (with bondgeek modifications)" % version

import sys

if sys.platform == 'cli':
    from autumn.Ipy import Model, DBConnection, Query, Filter, OneToMany, ForeignKey, escape
    
else:
    from model import Model
    from db.dbconnection import DBConnection
    from db.query import Query
    from db.filter import Filter
    from db.relations import OneToMany, ForeignKey
    from db import escape
