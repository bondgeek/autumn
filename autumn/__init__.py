'''
Fork of the autumn ORM

'''

__copyright__ = 'Copyright (c) 2008 Jared Kuolt'

version = (0,5,1)
version_string = "Autumn ORM version %d.%d.%d (with bondgeek modifications)" % version

import sys

from model import Model
from db.connection import autumn_db
from db.dbconnection import DBConnection, SQLITE3_Connection
from db.query import Query
from db.filter import Filter
from db.relations import OneToMany, ForeignKey
from db import escape
