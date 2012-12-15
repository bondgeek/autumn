# autumn.util.py
import os
import shutil
import glob
import time

import sqlite3

from threading import local as threading_local

# Autumn ORM

from .db.query import Query
from .db.connection import Database


"""
Convenience functions for the Autumn ORM.
"""
def sqlite_execute(sqltext, dbname, fetch_all=False):
    
    if not os.path.isfile(dbname):
        print("{}: file does not exist".format(dbname))
        return None
        
    cnxn = sqlite3.connect(dbname)
    cur = cnxn.cursor()
    
    cur.execute(sqltext)
    
    rc = cur.fetchall() if fetch_all else cur.description    
        
    cnxn.commit()
    cnxn.close()
    
    return rc
    
def table_exists(db, table_name):
    """
    Given an Autumn model, check to see if its table exists.
    """
    try:
        s_sql = "SELECT * FROM %s LIMIT 1;" % table_name
        Query.raw_sql(s_sql, db=db)
    except Exception:
        return False

    # if no exception, the table exists and we are done
    return True


def create_table(db, s_create_sql):
    """
    Create a table for an Autumn class.
    """
    Query.begin(db=db)
    Query.raw_sqlscript(s_create_sql, db=db)
    Query.commit(db=db)


def create_table_if_needed(db, table_name, s_create_sql):
    """
    Check to see if an Autumn class has its table created; create if needed.
    """
    if not table_exists(db, table_name):
        create_table(db, s_create_sql)


def sqlite3_dbcopy(source_file, dest_file):
    cnxn = sqlite3.connect (source_file)
    
    cur = cnxn.cursor ()
    cur.execute('begin immediate' )

    shutil.copyfile(source_file, dest_file)
    
    try:
        cur.execute('rollback')
    except:
        pass
    
    cnxn.close()
    
    return dest_file
    
    
def sqlite3_dbbackup (db_filepath, backup_dir):
    """
    Create a backup of sqlite3 database specified by db_filepath
    
    """
    if not os.path.isdir(backup_dir):
        msg = "backup directory does not exist: {}".format(backup_dir)
        raise Exception(msg)
    
    if not os.path.isfile(db_filepath):
        msg = "database file does not exist: {}".format(db_filepath)
        raise Exception(msg)
    
    backupname = os.path.basename(db_filepath) + time.strftime(".%Y%m%d-%H%M")
    backupfile = os.path.join(backup_dir, backupname)
    
    return sqlite3_dbcopy(db_filepath, backupfile)
    
def sqlite3_dbrestore(dbfile, backup_dir, restore_dir):
    """
    Restore a backup of sqlite3 database specified by dbfile 
    from backup_dir to restore_dir.
    
    """
    if not os.path.isdir(backup_dir):
        msg = "backup directory does not exist: {}".format(backup_dir)
        raise Exception(msg)
    
    if not os.path.isdir(restore_dir):
        msg = "backup directory does not exist: {}".format(restore_dir)
        raise Exception(msg)
        
    glob_file = ".".join((dbfile, "*"))
        
    glob_file = os.path.join(backup_dir, glob_file)
    dest_file = os.path.join(restore_dir, dbfile)
    
    files = glob.glob(glob_file)
    files.sort(reverse=True)
    
    assert files, "{} not found".format(glob_file)
    backup_file = files[0]
    
    return sqlite3_dbcopy(backup_file, dest_file)


class AutoConn(object):
    """
    A container that will automatically create a database connection object
    for each thread that accesses it.  Useful with SQLite, because the Python
    modules for SQLite require a different connection object for each thread.
    """
    def __init__(self, db_name, container=None):
        self.b_debug = False
        self.b_commit = True
        self.db_name = db_name
        self.container = threading_local()
    def __getattr__(self, name):
        try:
            if "conn" == name:
                return self.container.conn
        except BaseException:
            self.container.conn = Database()
            self.container.conn.connect('sqlite3', self.db_name)
            return self.container.conn
        raise AttributeError


# examples of usage:
#
# class FooClass(object):
#     db = autumn.util.AutoConn("foo.db")
#
# _create_sql = "_create_sql = """\
# DROP TABLE IF EXISTS bar;
# CREATE TABLE bar (
#     id INTEGER PRIMARY KEY,
#     value VARCHAR(128) NOT NULL,
#     UNIQUE (value));
# CREATE INDEX idx_bar0 ON bar (value);"""
#
# autumn.util.create_table_if_needed(FooClass.db, "bar", _create_sql)
#
# class Bar(FooClass, Model):
#    ...standard Autumn class stuff goes here...
