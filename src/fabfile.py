from __future__ import with_statement
from fabric.api import *
from fabric.contrib.console import confirm


__author__ = 'Patrick Butler'
__email__  = 'pbutler@killertux.org'

def reset_db():
    if not confirm("Are you sure?"):
        abort("Not resetting database")
    local("./kddcup2011/manage.py reset data", capture=False)
    local("./kddcup2011/manage.py syncdb", capture=False)

def select_psqltest():
    local("echo psqltest > kddcup2011/.dbrc")
def select_psql2011():
    local("echo psql2011 > kddcup2011/.dbrc")

def select_mysql2011():
    local("echo mysql2011 > kddcup2011/.dbrc")
def select_sqlite():
    local("echo sqlite > kddcup2011/.dbrc")
