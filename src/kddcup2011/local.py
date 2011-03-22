#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et

__author__ = 'Patrick Butler'
__email__  = 'pbutler@killertux.org'

import os

mydir = os.path.dirname(__file__)

DEBUG = True
TEMPLATE_DEBUG = DEBUG

DEFAULTDB = "sqlite"
dbrc = os.path.join(mydir, ".dbrc")

if os.path.exists(dbrc):
    DB = open(dbrc).read().strip()
else:
    DB = DEFAULTDB

DBS = {
        "sqlite" :
          { "engine" : 'django.db.backends.sqlite3',
            "name"   : os.path.join(mydir, "kddcup2011.sqlite"),
            "uri"    : "sqlite:///"+os.path.join(mydir, "kddcup2011.sqlite")},
        "psqltest" :
          { "engine" : 'django.db.backendspostgresql_psycopg2',
            "name"   : "kddcuptest",
            "uri"    : "posgresql://kddcuptest"},
        "psql2011" :
          { "engine" : 'django.db.backendspostgresql_psycopg2',
            "name"   : "kddcup2011",
            "uri"    : "posgresql://kddcup2011"},
      }

DATABASES = {
    'default': {
        'ENGINE': DBS[DB]["engine"],
            #'django.db.backends.sqlite3', # Add 'postgresql_psycopg2', 'postgresql', 'mysql', 'sqlite3' or 'oracle'.
        'NAME': DBS[DB]["name"],
            #os.path.join(mydir,"kddcup2011.sqlite"),
        'USER': '',                      # Not used with sqlite3.
        'PASSWORD': '',                  # Not used with sqlite3.
        'HOST': '',                      # Set to empty string for localhost. Not used with sqlite3.
        'PORT': '',                      # Set to empty string for default. Not used with sqlite3.
    }
}

sadb = DBS[DB]["uri"]
#engine = create_engine('postgresql:///kddcuptest')
#engine = create_engine('sqlite:///:memory:')
#'sqlite:///kddcup2011/kddcup2011.sqlite')
