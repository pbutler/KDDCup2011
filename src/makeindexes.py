#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog [options]
Python source code - replace this with a description of the code and write the code below this text.
"""

__author__ = [ 'Patrick Butler', "Depbrakash Patnaik" ]
__email__  = [ 'pabutler@vt.edu', "patnaik@vt.edu" ]

import sys
import time
import os
from kddcup2011 import *
#from kddcup2011.data.models import *
import datetime
import glob
from django.db import connection, transaction
from django.db.models.fields.related import ManyToManyField

def makeindexes():
    start = time.time()
    conn = engine.connect()
    trans = conn.begin()
    idx = Index("item_id", tRating.c.item_id)
    idx.create()
    stop = time.time()

    try:
        trans.commit()
    except Exception, e:
        print e
    conn.close()
    print "Index created in %d seconds" % (stop -start)

def main(args):
    import  optparse
    parser = optparse.OptionParser()
    parser.usage = __doc__
    parser.add_option("-q", "--quiet",
                      action="store_false", dest="verbose", default=True,
                      help="don't print status messages to stdout")
    parser.add_option("-r", "--no-ratings",
                      action="store_false", dest="ratings", default=True,
                      help="don't import ratings")
    (options, args) = parser.parse_args()

    if len(args) < 0:
        parser.error("Not enough arguments given")

    makeindexes()
    return 0

if __name__ == "__main__":
    import sys
    sys.exit( main( sys.argv ) )
