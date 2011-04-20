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

def maketypes():
    start = time.time()
    conn = engine.connect()

    conn.execute( tRating.update().values(type=1).where(tRating.c.item_id.in_(select([tTrack.c.track_id]))))
    conn.execute( tRating.update().values(type=2).where(tRating.c.item_id.in_(select([tArtist.c.artist_id]))))
    conn.execute( tRating.update().values(type=3).where(tRating.c.item_id.in_(select([tAlbum.c.album_id]))))
    conn.execute( tRating.update().values(type=4).where(tRating.c.item_id.in_(select([tGenre.c.genre_id]))))

    conn.execute( tRatingT.update().values(type=1).where(tRatingT.c.item_id.in_(select([tTrack.c.track_id]))))
    conn.execute( tRatingT.update().values(type=2).where(tRatingT.c.item_id.in_(select([tArtist.c.artist_id]))))
    conn.execute( tRatingT.update().values(type=3).where(tRatingT.c.item_id.in_(select([tAlbum.c.album_id]))))
    conn.execute( tRatingT.update().values(type=4).where(tRatingT.c.item_id.in_(select([tGenre.c.genre_id]))))


    conn.execute( tRatingV.update().values(type=1).where(tRatingV.c.item_id.in_(select([tTrack.c.track_id]))))
    conn.execute( tRatingV.update().values(type=2).where(tRatingV.c.item_id.in_(select([tArtist.c.artist_id]))))
    conn.execute( tRatingV.update().values(type=3).where(tRatingV.c.item_id.in_(select([tAlbum.c.album_id]))))
    conn.execute( tRatingV.update().values(type=4).where(tRatingV.c.item_id.in_(select([tGenre.c.genre_id]))))
    stop = time.time()

    print "Index created in %d seconds" % (stop -start)

def makeindexes():
    start = time.time()
    conn = engine.connect()
    trans = conn.begin()
    maketypes()
    idx = Index("item_id_type", tRating.c.item_id, tRating.c.type)
    idx.create()
    idx = Index("user_id_type", tRating.c.user_id, tRating.c.type)
    idx.create()


    idx = Index("item_id_typeV", tRatingV.c.item_id, tRatingV.c.type)
    idx.create()
    idx = Index("user_id_typeV", tRatingV.c.user_id, tRatingV.c.type)
    idx.create()

    idx = Index("item_id_typeT", tRatingT.c.item_id, tRatingT.c.type)
    idx.create()
    idx = Index("user_id_typeT", tRatingT.c.user_id, tRatingT.c.type)
    idx.create()
    conn.execute( "INSERT INTO data_user %s" %
            (select([func.distinct(tRating.c.user_id)])))
    try:
        trans.commit()
    except Exception, e:
        print e
    conn.close()
    stop = time.time()
    print "Index created in %d seconds" % (stop -start)

def main(args):
    import  optparse
    parser = optparse.OptionParser()
    parser.usage = __doc__
    parser.add_option("-q", "--quiet",
                      action="store_false", dest="verbose", default=True,
                      help="don't print status messages to stdout")
    parser.add_option("-m", "--make-types",
                      action="store_false", dest="all", default=True,
                      help="don't import ratings")
    (options, args) = parser.parse_args()

    if len(args) < 0:
        parser.error("Not enough arguments given")

    if not options.all:
        maketypes()
    else:
        makeindexes()
    return 0

if __name__ == "__main__":
    import sys
    sys.exit( main( sys.argv ) )
