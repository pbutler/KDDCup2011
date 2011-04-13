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

class Importer(object):
    def __init__(self, conn, table, max = 50000, parent = None):
        self.conn = conn
        self.table = table
        self.max = max
        self.parent = parent
        self.objs = []

    def add(self, obj):
        self.objs += [ obj ]
        if len(self.objs) >= self.max:
            self.execute()

    def execute(self):
        if not self.objs:
            return
        if self.parent:
                self.parent.execute() #finish(notchildren = True)
        sys.stdout.write("o")
        sys.stdout.flush()
        try:
            self.conn.execute( self.table.insert(), self.objs)
        except Exception, e:
            raise e
        self.objs = []

    def finish(self, notchildren = False):
        if len(self.objs) > 0:
            self.execute()

    def __del__(self):
        self.finish()

def readDatas(dir, opts):
    start = time.time()
    if opts.ratings:
        tRating.drop(checkfirst=True)
    orm.album_genre.drop(checkfirst=True)
    orm.track_genre.drop(checkfirst=True)
    tTrack.drop(checkfirst=True)
    tAlbum.drop(checkfirst=True)
    tArtist.drop(checkfirst=True)
    tGenre.drop(checkfirst=True)
    tUser.drop(checkfirst=True)
    if opts.ratings:
        orm.metadata.drop_all(engine)
    orm.metadata.create_all(engine)
    conn = engine.connect()
    trans = conn.begin()
    print "Artist",
    artistdata = open(os.path.join(dir, "artistData1.txt" ))
    i = Importer(conn, tArtist)
    for line in artistdata:
        id = int(line.strip())
        i.add( {'artist_id': id } )
    i.finish()
    artistdata.close()
    print "."

    print "Genre",
    genredata = open(os.path.join(dir, "genreData1.txt" ))
    i = Importer(conn, tGenre)
    for line in  genredata:
        id = int( line.strip())
        i.add( { 'genre_id': id } )
    i.finish()
    genredata.close()

    print "."
    print "Album",
    albumdata = open(os.path.join(dir, "albumData1.txt" ))
    i = Importer(conn, tAlbum)
    iag = Importer(conn, orm.album_genre, parent=i)
    for line in albumdata:
        row = line.strip().split("|")
        id = int(row[0])
        if row[1] == "None":
            artistid = None
        else:
            artistid = int(row[1])
        i.add( { 'album_id' : id, 'artist_id' : artistid} )
        for g in row[2:]:
            g = int(g)
            iag.add( { 'album_id' : id, 'genre_id' : g})
    i.finish()
    iag.finish()
    del iag
    albumdata.close()

    print "."

    print "Track",
    data = open(os.path.join(dir, "trackData1.txt"))
    i = Importer(conn, tTrack )
    iat = Importer(conn, orm.track_genre, parent=i)
    for line in data:
        row = line.strip().split("|")
        track_id = int(row[0])
        if row[1] == "None":
            album_id = None
        else:
            album_id = int(row[1])

        if row[2] == "None":
            artist_id = None
        else:
            artist_id = int(row[2])
        i.add({'track_id' : track_id, 'artist_id' : artist_id, 'album_id' : album_id} )

        for g in row[3:]:
            g = int(g)
            iat.add ( {'track_id' :track_id, 'genre_id' : g} )
    i.finish()
    iat.finish()
    del iat
    data.close()

    print "."

    if opts.ratings:
        filesandmodels = [ ("trainIdx1*txt", 1),
            ("validationIdx1*txt", 2),
            ("testIdx1*txt", 3),
            ]

        print "Ratings",
        users = {}
        iu = Importer(conn, tUser)
        i = Importer(conn, tRating, parent=iu)
        rid = 0
        ref = datetime.datetime(1990, 1,1)
        for file, type in filesandmodels:
            file = os.path.join(dir, file)
            file = glob.glob(file)[0]
            print file,
            data = open(os.path.join(dir, file))
            for line in data:
                user, nratings = [ int(c) for c in line.strip().split("|") ]
                if user not in users:
                    users[user] = 1
                    iu.add({ 'user_id' : user} )
                for n in range(int(nratings)):
                    line = data.next()
                    if type != 3:
                        item, score, day, timestamp = line.strip().split("\t")
                        score = int(score)
                    else:
                        item, day, timestamp = line.strip().split("\t")
                        score = None
                    hour, minu, sec = timestamp.split(":")
                    dt = ref + datetime.timedelta(int(day))
                    dt = dt.replace(hour = int(hour), minute = int(minu), second = int(sec))
                    rid += 1
                    i.add( {'item_id' : int(item), 'timestamp': dt, 'score': score,
                        'type': type, 'user_id' : user})
        iu.finish()
        i.finish()
        del iu
        print "."
    stop = time.time()

    try:
        trans.commit()
    except Exception, e:
        print e
    conn.close()
    print "Data read in %d seconds" % (stop -start)

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

    if len(args) < 1:
        parser.error("Not enough arguments given")


    readDatas(args[0], options)
    #print len(list(User.select())), len(list(Rating.select()))
    return 0

if __name__ == "__main__":
    import sys
    sys.exit( main( sys.argv ) )
