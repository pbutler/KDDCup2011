#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog [options]
Python source code - replace this with a description of the code and write the code below this text.
"""

__author__ = [ 'Patrick Butler', "Depbrakash Patnaik" ]
__email__  = [ 'pabutler@vt.edu', "patnaik@vt.edu" ]

import time
import os
from kddcup2011 import *
from django.db import connection, transaction
from django.db.models.fields.related import ManyToManyField

class Importer(object):
    def __init__(self, model, max = 10000):
        if isinstance(model, ManyToManyField):
            self.tablename = model.m2m_db_table()
            self.fnames = (model.m2m_column_name(), model.m2m_reverse_name())
            self.m2m = True
        else:
            self.m2m = False
            self.tablename = model._meta.db_table
            self.fields = model._meta.fields
            self.fnames = [ f.column for f in self.fields ]
            self.many = {}
            for m2m in model._meta.many_to_many:
                self.many[m2m.name] = Importer(m2m, max)

        self.max = max
        self.objs = []


    def add(self, obj):
        if isinstance(obj, list) or isinstance(obj, tuple):
            self.objs += [ list(obj) ]
        else:
            self.objs += [[  getattr(obj, f) for f in self.fnames ]]
        if len(self.objs) >= self.max:
            self.execute()

    def execute(self):
        c = connection.cursor()
        sql = "INSERT INTO %s "  % ( self.tablename)
        sql += "(" + ",".join([ f for f in self.fnames ]) + ")"
        sql += " VALUES "
        sql += "(" + ",".join([ '%s' for f in self.fnames ]) + ")"
        c.executemany(sql, self.objs)
        transaction.commit_unless_managed()
        self.objs = []

    def finish(self):
        if len(self.objs) > 0:
            self.execute()
        if not self.m2m:
            for i in self.many.values():
                i.finish()

    def __del__(self):
        self.finish()

def readDatas(dir):

    start = time.time()

    print "Artist",
    artistdata = open(os.path.join(dir, "artistData1.txt" ))
    i = Importer(Artist)
    for line in artistdata:
        id = int(line.strip())
        i.add( Artist(id) )
    i.finish()
    artistdata.close()
    print "."

    print "Genre",
    genredata = open(os.path.join(dir, "genreData1.txt" ))
    i = Importer(Genre)
    for line in  genredata:
        id = int( line.strip())
        i.add( Genre(id) )
    i.finish()
    genredata.close()

    print "."
    print "Album",
    albumdata = open(os.path.join(dir, "albumData1.txt" ))
    i = Importer(Album)
    for line in albumdata:
        row = line.strip().split("|")
        id = int(row[0])
        if row[1] == "None":
            artistid = None
        else:
            artistid = int(row[1])
        album = Album(id, artistid)
        if len(row) >= 3 and row[2] != "None":
            genres = map(int, row[2:])
            for g in genres:
                i.many['genres'].add ( (id, g) )
    i.finish()
    albumdata.close()

    print "."

    print "Track",
    data = open(os.path.join(dir, "trackData1.txt"))
    i = Importer( Track )
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
        i.add(Track(track_id, artist_id, album_id) )

        for g in row[3:]:
            g = int(g)
            i.many['genres'].add ( (track_id, g) )
    i.finish()
    data.close()

    print "."
    filesandmodels = [ ("trainIdx1.firstLines.txt", 1),
        ("validationIdx1.firstLines.txt", 2),
        ("testIdx1.firstLines.txt", 3),
        ]
    print "Ratings",
    users = {}
    i = Importer( Rating )
    iu = Importer( User, 1 )
    rid = 0
    for file, type in filesandmodels:
        print file,
        data = open(os.path.join(dir, file))
        for line in data:
            user, nratings = [ int(c) for c in line.strip().split("|") ]
            u = User(user)
            if user not in users:
                users[user] = 1
                iu.add(u)
            for n in range(int(nratings)):
                line = data.next()
                if type != 3:
                    item, score, day, timestamp = line.strip().split("\t")
                    score = int(score)
                else:
                    item, day, timestamp = line.strip().split("\t")
                    score = None
                hour, minu, sec = timestamp.split(":")
                dt = datetime.datetime.min + datetime.timedelta(int(day))
                dt = dt.replace(hour = int(hour), minute = int(minu), second = int(sec))
                rid += 1
                i.add(Rating(rid, int(item), dt, score, type, user, pk=None))
        i.finish()

    iu.finish()
    print "."
    stop = time.time()
    print "Data read in %d seconds" % (stop -start)

def main(args):
    import  optparse
    parser = optparse.OptionParser()
    parser.usage = __doc__
    parser.add_option("-q", "--quiet",
                      action="store_false", dest="verbose", default=True,
                      help="don't print status messages to stdout")
    (options, args) = parser.parse_args()

    if len(args) < 1:
        parser.error("Not enough arguments given")


    readDatas(args[0])
    #print len(list(User.select())), len(list(Rating.select()))
    return 0

if __name__ == "__main__":
    import sys
    sys.exit( main( sys.argv ) )
