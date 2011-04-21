#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog [options]
Python source code - replace this with a description of the code and write the code below this text.
"""

__author__ = 'Patrick Butler'
__email__  = 'pbutler@killertux.org'

import heapq
import itertools
import time
import sys

from kddcup2011 import *

PRUNETIME = 10
def get_users_rating(itemid, type):
    s = select( [tRating.c.user_id] ).where( and_(tRating.c.type == type,
        tRating.c.item_id == itemid))
    return [userid for userid,  in conn.execute(s) ]


def get_item_rating(itemid, type):
    s = select( [tRating.c.user_id, tRating.c.score] ).where( and_(tRating.c.type == type,
        tRating.c.item_id == itemid))
    return dict([a for a  in conn.execute(s) ])

#def get_items_users_rate(userids, type, i):
#    s = select( [func.distinct(tRating.c.item_id)] ).where( and_(tRating.c.type == type,
#        tRating.c.user_id.in_(userids), tRating.c.score != None,
#        tRating.c.item_id > i))
#    return [itemid for itemid,  in conn.execute(s) ]

def get_all_users_ratings(userids, type, i):
    s = select( [tRating.c.item_id, tRating.c.user_id, tRating.c.score] ).where( and_(tRating.c.type == type,
        tRating.c.user_id.in_(userids),
        tRating.c.item_id > i)).order_by(tRating.c.user_id, tRating.c.item_id)
    return [a for a in conn.execute(s) ]

class RatingCache(object):
    def __init__(self, connect):
        self.conn = connect
        self.data = {}
        self.max = 100
        self.lastprune = 0

    def prune(self, keep, mini):
        keepers = dict( zip(keep, range(len(keep))))
        for key in self.data.keys():
            if key not in keepers:
                del self.data[key]
            else:
                self.data[key] = [ (i,u,s) for i,u,s in self.data[key] if i > mini ]
    def relax(self, mini):
        for key in self.data.keys():
            self.data[key] = [ (i,u,s) for i,u,s in self.data[key] if i > mini ]

    def get_users(self, users, type, mini):
        self.lastprune += 1
        if len(self.data) > self.max:
            self.lastprune = 0
            self.prune(users)
        if self.lastprune >= PRUNETIME:
            self.lastprune = 0
            self.relax(mini)

        newusers = [ user for user in users if user not in self.data ]
        if len(newusers) > 0:
            newratings = get_all_users_ratings(newusers, type, mini)
            #sys.stderr.write("*%d/%d*\n" % (len(newusers), len(users)))
            for user, g in itertools.groupby(newratings, lambda x: x[1]):
                self.data[user] = list(g)
            for user in newusers:
                if user not in self.data:
                    self.data[user] =[]
        listiters = [ ( (i,u,s) for i,u,s in self.data[user] if i > mini) for user in users ]
        return heapq.merge(*listiters)


def main(args):
    import  optparse
    global conn, PRUNETIME
    parser = optparse.OptionParser()
    parser.usage = __doc__
    parser.add_option("-q", "--quiet",
                      action="store_false", dest="verbose", default=True,
                      help="don't print status messages to stdout")
    parser.add_option("-p", "--prunetime",
                      action="store", dest="prune", type=int, default=10,
                      help="don't print status messages to stdout")
    (options, args) = parser.parse_args()
    if len(args) < 0:
        parser.error("Not enough arguments given")

    PRUNETIME = options.prune
    conn = engine.connect()
    start = time.time()
    Abar()
    stop = time.time()
    print "done", stop-start

def Abar():
    s = select([tRating.c.item_id]).where(tRating.c.type==1).group_by(tRating.c.item_id) #.having(func.count() >1))
    result = conn.execute(s)
    print s
    print "Start"
    items = [ int(r) for r, in result ]
    items.sort()
    done = {}
    print len(items)
    cnt = 0
    rc = RatingCache(conn)
    for i in items:
        cnt += 1
        ratings = get_item_rating(i, 1)
        userids = ratings.keys()
        #sys.stderr.write("%s %s\n" % (i,cnt))
        results = rc.get_users(userids, 1, i)
        for j, g in itertools.groupby(results, lambda x: x[0]):
            g = list(g)
            n = float(len(g))
            abar = sum([ ratings[uid]*score for jj, uid, score in g]) / n
            print i,j,abar



if __name__ == "__main__":
    import sys
    sys.exit( main( sys.argv ) )


