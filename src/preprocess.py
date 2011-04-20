#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog [options]
Python source code - replace this with a description of the code and write the code below this text.
"""

__author__ = 'Patrick Butler'
__email__  = 'pbutler@killertux.org'

import itertools
import time
from kddcup2011 import *
import sys
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
    s = select( [tRating.c.user_id, tRating.c.item_id, tRating.c.score] ).where( and_(tRating.c.type == type,
        tRating.c.user_id.in_(userids),
        tRating.c.item_id > i)).order_by(tRating.c.item_id)
    return [a for a in conn.execute(s) ]

def main(args):
    import  optparse
    global conn
    parser = optparse.OptionParser()
    parser.usage = __doc__
    parser.add_option("-q", "--quiet",
                      action="store_false", dest="verbose", default=True,
                      help="don't print status messages to stdout")
    (options, args) = parser.parse_args()
    if len(args) < 0:
        parser.error("Not enough arguments given")

    conn = engine.connect()
    s = select([tRating.c.item_id]).where(tRating.c.type==1).group_by(tRating.c.item_id) #.having(func.count() >1))
    result = conn.execute(s)
    print s
    print "Start"
    start = time.time()
    items = [ int(r) for r, in result ]
    items.sort()
    done = {}
    print len(items)
    cnt = 0
    for i in items:
        cnt += 1
        ratings = get_item_rating(i, 1)
        userids = ratings.keys()
        results = get_all_users_ratings(userids, 1, i)
        sys.stderr.write("%s %s %s\n" % (i,cnt,len(results) ))
        for j, g in itertools.groupby(results, lambda x: x[1]):
            g = list(g)
            n = float(len(g))
            abar = sum([ ratings[uid]*score for uid, jj, score in g]) / n
            print i,j,abar


#        sys.stderr.write(">"); sys.stderr.flush()
#        users = get_users_rating(i, 1)
#        sys.stderr.write(">"); sys.stderr.flush()
#        subitems =[ j for j in  get_items_users_rate(users, 1) if j > i]
#        sys.stderr.write("%s" % len(subitems))
#        for j in subitems:
#            s = select( [ func.max(tRating.c.score),
#                func.min(tRating.c.score)  ] ).where( and_(tRating.c.type==1,
#                 #   tRating.c.score != None,
#                or_(tRating.c.item_id==i, tRating.c.item_id==j))
#                ).group_by(tRating.c.user_id).having(func.count() == 2)
#            result = conn.execute(s).fetchall()
#            l = len(result)
#            lf = float(l)
#            #print i, j, sum([ a*b for a,b in result]) / lf
#        print
    stop = time.time()
    print "done", stop-start
    return 0

if __name__ == "__main__":
    import sys
    sys.exit( main( sys.argv ) )


