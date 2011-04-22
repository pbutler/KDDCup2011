#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog [options]
Python source code - replace this with a description of the code and write the code below this text.
"""

__author__ = 'Patrick Butler'
__email__  = 'pbutler@killertux.org'

import os
import numpy as np
cimport numpy as np
import cPickle as pickle

user_t   = [('count', np.int), ('sum', int)]
track_t  = [('id', np.int), ('count', np.int ), ('sum', np.int ), 
            ('avg', np.float ), ('pavg', np.float)]
rating_t = [('user', np.int32), ('track', np.int32), ('rating', np.uint8),
        ('cache', np.int32)]

cdef packed struct user_s:
    np.int_t count
    np.int_t sum

cdef packed struct track_s:
    np.int_t id
    np.int_t count
    np.int_t sum
    np.float_t avg
    np.float_t pavg

cdef packed struct rating_s:
    np.int32_t user
    np.int32_t track
    np.uint8_t rating
    np.int32_t cache


INIT = 0.1

class SVD(object):
    def __init__(self, dir, nFeatures = 10):
        cdef int tidx, ridx, uidx, i, id
        self.dir = dir
        self.tmap = {}
        stats = open(os.path.join(dir, "info.txt")).readlines()
        stats = [ x.strip().split("=") for  x in stats]
        stats = dict( [ (k,int(v)) for k,v in stats] )
        self.stats = stats

        cdef np.ndarray[user_s, ndim=1] users  = np.ndarray(stats['nUsers'], dtype=user_t)
        cdef np.ndarray[track_s, ndim=1] tracks = np.ndarray(stats['nTracks'], dtype=track_t)
        cdef np.ndarray[rating_s,ndim=1] ratings=np.ndarray(stats['nRatings'], dtype=rating_t)
        trackFile = open(os.path.join(dir, "trackData1.txt"))
        tidx = 0
        for line in trackFile:
            data = line.strip().split("|")
            t = int(data[0])
            tracks[tidx].id = t
            tracks[tidx].sum = 0
            tracks[tidx].count = 0
            self.tmap[t] = tidx
            tidx += 1
        trackFile.close()

        self.nratings = 0
        trainFile = open(os.path.join(dir, "trainIdx1.txt"))
        uidx = 0
        ridx = 0
        cdef int a
        for line in trainFile:
            u, n = [ int(x) for x in line.split("|")  ]
            a = 0
            for i in range(n):
                line = trainFile.next()
                sid, score, day, time = line.strip().split("\t")
                id = int(sid)
                score = int(score)
                if id not in self.tmap:
                    n -= 1
                    continue
                a += score
                id = self.tmap[id]
                ratings[ridx].user = uidx
                ratings[ridx].track = id
                ratings[ridx].rating = score
                tracks[id].count += 1
                tracks[id].sum += score
                ridx += 1
            if n == 0:
                continue
            users[uidx].count = n
            users[uidx].sum = a 
            uidx += 1
        trainFile.close()
        users = np.resize(users, uidx)
        ratings = np.resize(ratings, ridx)
        cdef float ntrack
        cdef float  sumtrack
        for i in range(len(tracks)):
            ntrack = float(tracks[i].count)
            sumtrack  = float(tracks[i].sum)
            tracks[i].avg = 0
            if n > 0:
                tracks[i].avg  = sumtrack / n
            tracks[i].pavg = ( float( 50*25 + sumtrack) / (25 + sumtrack))

        self.users = users
        self.tracks = tracks
        self.ratings = ratings
        self.initFeatures(nFeatures)
        self.save()

    def initFeatures(self, nFeatures):
        self.nFeatures = nFeatures
        nUsers = self.stats['nUsers']
        nTracks = self.stats['nTracks']
        self.userFeatures = np.zeros(shape=(nFeatures, nUsers),
                dtype=np.float)
        self.trackFeatures = np.zeros(shape=(nFeatures, nTracks),
                dtype=np.float)
        self.userFeatures  += INIT
        self.trackFeatures += INIT

    def __getstate__(self):
        odict = self.__dict__.copy()
        del odict['ratings']
        del odict['tracks']
        del odict['users']
        del odict['userFeatures']
        del odict['trackFeatures']
        self.ratings.flush()
        self.tracks.flush()
        self.users.flush()
        return odict

    def __setstate__(self, dict):
        self.__dict__.update(dict)

    def loadmmaps(self):
        self.users = np.memmap(os.path.join(self.dir, "user.mmap"), dtype=user_t)
        self.ratings = np.memmap(os.path.join(self.dir, "rating.mmap"), dtype=rating_t)
        self.tracks = np.memmap(os.path.join(self.dir, "track.mmap"), dtype=track_t)

    def save(self):
        mmap = np.memmap(os.path.join(self.dir, "rating.mmap"),
                dtype=rating_t, shape=self.ratings.shape, mode="w+")
        mmap[:] = self.ratings[:]
        self.ratings = mmap
        mmap = np.memmap(os.path.join(self.dir, "user.mmap"),
                dtype=user_t, shape=self.users.shape, mode="w+")
        mmap[:] = self.users[:]
        self.users = mmap
        mmap = np.memmap(os.path.join(self.dir, "track.mmap"),
                dtype=track_t, shape=self.tracks.shape, mode="w+")
        mmap[:] = self.tracks[:]
        self.tracks = mmap

    @classmethod
    def load(cls, dir, nFeatures = 10):
        pklfile = os.path.join(dir, "cache")
        svd = pickle.load(open(pklfile))
        svd.dir = dir
        svd.loadmmaps()
        svd.initFeatures(nFeatures)
        return svd

    def dump(self):
        pickle.dump(self, open(os.path.join(self.dir, "cache"), "w"), -1)

    def train_all(self, nepochs = 10):
        shortPredict = self.shortPredict
        cdef np.ndarray[rating_s, ndim=1] ratings = self.ratings
        #ratings = self.ratings
        cdef int f
        cdef int e
        cdef int r
        cdef np.int_t u
        cdef np.int_t t
        cdef np.int_t s
        cdef np.int_t c
        cdef np.float_t p
        cdef np.float_t sq
        cdef np.float_t err
        cdef np.float_t uf
        cdef np.float_t tf
        #cdef rating_struct rating
        cdef np.ndarray[np.float_t, ndim=2] userFeatures = self.userFeatures
        cdef np.ndarray[np.float_t, ndim=2] trackFeatures = self.trackFeatures
        for f in range(self.nFeatures):
            print "Training Feature %d" % f
            for e in range(nepochs):
                sq = 0.
                for r in range(len(ratings)):
                    u = ratings[r].user
                    t = ratings[r].track
                    s = ratings[r].rating
                    c = ratings[r].cache
                    p = shortPredict(u, t, f, c, True)
                    err =  s - p
                    sq += err**2
                    uf = userFeatures[f, u]
                    tf = trackFeatures[f, t]

                    userFeatures[f, u]  = uf + .001*(err*tf - .015 * uf)
                    trackFeatures[f, t] = tf + .001*(err*uf - .015 * tf)
                print "  epoch=%d RMSE=%f" % (e, (sq/len(ratings))**.5)

            for r in range(len(ratings)):
                u = ratings[r].user
                t = ratings[r].track
                c = ratings[r].cache
                ratings[r].cache = shortPredict(u,t, f, c, False)

    def shortPredict(self, user, track, f, cache, trailing):
        if f > 0:
            sum = cache
        else:
            sum = self.tracks[track][4]

        sum += self.userFeatures[f, user] * self.trackFeatures[f, track]

        if sum < 0:
            sum = 0
        if sum > 100:
            sum  = 100

        if trailing:
            sum += INIT*INIT*(self.nFeatures-1-f)
            if sum < 0:
                sum = 0
            if sum > 100:
                sum  = 100
        return sum

#def main(args):
#    import  optparse
#    parser = optparse.OptionParser()
#    parser.usage = __doc__
#    parser.add_option("-q", "--quiet",
#                      action="store_false", dest="verbose", default=True,
#                      help="don't print status messages to stdout")
#    parser.add_option("-l", "--load",
#                      action="store_true", dest="load",
#                      help="load from a cache file")
#    parser.add_option("-f", "--features",
#                      action="store", type=int, dest="nFeatures", default=10,
#                      help="user nfeatures")
#    parser.add_option("-e", "--epochs",
#                      action="store", type=int, dest="nepochs", default=10,
#                      help="train through nepochs")
#
#    (options, args) = parser.parse_args()
#    if len(args) < 1:
#        parser.error("Not enough arguments given")
#    if options.load:
#        svd = SVD.load(args[0], options.nFeatures)
#    else:
#        svd = SVD(args[0], options.nFeatures)
#        svd.dump("cache")
#
#    svd.train_all(options.nepochs)
#
#    return 0
#
#
#
#if __name__ == "__main__":
#    import sys
#    sys.exit( main( sys.argv ) )


