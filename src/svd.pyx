#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog [options]
Python source code - replace this with a description of the code and write the code below this text.
"""

__author__ = 'Patrick Butler'
__email__  = 'pbutler@killertux.org'

import cython
cimport numpy as np

import os
import numpy as np
import cPickle as pickle
import time
import multiprocessing as mp

user_t   = [('count', np.uint), ('sum', np.uint)]
track_t  = [('id', np.uint), ('count', np.uint ), ('sum', np.uint ), 
            ('avg', np.float ), ('pavg', np.float)]
rating_t = [('user', np.uint32), ('track', np.uint32), ('rating', np.uint8),
        ('cache', np.uint16)]

#rating_t = 'int32, int32, uint8, int32'

cdef packed struct user_s:
    np.uint_t count
    np.uint_t sum

cdef packed struct track_s:
    np.uint_t id
    np.uint_t count
    np.uint_t sum
    np.float_t avg
    np.float_t pavg

cdef packed struct rating_s:
    np.uint32_t user
    np.uint32_t track
    np.uint8_t rating
    np.uint16_t cache

INIT = 0.1


class SVD(object):
    def __init__(self, dir, nFeatures = 10, rank = 1, nproc = 1):
        cdef int tidx, ridx, uidx, i, id
        self.nProcs = nproc
        self.rank = rank
        self.proc = mp.current_process()
        self.dir = dir
        self.tmap = {}
        #bad way to do this but it works 
        stats = open(os.path.join(dir, "info.txt")).readlines()
        stats = [ x.strip().split("=") for  x in stats]
        stats = dict( [ (k,int(v)) for k,v in stats] )
        self.__dict__.update(stats)

        self.initData()
        cdef np.ndarray[user_s, ndim=1] users    = self.users
        cdef np.ndarray[track_s, ndim=1] tracks  = self.tracks
        cdef np.ndarray[rating_s,ndim=1] ratings = self.ratings

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

        self.nRatings = ridx
        self.nUsers = uidx

        cdef float ntrack
        cdef float  sumtrack
        for i in range(self.nTracks):
            ntrack = float(tracks[i].count)
            sumtrack  = float(tracks[i].sum)
            tracks[i].avg = 0
            if n > 0:
                tracks[i].avg  = sumtrack / n
            tracks[i].pavg = ( float( 50*25 + sumtrack) / (25 + sumtrack))

        self.initModel(nFeatures)

    def initData(self):
        self.ratings = np.memmap(os.path.join(self.dir, "rating.mmap"),
                dtype=rating_t, shape=self.nRatings, mode="w+")
        self.users = np.memmap(os.path.join(self.dir, "user.mmap"),
                dtype=user_t, shape=self.nUsers, mode="w+")
        self.tracks = np.memmap(os.path.join(self.dir, "track.mmap"),
                dtype=track_t, shape=self.nTracks, mode="w+")

    def initModel(self, unsigned int nFeatures):
        cdef unsigned int f, i
        self.nFeatures = nFeatures
        cdef unsigned int nUsers = self.nUsers
        cdef unsigned int nTracks = self.nTracks
        cdef np.ndarray[np.float_t, ndim=2] userFeatures = np.memmap(
                os.path.join(self.dir, "userFeatures.mmap"),
                shape=(nFeatures, nUsers), dtype=np.float, mode='w+')
        cdef np.ndarray[np.float_t, ndim=2] trackFeatures = np.memmap(
                os.path.join(self.dir, "trackFeatures.mmap"),
                shape=(nFeatures, nTracks), dtype=np.float, mode='w+')
        for f in range(nFeatures):
            for i in range(nUsers):
                userFeatures[f, i]  = INIT 
            for i in range(nTracks): 
                trackFeatures[f, i] = INIT
        self.userFeatures = userFeatures
        self.trackFeatures = trackFeatures


    def getsize(self):
        total = 0
        for key, value in self.__dict__.iteritems():
            if isinstance(value, np.ndarray):
                els = reduce(lambda x,y: x*y, value.shape)
                size = els * value.dtype.itemsize
                print key, size, value.dtype.itemsize
                total += size
        return total

    def __getstate__(self):
        odict = self.__dict__.copy()
        for key, value in self.__dict__.iteritems():
            if isinstance(value, np.memmap):
                value.flush()
                del odict[key]
        del odict['rank']
        del odict['nProcs']
        del odict['proc']
        return odict

    def __setstate__(self, dict):
        self.__dict__.update(dict)

    def loadData(self):
        self.users = np.memmap(os.path.join(self.dir, "user.mmap"),
                dtype=user_t, mode='r')
        self.ratings = np.memmap(os.path.join(self.dir, "rating.mmap"),
                dtype=rating_t, mode='r+')
        self.tracks = np.memmap(os.path.join(self.dir, "track.mmap"),
                dtype=track_t, mode='r')
    
    def loadModel(self):
        self.trackFeatures = np.memmap(
                os.path.join(self.dir, "trackFeatures.mmap"),
                shape=(self.nFeatures, self.nTracks), dtype=np.float, mode='r+')
        self.userFeatures = np.memmap(
                os.path.join(self.dir, "userFeatures.mmap"),
                shape=(self.nFeatures, self.nUsers), dtype=np.float, mode='r+')

    @classmethod
    def load(cls, dir, rank=1, nproc=1):
        pklfile = os.path.join(dir, "cache")
        svd = pickle.load(open(pklfile))
        svd.nProcs = nproc
        svd.rank = rank
        svd.proc = mp.current_process()
        svd.dir = dir
        svd.loadData()
        svd.loadModel()
        return svd

    def dump(self):
        pickle.dump(self, open(os.path.join(self.dir, "cache"), "w"), -1)
    
    @cython.boundscheck(False)
    def train_all(self, nepochs = 10):
        cdef np.ndarray[rating_s, ndim=1] ratings = self.ratings
        cdef unsigned int f,e,r, nRatings = self.nRatings
        cdef np.float_t p, sq, err, uf ,tf 
        cdef np.ndarray[np.float_t, ndim=2] userFeatures = self.userFeatures
        cdef np.ndarray[np.float_t, ndim=2] trackFeatures = self.trackFeatures
        cdef rating_s rating


        for f in range(self.nFeatures):
            if self.nProcs > 1:
                self.proc.barrier()
            print "Training Feature %d" % f
            for e in range(nepochs):
                start = time.time()
                sq = 0.
                for r in range(self.rank, nRatings, self.nProcs):
                    rating = ratings[r]
                    p = <int>shortPredict(self, rating.user, rating.track, f,
                            rating.cache, True)
                    err =  rating.rating - p
                    sq += err**2
                    uf = userFeatures[f, rating.user]
                    tf = trackFeatures[f, rating.track]

                    userFeatures[f, rating.user]  = uf + .001*(err*tf - .015 * uf)
                    trackFeatures[f, rating.track] = tf + .001*(err*uf - .015 * tf)
                stop = time.time()
                print "  epoch=%d RMSE=%f time=%f" % (e, (sq/nRatings)**.5,
                        stop-start)

            if self.nProcs > 1:
                self.proc.barrier()

            for r in range(self.rank, nRatings, self.nProcs):
                rating = ratings[r]
                ratings[r].cache = <int>shortPredict(self, rating.user, rating.track, f,
                            rating.cache, False)

@cython.boundscheck(False)
cdef inline float shortPredict(self, int user, int track, int f, int cache, bint trailing):
    cdef np.ndarray[np.float_t, ndim=2] userFeatures = self.userFeatures
    cdef np.ndarray[np.float_t, ndim=2] trackFeatures = self.trackFeatures
    cdef float sum
    cdef int nFeatures = self.nFeatures

    if f > 0:
        sum = cache
    else:
        sum = self.tracks[track][4]

    sum += userFeatures[f, user] * trackFeatures[f, track]

    if sum < 0:
        sum = 0
    if sum > 100:
        sum  = 100

    if trailing:
        sum += INIT*INIT*(nFeatures-1-f)
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


