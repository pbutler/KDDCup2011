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
import random

user_t   = [('id', np.uint), ('count', np.uint), ('sum', np.uint)]
track_t  = [('id', np.uint), ('count', np.uint ), ('sum', np.uint ), 
            ('avg', np.float ), ('pavg', np.float)]
rating_t = [('user', np.uint32), ('track', np.uint32), ('rating', np.uint8),
        ('cache', np.uint16)]

cdef packed struct user_s:
    np.uint_t id
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
GAMMA = .002
LAMBDA = .04

class SVD(object):
    def __init__(self, dir, nFeatures = 10, rank = 0, nproc = 1):
        cdef int tidx, ridx, uidx, i, id
        self.nProcs = nproc
        self.rank = rank
        self.proc = mp.current_process()
        self.dir = dir
        self.tmap = {}
        self.umap = {}
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

        genreFile = open(os.path.join(dir, "genreData1.txt"))
        for line in genreFile:
            data = line.strip().split("|")
            t = int(data[0])
            tracks[tidx].id = t
            tracks[tidx].sum = 0
            tracks[tidx].count = 0
            self.tmap[t] = tidx
            tidx += 1
        genreFile.close()

        albumFile = open(os.path.join(dir, "albumData1.txt"))
        for line in albumFile:
            data = line.strip().split("|")
            t = int(data[0])
            tracks[tidx].id = t
            tracks[tidx].sum = 0
            tracks[tidx].count = 0
            self.tmap[t] = tidx
            tidx += 1
        albumFile.close()

        artistFile = open(os.path.join(dir, "artistData1.txt"))
        for line in artistFile:
            data = line.strip().split("|")
            t = int(data[0])
            tracks[tidx].id = t
            tracks[tidx].sum = 0
            tracks[tidx].count = 0
            self.tmap[t] = tidx
            tidx += 1
        artistFile.close()
        self.nTracks = tidx

        trainFile = open(os.path.join(dir, "trainIdx1.txt"))
        uidx = 0
        ridx = 0
        self.mu = 0.
        cdef int a
        for line in trainFile:
            u, n = [ int(x) for x in line.split("|")  ]
            a = 0
            for i in range(n):
                line = trainFile.next()
                sid, score, day, tm = line.strip().split("\t")
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
                tracks[id].sum += <np.float_t>(score)
                ridx += 1
            if n == 0:
                continue
            users[uidx].id = u
            self.umap[u] = uidx
            users[uidx].count = n
            users[uidx].sum = a 
            self.mu += a
            uidx += 1
        trainFile.close()

        self.nRatings = ridx
        self.nUsers = uidx
        self.mu /= ridx
        print "mu = %g\n" % self.mu
        cdef float ntrack
        cdef float  sumtrack
        for i in range(self.nTracks):
            ntrack = <float>tracks[i].count
            sumtrack  = <float>tracks[i].sum
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
        cdef np.ndarray[user_s, ndim=1] users = self.users
        cdef np.ndarray[track_s, ndim=1] tracks = self.tracks

        cdef np.ndarray[np.float_t, ndim=2] userFeatures = np.memmap(
                os.path.join(self.dir, "userFeatures.mmap"),
                shape=(nFeatures, nUsers), dtype=np.float, mode='w+')
        cdef np.ndarray[np.float_t, ndim=2] trackFeatures = np.memmap(
                os.path.join(self.dir, "trackFeatures.mmap"),
                shape=(nFeatures, nTracks), dtype=np.float, mode='w+')

        cdef np.ndarray[np.float_t, ndim=1] bu = np.memmap(
                os.path.join(self.dir, "userBline.mmap"),
                shape=(nUsers), dtype=np.float, mode='w+')
        cdef np.ndarray[np.float_t, ndim=1] bi = np.memmap(
                os.path.join(self.dir, "itemBline.mmap"),
                shape=(nTracks), dtype=np.float, mode='w+')

        for f in range(nFeatures):
            for i in range(nUsers):
                userFeatures[f, i]  = (random.random()*INIT *3) - 2*INIT
            for i in range(nTracks): 
                trackFeatures[f, i] = (random.random()*INIT *3) - 2*INIT
        for i in range(nUsers):
            bu[i] = ( users[i].sum + self.mu*25) / (25 + users[i].count)

        for i in range(nTracks):
            bi[i] = ( tracks[i].sum - self.mu*tracks[i].count) / (25 + tracks[i].count)
        self.bu = bu
        self.bi = bi
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
        self.bu = np.memmap(
                os.path.join(self.dir, "userBline.mmap"),
                shape=(self.nUsers), dtype=np.float, mode='r+')
        self.bi = np.memmap(
                os.path.join(self.dir, "itemBline.mmap"),
                shape=(self.nTracks), dtype=np.float, mode='r+')

    @classmethod
    def load(cls, dir, rank=0, nproc=1):
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
        cdef int lastUser = -1
        cdef np.ndarray[rating_s, ndim=1] ratings = self.ratings
        cdef unsigned int f,e,r, nRatings = self.nRatings
        cdef np.float_t p, sq, err, uf ,tf, tmp_bi, tmp_bu
        cdef np.float_t mu = self.mu, ru
        cdef np.ndarray[np.float_t, ndim=2] userFeatures = self.userFeatures
        cdef np.ndarray[np.float_t, ndim=2] trackFeatures = self.trackFeatures
        cdef np.ndarray[np.float_t, ndim=1] bu = self.bu
        cdef np.ndarray[np.float_t, ndim=1] bi = self.bi
        cdef rating_s rating
        cdef unsigned int f1

        fullstart = time.time()
        for e in range(nepochs):
            if self.nProcs > 1:
                self.proc.barrier()
            start = time.time()
            sq = 0.
            ftotime = 0
            lastUser = -1
            for r in range(nRatings):
                rating = ratings[r]
                p = mu + bi[rating.track] + bu[rating.user]

                for f1 in range(self.nFeatures): #f+1):
                    p += userFeatures[f1, rating.user] * trackFeatures[f1, rating.track]
                #p = <int>shortPredict(self, rating.user, rating.track, f,
                #        rating.cache, True)
                err = float(rating.rating) - p
                sq += err**2.
                #ftime = time.time()
                for f in range(self.rank, self.nFeatures, self.nProcs):
                    tmp_bi = bi[rating.track]
                    tmp_bu = bu[rating.user]
                    uf = userFeatures[f, rating.user]
                    tf = trackFeatures[f, rating.track]
            
                    
                    bi[rating.track] = GAMMA * ( err - LAMBDA*tmp_bi )
                    bu[rating.user] = GAMMA * ( err - LAMBDA*tmp_bu )
                    userFeatures[f, rating.user]  = uf + GAMMA*(err*tf - LAMBDA * uf)
                    trackFeatures[f, rating.track] = tf + GAMMA*(err*uf - LAMBDA * tf)
                #ftotime += (time.time() - ftime)
            stop = time.time()
            if self.nProcs == 1 or (self.nProcs - 1) == self.rank or True:
                print "  epoch=%d RMSE=%f time=%f rank=%d" % (e,
                        (sq/nRatings)**.5, stop-start, self.rank) #, ftotime)
        if self.nProcs > 1:
            self.proc.barrier()

        if self.rank == 0:
            print "Full time %g" % (time.time()-fullstart)
            #if self.nProcs > 1:
            #    self.proc.barrier()

            #for r in range(self.rank, nRatings, self.nProcs):
            #    rating = ratings[r]
            #    ratings[r].cache = <int>shortPredict(self, rating.user, rating.track, f,
            #                rating.cache, False)

        #for f in range(self.nFeatures):
        #    print "%d, %f, %f" %( f, np.mean(userFeatures[f]),np.mean(trackFeatures[f]))
        return


    def predict(self, user, item):
        sum = self.mu + self.bi[item] + self.bu[user]
        for f in range(self.nFeatures):
            sum += self.userFeatures[f, user] * self.trackFeatures[f, item]
        if sum > 100:
            sum = 100
        if sum < 0:
            sum = 0
        return sum

    def validate(self):
        file = open(os.path.join(self.dir, "validationIdx1.txt"))
        sq = 0.
        total = 0
        for line in file:
             user, num = [int(x) for x in line.strip().split("|")]
             for  i in range(num):
                line = file.next().strip()
                item, score, day, tm = line.split("\t")
                score = int(score)
                item = int(item)
                if item not in self.tmap:
                    continue
                if user not  in self.umap:
                    continue
                uid = self.umap[user]
                item = self.tmap[item]
                pred = self.predict(uid, item)
                err = pred -score
                print pred,score
                sq += err**2.
                total += 1
        print "RMSE=%g\n" % (sq/total)**.5

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
                
