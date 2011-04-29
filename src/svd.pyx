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

user_t   = [('id', np.uint), ('count', np.uint32), ('sum', np.float32)]
track_t  = [('id', np.uint), ('count', np.uint32 ), ('sum', np.float32 )]
rating_t = [('user', np.uint32), ('track', np.uint32), ('rating', np.float32),
        ('cache', np.float32)]

cdef packed struct user_s:
    np.uint_t id
    np.uint32_t count
    np.float32_t sum

cdef packed struct track_s:
    np.uint_t id
    np.uint32_t count
    np.float32_t sum

cdef packed struct rating_s:
    np.uint32_t user
    np.uint32_t track
    np.float32_t rating
    np.float32_t cache

cdef np.float_t INIT = 0.1 
cdef np.float_t GAMMA = .002 *5 #/ 100 #.010 
cdef np.float_t GAMMA2 = .001*5 #/ 100 #1e-5 #.002 
cdef np.float_t LAMBDA = .04 

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
        cdef np.float32_t a
        cdef np.float32_t score
        for line in trainFile:
            u, n = [ int(x) for x in line.split("|")  ]
            a = 0
            for i in range(n):
                line = trainFile.next()
                sid, stmp, day, tm = line.strip().split("\t")
                id = int(sid)
                score =  float(stmp)
                score /= 100
                if id not in self.tmap:
                    n -= 1
                    continue
                a += score
                id = self.tmap[id]
                ratings[ridx].user = uidx
                ratings[ridx].track = id
                ratings[ridx].rating = score
                tracks[id].count += 1.
                tracks[id].sum += score
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
        #print "mu = %g\n" % self.mu
        self.initModel(nFeatures)

    def initData(self):
        self.ratings = np.memmap(os.path.join(self.dir, "rating.mmap"),
                dtype=rating_t, shape=self.nRatings, mode="w+")
        self.users = np.memmap(os.path.join(self.dir, "user.mmap"),
                dtype=user_t, shape=self.nUsers, mode="w+")
        self.tracks = np.memmap(os.path.join(self.dir, "track.mmap"),
                dtype=track_t, shape=self.nTracks, mode="w+")

    def initModel(self, unsigned int nFeatures):
        cdef unsigned int f, i, r, ri, u, t
        self.nFeatures = nFeatures
        cdef unsigned int nUsers = self.nUsers
        cdef unsigned int nTracks = self.nTracks
        cdef np.float_t mu = self.mu, pred, err
        cdef np.ndarray[user_s, ndim=1] users = self.users
        cdef np.ndarray[rating_s, ndim=1] ratings = self.ratings
        cdef np.ndarray[track_s, ndim=1] tracks = self.tracks
        cdef user_s user
        cdef rating_s rating

        cdef np.ndarray[np.float_t, ndim=2] p = np.memmap(
                os.path.join(self.dir, "userFeatures.mmap"),
                shape=(nFeatures, nUsers), dtype=np.float, mode='w+')
        cdef np.ndarray[np.float_t, ndim=2] q = np.memmap(
                os.path.join(self.dir, "trackFeatures.mmap"),
                shape=(nFeatures, nTracks), dtype=np.float, mode='w+')

        cdef np.ndarray[np.float_t, ndim=2] x = np.memmap(
                os.path.join(self.dir, "x.mmap"),
                shape=(nFeatures, nTracks), dtype=np.float, mode='w+')

        cdef np.ndarray[np.float_t, ndim=2] y = np.memmap(
                os.path.join(self.dir, "y.mmap"),
                shape=(nFeatures, nTracks), dtype=np.float, mode='w+')

        cdef np.ndarray[np.float_t, ndim=1] bu = np.memmap(
                os.path.join(self.dir, "userBline.mmap"),
                shape=(nUsers), dtype=np.float, mode='w+')
        cdef np.ndarray[np.float_t, ndim=1] bi = np.memmap(
                os.path.join(self.dir, "itemBline.mmap"),
                shape=(nTracks), dtype=np.float, mode='w+')
        lasterr = 100000
        for i in range(nUsers):
            user = users[i]
            bu[i] = 0 #<float>user.sum / <float>user.count - mu
        for f in range(nFeatures):
            for i in range(nTracks): 
                q[f, i] = .1 / (self.nFeatures)**.5 * (random.random()-.5)#(random.random()*I*3)-2*I
                x[f, i] = .1 / (self.nFeatures)**.5 * (random.random() -.5) 
                y[f, i] = .1 / (self.nFeatures)**.5 * (random.random() -.5)
                #x[f, i] = random.random()*.02 - .01
                #y[f, i] = random.random()*.02 - .01 #I*3) - 2*I
        for i in range(nTracks):
            bi[i] = 0 #tracks[i].pavg  - mu #0.

        lasterr = 100000
        nRatings = self.nRatings
        while 1: #True:
            sq = 0.
            for r in range(nRatings):
                rating = ratings[r]
                t = rating.track
                u = rating.user
                pred =  mu + bu[rating.user] + bi[rating.track]
                err = (<float>rating.rating - pred)
                sq  += err**2

                bi[t] = bi[t] +  2*GAMMA*(err - 2*LAMBDA*bi[t])
                bu[u] = bu[u] + 2*GAMMA*(err - LAMBDA*bu[u])
            err = (sq / <float>nRatings)**.5
            print "RMSE of biases = %g" % err
            if (lasterr - err) < .000001:
                break
            lasterr = err

        sum = 0.
        for r in range(nRatings):
            rating = ratings[r]
            t = rating.track
            u = rating.user
            rating.cache = mu + bu[u] + bi[t]
            sum += rating.rating - rating.cache
        print sum / self.nRatings, np.min(bu), np.min(bi)

        print "MEAN(global) = %g" % mu
        print "MEAN(bu) = %g"     % np.mean(bu)
        print "MEAN(bi) = %g"     % np.mean(bi)
        print np.mean(q[0]), 
        print np.mean(x[0]), 
        print np.mean(y[0]), 
        
        self.bu = bu
        self.bi = bi
        self.p = p
        self.q = q


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
                dtype=rating_t, mode='r')
        self.tracks = np.memmap(os.path.join(self.dir, "track.mmap"),
                dtype=track_t, mode='r')
    
    def loadModel(self):
        self.q = np.memmap(
                os.path.join(self.dir, "trackFeatures.mmap"),
                shape=(self.nFeatures, self.nTracks), dtype=np.float, mode='r+')
        self.p = np.memmap(
                os.path.join(self.dir, "userFeatures.mmap"),
                shape=(self.nFeatures, self.nUsers), dtype=np.float, mode='r+')
        self.bu = np.memmap(
                os.path.join(self.dir, "userBline.mmap"),
                shape=(self.nUsers), dtype=np.float, mode='r+')
        self.bi = np.memmap(
                os.path.join(self.dir, "itemBline.mmap"),
                shape=(self.nTracks), dtype=np.float, mode='r+')
        self.x = np.memmap(
                os.path.join(self.dir, "x.mmap"),
                shape=(self.nFeatures, self.nTracks), dtype=np.float, mode='r+')
        self.y = np.memmap(
                os.path.join(self.dir, "y.mmap"),
                shape=(self.nFeatures, self.nTracks), dtype=np.float, mode='r+')
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
        cdef unsigned int f,e,r, nRatings = self.nRatings, k,j, i, u,t,ri
        cdef np.float_t pred, sq, err, uf ,tf, tmp_bi, tmp_bu
        cdef np.float_t mu = self.mu, ru, tmpp, tmpq
        cdef np.ndarray[user_s, ndim=1] users = self.users
        cdef np.ndarray[np.float_t, ndim=2] p = self.p
        cdef np.ndarray[np.float_t, ndim=2] q = self.q
        cdef np.ndarray[np.float_t, ndim=2] x = self.x
        cdef np.ndarray[np.float_t, ndim=2] y = self.y
        cdef np.ndarray[np.float_t, ndim=1] bu = self.bu
        cdef np.ndarray[np.float_t, ndim=1] bi = self.bi
        cdef rating_s rating
        cdef user_s user
        cdef unsigned int f1
        cdef float tmpr 
    
        cdef np.ndarray[np.float_t, ndim=1] sum = np.ndarray(self.nFeatures, dtype=np.float)

        fullstart = time.time()
        for e in range(nepochs):
            if self.nProcs > 1:
                self.proc.barrier()
            start = time.time()
            sq = 0.
            ftotime = 0
            r = 0
            for u in range(self.nUsers):
                user = users[u]
                ru = 1. / (<np.float_t>user.count)**.5
                for  f in range(self.nFeatures):
                    p[f, u] =  0
                    for ri in range(user.count):
                        rating = ratings[r + ri]
                        t = rating.track
                        tmpr = (<np.float_t>rating.rating - rating.cache) 
                        p[f,u] += tmpr*x[f,t] + y[f,t]
                    p[f,u] *= ru

                for f in range(self.nFeatures):
                    sum[f] = 0

                for ri in range(user.count):
                    rating = ratings[r+ri]
                    t = rating.track
                    pred = mu + bi[t] + bu[u]
                    for f in range(self.nFeatures):
                        pred += p[f,u] * q[f, t]
                    #if pred > 100:
                    #    pred = 100
                    #if pred < 0:
                    #    pred = 0
                    err =  <np.float_t>rating.rating - pred
                    #print err
                    sq += (err*100)**2.
                    #err /= 100

                    for f in range(self.nFeatures):
                        tmpp = p[f,u]
                        tmpq = q[f,t]
                        sum[f] += err*tmpq  #/ 10000.
                        #if sum[0] > 1000:
                        #    print err, tmpq
                        #p[f,u] = tmpp + GAMMA * ( err *tmpq - LAMBDA*tmpp)
                        q[f, t] = tmpq + GAMMA * ( err*tmpp - LAMBDA*tmpq)

                        tmp_bi = bi[t]
                        bi[t] = tmp_bi + GAMMA * ( err - LAMBDA*tmp_bi )
                        tmp_bu = bu[u]
                        bu[u] = tmp_bu + GAMMA * ( err - LAMBDA*tmp_bu )
                    
                #for f in range(self.nFeatures):
                #    sum[f] *= 1./40**3
                #print sum[0]
                for f in range(self.nFeatures):
                    for ri in range(user.count):
                        rating = ratings[r+ri]
                        t = rating.track
                        tmpr = <np.float_t>rating.rating - rating.cache
                        tmpx = x[f,t]
                        tmpy = y[f,t]
                        x[f,t] = tmpx + GAMMA2*(ru*(tmpr)*sum[f] - LAMBDA * tmpx)
                        y[f,t] = tmpy + GAMMA2*(ru*sum[f] - LAMBDA * tmpy) 
                r +=  user.count
                #ftotime += (time.time() - ftime)
            stop = time.time()
            if self.nProcs == 1 or (self.nProcs - 1) == self.rank or True:
                print "  epoch=%d RMSE=%f time=%f rank=%d v=%g" % (e,
                        (sq/nRatings)**.5, stop-start, self.rank,
                        self.validate()) #, ftotime)
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

        print np.mean(p[0]),np.min(p[0]), np.max(p[0]), p[0][:10]
        print np.mean(q[0]),np.min(q[0]), np.max(q[0]), q[0][:10]
        print np.mean(x[0]),np.min(x[0]), np.max(x[0]), x[0][:10]
        print np.mean(y[0]),np.min(y[0]), np.max(y[0]), y[0][:10]
        return


    def predict(self, user, item):
        sum = self.mu + self.bi[item] + self.bu[user]
        for f in range(self.nFeatures):
            sum += self.p[f, user] * self.q[f, item]
        #sum  = (sum - 1.5)*25
        if sum > 1:
            sum = 1
        if sum < 0:
            sum = 0
        return sum*100

    def validate(self):
        file = open(os.path.join(self.dir, "validationIdx1.txt"))
        sq = 0.
        total = 0
        for line in file:
             user, num = [int(x) for x in line.strip().split("|")]
             for  i in range(num):
                line = file.next().strip()
                item, score, day, tm = line.split("\t")
                score = float(score)
                item = int(item)
                if item not in self.tmap:
                    continue
                if user not  in self.umap:
                    continue
                uid = self.umap[user]
                item = self.tmap[item]
                pred = self.predict(uid, item)
                err = pred -score
                sq += err**2.
                total += 1
        #print "RMSE=%g\n" % (sq/total)**.5
        return (sq/total)**.5

    #def modelStats(self):
    #    f = open("model.txt", "w")
    #    for f in range(self.nFeatures):
    #        f.write("Features %d" % f)
    #        f.write("q: %g to %g  mean=%g\n" %( np.min(q[f]), np.max(q[f]), np.mean(q[f])))
    #        f.write("p: %g to %g  mean=%g\n" %( np.min(p[f]), np.max(p[f]), np.mean(p[f])))
    #        f.write("x: %g to %g  mean=%g\n" %( np.min(x[f]), np.max(x[f]), np.mean(x[f])))
    #        f.write("y: %g to %g  mean=%g\n" %( np.min(y[f]), np.max(y[f]), np.mean(y[f])))
        

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
                
