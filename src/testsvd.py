#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog [options]
Python source code - replace this with a description of the code and write the code below this text.
"""

__author__ = 'Patrick Butler'
__email__  = 'pbutler@killertux.org'

user_t   = 'int, int' #ncount, sum
track_t  = 'int, int, int, float, float' #id, count, sum, avg, pavg
rating_t = 'int,int,int,int' #user, movie, rating, cache

from svd import SVD
print "Loaded Fast SVD"

def main(args):
    import  optparse
    parser = optparse.OptionParser()
    parser.usage = __doc__
    parser.add_option("-q", "--quiet",
                      action="store_false", dest="verbose", default=True,
                      help="don't print status messages to stdout")
    parser.add_option("-l", "--load",
                      action="store_true", dest="load",
                      help="load from a cache file")
    parser.add_option("-c", "--cache",
                      action="store_true", dest="cache",
                      help="build a cache of data")
    parser.add_option("-f", "--features",
                      action="store", type=int, dest="nFeatures", default=10,
                      help="user nfeatures")
    parser.add_option("-e", "--epochs",
                      action="store", type=int, dest="nepochs", default=10,
                      help="train through nepochs")
    parser.add_option("-s", "--slow",
                      action="store_true", dest="slow",
                      help="use non cython model (probably will be obsolete")
    parser.add_option("-S", "--size",
                      action="store_true", dest="getsize", default=False,
                      help="print np.ndarray sizes and exit")
    parser.add_option("-p", "--nprocs",
                      action="store", dest="nprocs", type=int, default=1,
                      help="run in threaded mode",)
    (options, args) = parser.parse_args()
    if len(args) < 1:
        parser.error("Not enough arguments given")


    if options.load:
        svd = SVD.load(args[0])
    else:
        svd = SVD(args[0], options.nFeatures)

    if options.cache:
        svd.dump()
    elif options.getsize:
        size = svd.getsize()
        print "%d bytes, %dMB" % (size, size / 2.**20)
    else:
        if options.nprocs == 1:
            svd.train_all(options.nepochs)
        else:
            import mpsvd
            mpsvd.runManySVDs(args[0], options.nprocs, options.nepochs)
    return 0



if __name__ == "__main__":
    import sys
    sys.exit( main( sys.argv ) )


