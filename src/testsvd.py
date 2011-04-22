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

try:
    from svd import SVD
except ImportError:
    from svdslow import SVD

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
    parser.add_option("-f", "--features",
                      action="store", type=int, dest="nFeatures", default=10,
                      help="user nfeatures")
    parser.add_option("-e", "--epochs",
                      action="store", type=int, dest="nepochs", default=10,
                      help="train through nepochs")

    (options, args) = parser.parse_args()
    if len(args) < 1:
        parser.error("Not enough arguments given")
    if options.load:
        svd = SVD.load(args[0], options.nFeatures)
        svd.train_all(options.nepochs)
    else:
        svd = SVD(args[0], options.nFeatures)
        svd.dump("cache")


    return 0



if __name__ == "__main__":
    import sys
    sys.exit( main( sys.argv ) )


