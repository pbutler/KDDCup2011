#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog [options]
Python source code - replace this with a description of the code and write the code below this text.
"""

__author__ = 'Patrick Butler'
__email__  = 'pbutler@killertux.org'

import svd
import multiprocessing as mp
from multiprocessing import pool as pool
from math import ceil, log

class SVDProcess( mp.Process ):
    def __init__(self, file, args, rank, numproc, events):
        mp.Process.__init__(self)
        if args == None:
            args = []

        self.file = file
        self.args = args
        self.rank = rank
        self.events = events
        self.numproc = numproc

    def run(self):
        args = self.args[0]
        kwargs = self.args[1]
        self.svd = svd.SVD.load(self.file, self.rank, self.numproc)
        self.barrier()
        self.svd.train_all(*args, **kwargs)
        self.barrier()

    def barrier(self):
        if self.numproc == 1:
            return
        for k in range(int(ceil(log(self.numproc)/log(2)))):
            # send event to thread (rank + 2**k) % numproc
            receiver = (self.rank + 2**k) % self.numproc
            evt = self.events[ self.rank * self.numproc + receiver ]
            evt.set()
            # wait for event from thread (rank - 2**k) % numproc
            sender = (self.rank - 2**k) % self.numproc
            evt = self.events[ sender * self.numproc + self.rank ]
            evt.wait()
            evt.clear()

def runManySVDs(file, numproc = 2, *args, **kwargs):

        events = [ mp.Event() for i in range(numproc**2)]
        procs = [ SVDProcess(file=file, args = (args,kwargs), rank=i,
            numproc = numproc, events = events) for i in range(numproc)]

        for p in procs:
            p.start()

        for p in procs:
            p.join()

def foo():
    proc = mp.current_process()
    proc.barrier()
    print proc.rank

def main(args):
    import  optparse
    parser = optparse.OptionParser()
    parser.usage = __doc__
    parser.add_option("-q", "--quiet",
                      action="store_false", dest="verbose", default=True,
                      help="don't print status messages to stdout")
    (options, args) = parser.parse_args()
    if len(args) < 0:
        parser.error("Not enough arguments given")
    bp = BarrierPool(target=foo)
    bp.start()
    bp.join()
    return 0



if __name__ == "__main__":
    import sys
    sys.exit( main( sys.argv ) )


