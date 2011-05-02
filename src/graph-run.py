#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog [options]
Python source code - replace this with a description of the code and write the code below this text.
"""


__author__ = 'Patrick Butler'
__email__  = 'pbutler@killertux.org'

import matplotlib.pyplot as plt
import sys
import select

def main(args):
    import  optparse
    parser = optparse.OptionParser()
    parser.usage = __doc__
    parser.add_option("-q", "--quiet",
                      action="store_false", dest="verbose", default=True,
                      help="don't print status messages to stdout")
    (options, args) = parser.parse_args()


    plt.ion()
    f = plt.figure()
    a = f.add_subplot(111)
    a.set_xlim(0,100)
    a.set_ylim(0,.2)
    feature = None
    curl = None
    i = 0
    x = []
    y = []
    plt.show()
    file = sys.stdin #open("test")
    #for line in file: #.readline():
    curl = a.plot(x,y)[0]
    x = []
    y = []
    while True:
        line = sys.stdin.readline()
        sys.stdout.write(line)
        line = line.strip()
        if not line.startswith("epoch"):
            continue

        cols = line.split()
        e = cols[0]
        r = cols[1]
        e = int(e.split("=")[1])
        r = float(r.split("=")[1])
        x += [e]
        y += [r]
        curl.set_data(x,y)
        f.canvas.draw()
        plt.draw()
    plt.ioff()
    plt.show()

    import time
    time.sleep(10)
    print "done"
    if len(args) < 0:
        parser.error("Not enough arguments given")
    return 0



if __name__ == "__main__":
    import sys
    sys.exit( main( sys.argv ) )


