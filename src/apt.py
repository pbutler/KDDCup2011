#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog [options]
Python source code - replace this with a description of the code and write the code below this text.
"""

__author__ = 'Patrick Butler'
__email__  = 'pbutler@killertux.org'

import sys
import os
import subprocess
import random
import re
from multiprocessing import Pool

source = "main.cpp"
testdir = "../testdata10"

def singleton(cls):
    instances = {}
    def getinstance():
        if cls not in instances:
            instances[cls] = cls()
        return instances[cls]
    return getinstance

@singleton
class Source(object):
    def __init__(self):
        print "Digesting Source"
        startGuard = "//**START PARAMS"

        endGuard = "//**END PARAMS"
        mode = 0  #0=preamble, 1=params, 2=rest
        self.preamble = ""
        self.rest = ""
        self.params= {}
        file = open(source)
        reparam = re.compile(r"double\s+(\S+)\s*=\s*([^ ;]+)\s*;.*")
        for line in file:
            if line.startswith(startGuard):
                mode = 1
                continue
            elif line.startswith(endGuard):
                mode = 2
                continue

            if mode == 0:
                self.preamble += line
            elif mode == 2:
                self.rest += line
            else:
                if line.strip() == "":
                    continue
                match = reparam.search(line)
                if match is None:
                    print "OOPS", line
                    continue
                name, value = match.group(1), float(match.group(2))
                self.params[name] = value

    def getGenes(self):
        ret = self.params.copy()
        #for k in ret:
        #    ret[k] = 1
        return ret


class GAPT(object):
    def __init__(self, genes = None):
        if genes:
            self.genes = genes.copy()
        else:
            s = Source()
            self.genes = s.getGenes()
            self.mutate( 1+int(random.random() * len(self.genes)))
        self.valuation = None

    def mate(self, babymama):
        babygenes = {}
        babygenes2 = {}

        for k in self.genes:
            p = random.random()
            babygenes[k] = p*self.genes[k]+ (1.-p)*babymama.genes[k]
            babygenes2[k] = (1.-p)*self.genes[k]+ p*babymama.genes[k]
        return [ GAPT(babygenes), GAPT(babygenes2)]

    def evaluate(self):
        if self.valuation is not None:
            return self.valuation
        #s = Source()
        #s.compileWithParams(self.genes)
        curdir = os.getcwd()
        os.chdir(testdir)
        file = open("params.txt", "w")
        for k in self.genes:
            file.write("%s=%g\n" %(k, self.genes[k]))
        file.close()
        results = subprocess.Popen(["../src/main","-P", "-m", "-t"],stdout=subprocess.PIPE).communicate()[0]
        #print results
        os.chdir(curdir)
        vrmsere = re.compile(r"Best is ([-e0-9.]+)")
        val = 0
        for line in results.strip().split("\n"):
            if "nan" in line:
                val = 10000000.
                break
            match = vrmsere.search(line)
            if match:
                val = float( match.group(1))
        self.valuation = val
        print val,
        sys.stdout.flush()
        return val

    def mutate(self, n = None):
        if n is None:
            w = len(self.genes) / 2.
            n = random.normalvariate(2*w/3., w/2)
            n = int(min(max(1, n), len(self.genes)))
        n = 1 #len(self.genes)
        keys = random.sample(self.genes.keys(), n)
        for k in keys:
            m = 10**int(random.random()*3+2)
            r= int(random.normalvariate(0, 10./3)) / float(m)
            if r == 0:
                r = 2*int(random.random()-.5) / float(m)
            self.genes[k] += r
            if self.genes[k] <= 0:
                self.genes[k] = int(1+100*random.random()) / 1000.

            if k.startswith("decay") and self.genes[k] >= 1.:
                self.genes[k] = random.random() /4. + .5
            #if k == "itemStep":
            #    self.genes[k] += int((random.random()-.5)*100)/10000.
            #else:
            #    self.genes[k] += random.normalvariate(0, .1/3)
            #    if self.genes[k] < 0:
            #        self.genes[k] = random.random()
            #if k.endswith("Step"):
            #    mn = 0
            #    mx = 2 #.001
            #elif k.endswith("Min"):
            #    mn = -.3
            #    mx = 0
            #elif k.endswith("Max"):
            #    mn = 0
            #    mx = .1
            #else:
            #    mn = 0
            #    mx = 2
            #self.genes[k] = (mx-mn)*(random.random()) + mn

    def copy(self):
        return GAPT(self.genes.copy())

    def __str__(self):
        return self.__repr__()
    def __repr__(self):
        return  ", ".join( [ k+" = " + str(v) for k,v in self.genes.items() ])

def selectWithProb(l):
    #return int( len(l)*random.random() )
    l = [ 1./(i*i) for i in range(1,1+len(l)) ]
    all = sum(l)
    r = random.random()*all
    a = 0
    for i in range(len(l)):
        if a < r  and r < a+l[i]:
            return i
        a += l[i]

def sim(a,b):
    acc = 0
    for k in a.genes.keys():
        acc += abs(a.genes[k] - b.genes[k]) / (a.genes[k] + b.genes[k])
    return acc

def cull(population, maxpop):
    ranks = [ (p.evaluate(), p ) for p in population ]
    ranks.sort()

def foo(p):
    return p.evaluate(), p

def runGA(pop = 50, noffspring = 20, mrate = .5, runs = 100):
    population = [ GAPT() for i in range(pop)]

    #pool = Pool(4)

    best = None
    bestN = None
    try:
        for run in range(runs):
            #evaluate
            ranks = map(foo, population) # for p in population ]
            #ranks = pool.map(foo, population) # for p in population ]
            ranks.sort()

            population = [ p for e,p in ranks]
            fits = [ e for e,p in ranks]

            #breed
            offspring = []

            for i in range(noffspring):
                mom, dad = selectWithProb(fits), selectWithProb(fits)
                while mom == dad:
                    mom, dad = selectWithProb(fits), selectWithProb(fits)
                #print mom, dad
                offspring += population[dad].mate(population[mom])
                #print population[mom]
                #print population[dad]
                #print offspring[-1]
                #print
            #mutate
            for p in offspring:
                if random.random() < mrate:
                    p.mutate()

            #merge
            population += offspring
            print "--",
            #cull
            ranks = [ foo(p) for p in population ]
            ranks.sort()
            ranks = ranks[:pop]
            population = [ p for e,p in ranks]
            print
            #print ", ".join( map(str, [ e for e,p in ranks]))

            if best is None or (ranks[0][0] < bestN):
                best = ranks[0][1].copy()
                bestN = ranks[0][0]
            print "Best in run",run,"is",bestN
            print best

    except KeyboardInterrupt:
        pass
    return bestN, best


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

    n, p = runGA()
    print "Best was", n
    for k in p.genes:
        print "double %s = %g;" %( k,p.genes[k])
    return 0



if __name__ == "__main__":
    import sys
    sys.exit( main( sys.argv ) )


