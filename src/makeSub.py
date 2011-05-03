#!/usr/bin/env python
'''
Use this script for converting a textual Track1 prediction file into a
well formatted binary submission file.

The script accepts two arguments:
<prediction file> (input) and <submission file> (output)

The input prediction file should contain 6005940 lines, corresponding
to the 6005940 user-item pairs in the test set.
Each line contains a predicted score (a real number between 0 and 100).
The generated output file can be submitted to the KDD-Cup'11 evaluation
system.
'''


import sys
from sys import stdout,stderr

if sys.argv.__len__()<3:
    print >> stderr, "Error: Must supply two command line arguments: <prediction file> (input) and <submission file> (output)"
    sys.exit()

inFile = open(sys.argv[1],"r")
outFile = open(sys.argv[2], "wb")

ExpectedTestSize = 6005940

lineNum = 0
sumPreds = 0
for line in inFile:
    lineNum += 1
    prediction = float(line)
    if prediction<0 or prediction>100:
        print >> stderr, "Error: out of bounds prediction at line", lineNum
        sys.exit()
    roundScore = int(2.55*prediction+0.5)
    outFile.write(chr(roundScore))
    sumPreds += prediction

if lineNum!=ExpectedTestSize:
    print >> stderr, "Error: expected %d predictions, but read %d ones"%(ExpectedTestSize,lineNum)
    sys.exit()


inFile.close()
outFile.close()

print >> stderr, "**Completed successfully (mean prediction: %lf)**"%(sumPreds/ExpectedTestSize)
