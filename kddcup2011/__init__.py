#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog [options]
Python source code - replace this with a description of the code and write the
code below this text.
"""

__author__ = 'Patrick Butler'
__email__  = 'pbutler@killertux.org'

#try:
#    import json
#except ImportError:
#    import simplejson as json
#import socket

# Echo server program
import time
import sys
import os
import logging
import datetime
import subprocess

TEST = False

mainpath = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append( mainpath )
mainpath = os.path.join(mainpath, "kddcup2011") #netsec")
sys.path.append( mainpath )
os.environ["DJANGO_SETTINGS_MODULE"] = "kddcup2011.settings"

import django
from data.models import *

