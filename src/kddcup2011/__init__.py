#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog [options]
Python source code - replace this with a description of the code and write the
code below this text.
"""

__author__ = 'Patrick Butler'
__email__  = 'pbutler@killertux.org'

import sys
import os
import datetime
from sqlalchemy import *
TEST = False

try:
    mainpath = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    sys.path.append( mainpath )
    mainpath = os.path.join(mainpath, "kddcup2011") #netsec")
    sys.path.append( mainpath )
    os.environ["DJANGO_SETTINGS_MODULE"] = "kddcup2011.settings"
    import django
except Exception, e:
    print "Django not installed"

from sqlalchemy import *
import orm

engine = orm.engine
tUser = orm.User.__table__
tRating = orm.Rating.__table__
tGenre = orm.Genre.__table__
tArtist = orm.Artist.__table__
tAlbum = orm.Album.__table__
tTrack = orm.Track.__table__
tAlbumGenre = orm.album_genre
tTrackGenre = orm.track_genre

