#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog [options]
Python source code - replace this with a description of the code and write the code below this text.
"""

__author__ = [ 'Patrick Butler', "Depbrakash Patnaik" ]
__email__  = [ 'pabutler@vt.edu', "patnaik@vt.edu" ]

import time
import os
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker, relation, backref,create_session
from sqlalchemy.ext.declarative import declarative_base
import datetime

import localsettings

engine = create_engine(localsettings.sadb)
#conn = engine.connect()
Session = sessionmaker(autocommit = False, autoflush = True)
Session.configure(bind=engine)
metadata = MetaData()
metadata.bind = engine
session = Session()

Base = declarative_base(engine, metadata)

class SQLData:
    @classmethod
    def select(cls):
        return session.query(cls)
    def add(self):
        return session.add(self)
    def save(self):
        return session.save(self)
    def delete(self):
        return session.delete(self)

album_genre = Table('data_album_genres', Base.metadata,
    Column('album_id', Integer, ForeignKey('data_album.album_id')),
    Column('genre_id', Integer, ForeignKey('data_genre.genre_id'))
)

track_genre = Table('data_track_genres', Base.metadata,
    Column('track_id', Integer, ForeignKey('data_track.track_id')),
    Column('genre_id', Integer, ForeignKey('data_genre.genre_id'))
)

class User(Base,SQLData):
    __tablename__ = 'data_user'
    user_id = Column(Integer, primary_key=True)

    def __init__(self, user_id): #title=None, num_words=None, time=None, hash=None):
        self.user_id = user_id

    def __repr__(self):
        return "<User: %d>" % self.user_id

class Rating(Base, SQLData):
    __tablename__ = 'data_ratings'
    rating_id  = Column(Integer, primary_key=True, autoincrement=True)
    item_id  = Column(Integer) #, primary_key=True)
    timestamp          = Column(DateTime)
    score = Column(Integer)
    user_id   = Column(Integer, ForeignKey('data_user.user_id'))
    user = relation(User, backref=backref('ratings'))

    def __init__(self, item_id = None, timestamp = None, score= None, user=None):
        self.rating_id = None #rating_id
        self.item_id = item_id
        self.timestamp = timestamp
        self.score = score
        self.user = user

    def __repr__(self):
        return "<Rating(%s): %s of %s @ %s>" % (self.rating_id, self.item_id,
                self.score, self.timestamp)


class Genre(Base, SQLData):
    __tablename__ = 'data_genre'
    genre_id = Column(Integer, primary_key=True)
    def __init__(self, genre_id = None):
        self.genre_id = genre_id

class Artist(Base, SQLData):
    __tablename__ = 'data_artist'
    artist_id = Column(Integer, primary_key=True)

    def __init__(self, artist_id = None):
       self.artist_id = artist_id

class Album(Base, SQLData):
    __tablename__ = 'data_album'
    album_id = Column(Integer, primary_key=True)
    artist_id   = Column(Integer, ForeignKey('data_artist.artist_id'))
    artist = relation(Artist, backref=backref('albums'))
    genres = relation("Genre", secondary=album_genre, backref="albums")

    def __init__(self, album_id = None, artist = None):
        self.album_id = album_id
        if isinstance(artist, int):
            self.artist_id = artist
        else:
            self.artist = artist

    def __repr__(self):
        return "<Album(%s): by %s in %d genres>" % ( self.album_id,
                self.artist_id, len(self.genres))

class Track(Base, SQLData):
    __tablename__ = 'data_track'
    track_id = Column(Integer, primary_key=True)
    artist_id   = Column(Integer, ForeignKey('data_artist.artist_id'))
    artist = relation(Artist, backref=backref('tracks'))
    album_id   = Column(Integer, ForeignKey('data_album.album_id'))
    album = relation(Album, backref=backref('albums'))
    genres = relation("Genre", secondary=track_genre, backref="tracks")

    def __init__(self, album_id = None, artist = None):
        self.album_id = album_id
        if isinstance(artist, int):
            self.artist_id = artist
        else:
            self.artist = artist

    def __repr__(self):
        return "<Track(%s): by %s in %d genres>" % ( self.album_id,
                self.artist_id, len(self.genres))
def main(args):
    import  optparse
    parser = optparse.OptionParser()
    parser.usage = __doc__
    parser.add_option("-q", "--quiet",
                      action="store_false", dest="verbose", default=True,
                      help="don't print status messages to stdout")
    (options, args) = parser.parse_args()

    if len(args) < 1:
        parser.error("Not enough arguments given")
    metadata.create_all(engine)
    return 0

if __name__ == "__main__":
    import sys
    sys.exit( main( sys.argv ) )
