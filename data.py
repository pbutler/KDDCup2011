#!/usr/bin/env python

from sqlalchemy import *
from sqlalchemy.orm import sessionmaker, relation, backref,create_session
from sqlalchemy.ext.declarative import declarative_base
import datetime

db = create_engine('sqlite:///:memory:')
metadata = MetaData()
session = create_session(bind=db) #, autocommit=True, autoflush=False)

Base = declarative_base(db, metadata)

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

class User(Base,SQLData):
    __tablename__ = 'users'
    user_id = Column(Integer, primary_key=True)

    def __init__(self, user_id): #title=None, num_words=None, time=None, hash=None):
        self.user_id = user_id

    def __repr__(self):
        return "<User: %d>" % self.user_id


#class Word(Base, SQLData):
#    __tablename__ = 'words'
#    word_id = Column(Integer, primary_key=True)
#    word_string = Column(String(200))
#
#    def __init__(self, word_string = None):
#        self.word_string = word_string
#
#    def __repr__(self):
#        return "<Word: %s(%d)>" % (self.word_string, self.word_id)

class Rating(Base, SQLData):
    __tablename__ = 'ratings'
    rating_id  = Column(Integer, primary_key=True, autoincrement=True)
    item_id  = Column(Integer) #, primary_key=True)
    time          = Column(DateTime)
    score = Column(Integer)
    user_id   = Column(Integer, ForeignKey('users.user_id'))
    user = relation(User, backref=backref('ratings'))

    def __init__(self, item_id = None, time = None, score= None, user=None):
        self.rating_id = None #rating_id
        self.item_id = item_id
        self.time = time
        self.score = score
        self.user = user

    def __repr__(self):
        return "<Rating(%s): %s of %s @ %s>" % (self.rating_id, self.item_id, self.score, self.time)

def readData(file):
    data = open(file)

    for line in data:
        userid, nratings = line.strip().split("|")
        u = User(userid)
        u.add()
        print userid
        for i in range(int(nratings)):
            line = data.next()
            item, score, day, time = line.strip().split("\t")
            hour, minu, sec = time.split(":")
            dt = datetime.datetime.min + datetime.timedelta(int(day))
            dt = dt.replace(hour = int(hour), minute = int(minu), second = int(sec))
            r = Rating(int(item), dt, int(score), u)
            r.add()
            print item, score, dt
        session.flush()

if __name__ == "__main__":
    metadata.create_all(db)
    readData("testdata/trainIdx1.firstLines.txt")
    print len(list(User.select())), len(list(Rating.select()))
