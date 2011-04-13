Required Packages
=================
- Django==1.2
- Fabric==1.0.1
- SQLAlchemy==0.6.6
- psycopg2==2.4

Usage
=====
``./importdata.py <datadirname>``

Imports data files to the currently select filename

``fabric select_sqlite``

Select sqlite database

``fabric select_psqltest``

Select postgresql database of test data

``fabric select_psql2011``

Select postgresql database of final data

``fabric select_mysql2011``

Select MySQ database of final data


The API can be accesed via:

``from kddcup2011 import *``

Tables
------
- tAlbum
- tArtist
- tGenre
- tRating
- tTrack
- tUser

A simple selection might look like::
  tArtist.select([tArtist.c.artist_id]).execute.fetchall()


SQLAlchemy Tutorials
--------------------
More information can be found at the SQLAlchemy website.

- `A SQL Expression Language Tutorial by SQLAlchemy <http://www.sqlalchemy.org/docs/core/tutorial.html#using-joins>`_
- `A step-by-step SQLAlchemy tutorial <http://www.rmunn.com/sqlalchemy-tutorial/tutorial.html>`_


Examples
--------

Return number of users in DB::
  >> tUser.count().execute().fetchall()
  [(1000990L,)]


Return number of albums in each genre (not including null)::
 >> s = select([tAlbumGenre.c.genre_id, func.count(1)]).group_by(tAlbumGenre.c.genre_id)
 >> res = conn.execute(s)
 >> res.fetchall()
 [(14741L, 571L),
  (17863L, 6077L),
  (19484L, 490L),
  (20638L, 496L),
  ...

There are also things like ``func.avg`` but remember to use these with the ``group_by`` function.

