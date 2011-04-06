Required Packages
=================
 * Django==1.2
 * Fabric==1.0.1
 * SQLAlchemy==0.6.6
 * psycopg2==2.4

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


The API can be accesed via:

``from kddcup2011 import *``

Tables
------
   * tAlbum
   * tArtist
   * tGenre
   * tRating
   * tTrack
   * tUser

 A simple selection might look like::
   tArtist.select([tArtist.c.artist_id]).execute.fetchall()

More information can be found at the SQLAlchemy website.

