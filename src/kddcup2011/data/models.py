from django.db import models

# Create your models here.

class User(models.Model):
    user_id = models.IntegerField(primary_key=True)

class Rating(models.Model):
    """
    type is one of
    1: Training
    2: Verification
    3: Testing (scores == NULL)
    """
    rating_id = models.IntegerField(primary_key=True)
    item_id = models.IntegerField()
    timestamp = models.DateTimeField()
    score = models.IntegerField(null=True)
    type = models.IntegerField()
    user = models.ForeignKey('User')

class Genre(models.Model):
    genre_id = models.IntegerField(primary_key=True)

class Artist(models.Model):
    artist_id = models.IntegerField(primary_key=True)

class Album(models.Model):
    album_id = models.IntegerField(primary_key=True)
    artist = models.ForeignKey('Artist', null=True)
    genres = models.ManyToManyField('Genre')

class Track(models.Model):
    track_id = models.IntegerField(primary_key=True)
    artist = models.ForeignKey('Artist', null = True)
    album = models.ForeignKey('Album', null = True)
    genres = models.ManyToManyField('Genre')



