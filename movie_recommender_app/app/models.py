from sqlalchemy.orm import declarative_base
from sqlalchemy import Table, Column, Integer, String, Float

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    userId = Column(Integer, primary_key=True)
    username = Column(String)
    password = Column(String)

class Movie(Base):
    __tablename__ = 'movies'
    movieId = Column(Integer, primary_key=True)
    title = Column(String)
    plotSummary = Column(String)
    releaseYear = Column(String)
    releaseDate = Column(String)
    posterPath = Column(String)
    mpaa = Column(String)
    languages = Column(String)
    directors = Column(String)
    actors = Column(String)
    genres = Column(String)
    
    def get_languages(self):
        return self.languages.split('|')

    def get_genres(self):
        return self.genres.split('|')
    def get_directors(self):
        return self.directors.split('|')
    def get_actors(self):
        return self.actors.split('|')


class Rating(Base):
    __tablename__ = 'ratings'
    movieId = Column(Integer, primary_key=True)
    userId = Column(Integer, primary_key=True)
    rating = Column(Float)

class Genres(Base):
    __tablename__ = 'genres'
    movieId = Column(Integer, primary_key=True)
    genre = Column(String, primary_key=True)

class Languages(Base):
    __tablename__ = 'languages'
    movieId = Column(Integer, primary_key=True)
    language = Column(String, primary_key=True)


class Directors(Base):
    __tablename__ = 'directors'
    movieId = Column(Integer, primary_key=True)
    director = Column(String, primary_key=True)

class Actors(Base):
    __tablename__ = 'actors'
    movieId = Column(Integer, primary_key=True)
    actor = Column(String, primary_key=True)