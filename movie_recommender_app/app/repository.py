from sqlalchemy import create_engine, select, func, desc
from sqlalchemy.orm import Session
from app.models import User, Movie, Rating, Genres, Languages, Directors, Actors
class Repository:
    def __init__(self) -> None:
        self.engine = create_engine('postgresql+psycopg2://postgres:noi123456@noing-db.c2qkku433l07.ap-southeast-1.rds.amazonaws.com:5432/postgres')
        self.db_session = Session(self.engine)

    def login(self, username, password):
        current_user = self.db_session.query(User).filter(User.username==username and User.password==password).first()
        if current_user:
            return current_user
        else:
            return None
    
    def get_movies(self, limit=10):
        movies = self.db_session.query(Movie).limit(10).all()
        return movies
    
    def get_movie_by_id(self, id):
        movie = self.db_session.query(Movie).filter(Movie.movieId == id).first()
        return movie
    
    def add_user(self, username, passsword):
        max_user_id = self.db_session.query(func.max(User.userId)).scalar()
        user = User(userId=max_user_id+1, username=username, password=passsword)
        result = self.db_session.add(user)
        self.db_session.commit()
        if result:
            return True
        else: return False

    def get_top_rating_movie(self, limit=12):
        #get top rating movieID and average rating --> list[(movieId,avgRating),...]
        top_rating_moiveID = self.db_session.query(Rating.movieId, func.avg(Rating.rating).label('avgRating'),).group_by(Rating.movieId).order_by(desc('avgRating')).limit(12).all()
        movies = []
        for i in top_rating_moiveID:
            movies.append(self.get_movie_by_id(i.movieId))
        return movies

    def get_latest_movies(self, limit=12):
        lastest_movies = self.db_session.query(Movie).order_by(Movie.releaseDate.desc()).filter(Movie.releaseDate != "None" and Movie.releaseDate != "").limit(12).all()
        return lastest_movies
    
    def get_average_rating_movie_by_id(self, id):
        avgRating_movie = self.db_session.query(func.avg(Rating.rating).label('avgRating')).group_by(Rating.movieId).filter(Rating.movieId==id).first()
        return round(avgRating_movie[0],1)

    def get_genres_by_id(self, id):
        genres = self.db_session.query(Genres).filter(Genres.movieId==id).all()
        return genres

    def get_laguage_movie_by_id(self, id):
        languages = self.db_session.query(Languages).filter(Languages.movieId==id).all()
        return languages

    def get_actors_by_id(self, id):
        actors = self.db_session.query(Actors).filter(Actors.movieId==id).all()
        return actors

    def get_director_by_id(self, id):
        directors = self.db_session.query(Directors).filter(Directors.movieId==id).all()
        return directors