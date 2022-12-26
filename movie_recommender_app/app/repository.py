from sqlalchemy import create_engine, select, func, desc
from sqlalchemy.orm import Session
from app.models import User, Movie, Rating, Genres, Languages, Directors, Actors
from google.cloud import bigquery

class Repository:
    def __init__(self) -> None:
        self.engine = create_engine('postgresql+psycopg2://postgres:noi123456@noing-db.c2qkku433l07.ap-southeast-1.rds.amazonaws.com:5432/postgres')
        self.db_session = Session(self.engine)
        self.bq_client = bigquery.Client()

    def login(self, username, password):
        current_user = self.db_session.query(User).filter(User.username==username and User.password==password).first()
        if current_user:
            return current_user
        else:
            return None
    
    def get_movies(self, limit=12):
        movies = self.db_session.query(Movie).limit(limit).all()
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
    
    def get_rating_by_user_movie(self, userId, movieId):
        rating = self.db_session.query(Rating).filter(Rating.movieId == movieId and Rating.userId == userId).first().rating
        if not rating:
            return None
        return rating
    
    def add_rating(self, userId, movieId, rating):
        rating = Rating(userId=userId, movieId=movieId, rating=rating)
        result = self.db_session.add(rating)
        self.db_session.commit()
        if result:
            return True
        else: return False

    def get_rating_predict(self, userId, movieId):
        sql_query = f'select predict from `sonorous-reach-371710.recommenders.mf_recommender` where user = {userId} and movie = {movieId} limit 1'
        query_job = self.bq_client.query(sql_query)
        for row in query_job.result():
            pred = row[0]
        return pred
    

    def update_rating(self, rating: Rating, new_rating):
        rating.rating = new_rating
        self.db_session.commit()
        return True

    def get_top_rating_movie(self, limit=12):
        #get top rating movieID and average rating --> list[(movieId,avgRating),...]
        top_rating_moiveID = self.db_session.query(Rating.movieId, func.avg(Rating.rating).label('avgRating'),).group_by(Rating.movieId).order_by(desc('avgRating')).limit(limit).all()
        movies = []
        for i in top_rating_moiveID:
            movies.append(self.get_movie_by_id(i.movieId))
        return movies

    def get_latest_movies(self, limit=12):
        lastest_movies = self.db_session.query(Movie).order_by(Movie.releaseDate.desc()).filter(Movie.releaseDate != "None" and Movie.releaseDate != "").limit(limit).all()
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
    
    def get_cartoon_movie(self, limit=12):
        cartoons = self.db_session.query(Movie).join(Genres, Movie.movieId==Genres.movieId).filter(Genres.genre=='Animation').limit(limit).all()
        return cartoons

    def get_star_rating_movie_by_id(self,id):
        ratings = self.db_session.query(Rating.rating).filter(Rating.movieId==id).all()
        list_rating = list(*zip(*ratings))
        result = []
        #result = [(1 star, ratio %, number of 1 star), ..., (5 star, ratio %, number of 5 star), number of vote]
        for i in range(5):
            star = i+1
            ratio = round((list_rating.count(i+0.5)/len(list_rating))*100,2)+round((list_rating.count(i+1)/len(list_rating))*100,2)
            count = list_rating.count(i+0.5)+list_rating.count(i+1)
            tuple_temp = (star,ratio,count) 
            result.append(tuple_temp)
        result.append(len(list_rating))
        return result
    
    def get_movie_recommend_for_user(self, user_id, limit=12):
        

        sql_query = f'select movie, predict from `sonorous-reach-371710.recommenders.mf_recommender` where user = {user_id} order by predict desc limit {limit}'
        movies = []
        query_job = self.bq_client.query(sql_query)
        for row in query_job.result():
            movies.append(self.get_movie_by_id(row[0]))
        return movies
    