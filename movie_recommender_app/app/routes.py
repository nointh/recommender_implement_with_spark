# coding=utf-8

from flask import render_template, flash, redirect, session, url_for, request, g, Markup, abort, request
from app import app
from app.repository import Repository

@app.route('/')
@app.route('/index')
def index():
    repository = Repository()
    movies = repository.get_movies(limit=12)
    top_rating_movies = repository.get_top_rating_movie(limit=12)
    latest_movies = repository.get_latest_movies(limit=12)
    cartoons = repository.get_cartoon_movie(limit=12)
    return render_template('index.html', movies=movies, top_rating_movies=top_rating_movies, latest_movies=latest_movies, cartoons=cartoons)

@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/detail/<int:id>')
def detail(id):
    repository = Repository()
    movie = repository.get_movie_by_id(id)
    if not movie:
        abort(404)
    rating = repository.get_average_rating_movie_by_id(id)
    genres = repository.get_genres_by_id(id)
    stars = repository.get_star_rating_movie_by_id(id)
    if session['user_id']:
        self_rating = repository.get_rating_by_user_movie(movieId=id, userId=session.get('user_id')) if session.get('user_id') else None
        pred_rating = repository.get_rating_predict(movieId=id, userId=session.get('user_id')) if session.get('user_id') else None

        recommend_movies = repository.get_movie_recommend_for_user(session['user_id'])
        return render_template('detail.html', movie=movie, rating=rating, genres=genres, stars=stars, recommend_movies=recommend_movies, self_rating=self_rating, pred_rating=pred_rating)
    return render_template('detail.html', movie=movie, rating=rating, genres=genres, stars=stars)


@app.route('/rate/<int:movie>', methods=['POST'])
def rate(movie):
    repository = Repository()
    if not session['user_id']:
        return redirect('/login')
    new_rating = request.form.get('rating')
    userId = session['user_id']
    movieId = movie
    rating = repository.get_rating_object_by_user_movie(userId=userId, movieId=movieId)
    if rating:
        repository.update_rating(rating=rating, new_rating=new_rating)
    else:
        repository.add_rating(userId=userId, movieId=movieId, rating=new_rating)
    return redirect(f'/detail/{movie}')


@app.route('/category')
def category():
    return render_template('category.html')

@app.route('/login', methods=['POST', 'GET'])
def login():
    if request.method == 'POST':
        repository = Repository()
        username = request.form.get('username')
        password = request.form.get('password')
        user = repository.login(username=username, password=password)
        if not user:
            return redirect('/login')
        session['user_id'] = user.userId
        session['username'] = user.username
        return redirect('/')
    return render_template('login.html')

@app.route('/signup', methods=['POST', 'GET'])
def signup():
    if request.method == 'POST':
        repository = Repository()
        try:
            username = request.form.get('username')
            password = request.form.get('password')
            user = repository.add_user(username, password)
        except Exception as e:
            return redirect('/signup')
        return redirect('/')
    return render_template('signup.html')

@app.route('/logout')
def logout():
    session.pop('username')
    session.pop('user_id')
    return redirect('/')