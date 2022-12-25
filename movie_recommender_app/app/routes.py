# coding=utf-8

from flask import render_template, flash, redirect, session, url_for, request, g, Markup, abort, request
from app import app
from app.repository import Repository

@app.route('/')
@app.route('/index')
def index():
    repository = Repository()
    movies = repository.get_movies(limit=10)
    return render_template('index.html', movies=movies)

@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/detail/<int:id>')
def detail(id):
    repository = Repository()
    movie = repository.get_movie_by_id(id)
    if not movie:
        abort(404)
    return render_template('detail.html', movie=movie)

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