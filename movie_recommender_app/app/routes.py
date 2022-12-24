# coding=utf-8

from flask import render_template, flash, redirect, session, url_for, request, g, Markup
from app import app

@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html')

@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/detail')
def detail():
    return render_template('detail.html')

@app.route('/category')
def category():
    return render_template('category.html')

@app.route('/login', methods=['POST', 'GET'])
def login():
    if request.method == 'POST':
        session['username'] = request.form.get('username')
        return redirect('/')
    return render_template('login.html')