from flask import Flask
from flask_session import Session
app = Flask(__name__)
# app.config.from_object('config')
app.config['SESSION_PERNAMENT'] = False
app.config['SESSION_TYPE'] = 'filesystem'
Session(app)

from app import routes