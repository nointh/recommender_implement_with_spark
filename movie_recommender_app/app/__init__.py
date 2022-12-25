from flask import Flask
app = Flask(__name__)
# app.config.from_object('config')
app.secret_key = '205de885fa5da1129b6ed4180780cbdb'

from app import routes