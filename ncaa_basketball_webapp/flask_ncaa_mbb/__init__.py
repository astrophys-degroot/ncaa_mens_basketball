from flask import Flask

app = Flask(__name__)
from flask_ncaa_mbb import views
