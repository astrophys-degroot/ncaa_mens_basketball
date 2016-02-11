from flask import Flask

application = Flask(__name__)
from flask_ncaa_mbb import views
