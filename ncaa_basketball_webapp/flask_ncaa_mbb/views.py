
#importing a whole lot of packages
from flask import render_template, request
from flask_ncaa_mbb import app
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2
import pandas as pd
import numpy as np
from ModelBase import base_model


#setting up the SQL connection
user = 'smaug'
host = 'localhost'
dbname = 'ncaa_mbb_db'
db = create_engine('postgres://%s%s/%s'%(user,host,dbname))
con = None
con = psycopg2.connect(database = dbname, user = user)


@app.route('/')
def home_page():
    return render_template("index.html")



@app.route('/input')
def teams_input():
    return render_template("input.html")


@app.route('/output')
def teams_output():
    totry = np.arange(0,130,1)

    team1 = request.args.get('team1name')    
    team2 = request.args.get('team2name')
    winprob = base_model(team1, team2)
    winprob = str(winprob*100.0)[0:5] + '%'

    return render_template("output.html", team1=team1, team2=team2, winprob=winprob)



@app.route('/index')
def index():
    return 'Hello, big, bright, beautiful, bountiful, cosmos!'


@app.route('/db')
def team_page_fancy():
    sql_query = """
                SELECT pts, fgper, reb FROM teams1415 WHERE team_name='Michigan State Spartans';          
                """
    query_results=pd.read_sql_query(sql_query,con)
    stats = []
    for i in range(0,query_results.shape[0]):
        stats.append(dict(pts=query_results.iloc[i]['pts'],
                          fgper=query_results.iloc[i]['fgper'],
                          reb=query_results.iloc[i]['reb']))
    return render_template('team.html',stats=stats)
