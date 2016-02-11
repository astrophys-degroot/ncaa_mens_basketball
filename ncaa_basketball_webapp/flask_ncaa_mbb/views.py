
#importing a whole lot of packages
from flask import render_template, request
from flask_ncaa_mbb import application
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2
import pandas as pd
import numpy as np
from ModelBase import base_model
import plotly.plotly as py
import plotly.tools as tls
import urllib2

#setting up the SQL connection
user = 'smaug'
host = 'localhost'
dbname = 'ncaa_mbb_db'
try:
    db = create_engine('postgres://%s%s/%s'%(user,host,dbname))
    con = None
    con = psycopg2.connect(database = dbname, user = user)
except:
    con = None
    db = None


@application.route('/')
def home_page():
    return render_template("index.html")


@application.route('/input', methods=['POST', 'GET'])
def teams_input():
    return render_template("input.html")


@application.route('/output', methods=['POST', 'GET'])
def teams_output():
    #totry = np.arange(0,130,1)

     
    team1_png = request.args.get('team1')
    if team1_png is None:
        team1_png = 'Ohio_State_Buckeyes'
    team1 = team1_png.replace('_', ' ')

    
    team2_png = request.args.get('team2')
    if team2_png is None:
        team2_png = 'Michigan_State_Spartans'
    team2 = team2_png.replace('_', ' ')


    win_dict = base_model(team1, team2)
    winprob = win_dict['prob']
    if winprob <= 0.5:
        winprob = int(round((1.0-winprob)*100.0))
        teamwin=team2_png
        winner = team2
        keys = win_dict['keys']
    else:
        winprob = int(round((winprob*100.0)))
        teamwin=team1_png
        winner = team1
        keys = win_dict['keys']


    myplot = tls.get_embed(win_dict['url'])
    myplot = myplot.replace('525', '400')
    myplot = myplot.replace('100%', '56%')
    return render_template("output.html", team1_png=team1_png, team2_png=team2_png,
                           teamwin=teamwin, winner=winner, myplot=myplot, winprob=winprob,
                           key1=keys[0],key2=keys[1],key3=keys[2])



@application.route('/slides', methods=['POST', 'GET'])
def slides_output():
    return render_template("slides.html")


@application.route('/index')
def index():
    return 'Hello, big, bright, beautiful, bountiful, cosmos!'


@application.route('/db')
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
