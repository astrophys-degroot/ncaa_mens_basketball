
#importing a whole lot of packages
from flask import render_template, request
from flask_ncaa_mbb import app
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2
import pandas as pd
import numpy as np
from ModelBase import base_model, final_prob


#setting up the SQL connection
user = 'smaug'
host = 'localhost'
dbname = 'ncaa_mbb_db'
db = create_engine('postgres://%s%s/%s'%(user,host,dbname))
con = None
con = psycopg2.connect(database = dbname, user = user)


@app.route('/')


@app.route('/input')
def teams_input():
    return render_template("input.html")


@app.route('/output')
def teams_output():
    totry = np.arange(0,130,1)

    team1 = request.args.get('team1name')
    sql_query = '''
                SELECT pts, wl_int 
                FROM %s LEFT JOIN %s ON (%s.wl=%s.wl)
                WHERE team_name = '%s';
                ''' % ('teams1415', 'winloss', 'teams1415', 'winloss', team1)
    team1_sql = pd.read_sql_query(sql_query,con)
    team1_XS = list(team1_sql['pts'].values)   
    team1_XS = np.array([team1_XS]).transpose()
    team1_YS = list(team1_sql['wl_int'].values)    
    team1_YS = np.array(team1_YS)
    team1_prob = base_model(team1_XS, team1_YS, totry)

    
    team2 = request.args.get('team2name')
    sql_query = '''
                SELECT pts, wl_int 
                FROM %s LEFT JOIN %s ON (%s.wl=%s.wl)
                WHERE team_name = '%s';
                ''' % ('teams1415', 'winloss', 'teams1415', 'winloss', team2)
    team2_sql = pd.read_sql_query(sql_query,con)
    team2_sql = pd.read_sql_query(sql_query,con)
    team2_XS = list(team2_sql['pts'].values)   
    team2_XS = np.array([team2_XS]).transpose()
    team2_YS = list(team2_sql['wl_int'].values)    
    team2_YS = np.array(team2_YS)
    team2_prob = base_model(team2_XS, team2_YS, totry)

    myfinalprob = final_prob(team1_prob, team2_prob)

    mydict = {'mytry':totry, 'myfinalprob':myfinalprob}
    return render_template("output.html", team1=team1, team2=team2, mydict=mydict)



@app.route('/index')
def index():
    return 'Hello, big, bright, beautiful, bountiful, cosmos!'


@app.route('/db')
def team_page_fancy():
    sql_query = """
                SELECT pts, fgper, reb FROM teams1415 WHERE team_name='Wisconsin Badgers';          
                """
    query_results=pd.read_sql_query(sql_query,con)
    stats = []
    for i in range(0,query_results.shape[0]):
        stats.append(dict(pts=query_results.iloc[i]['pts'],
                          fgper=query_results.iloc[i]['fgper'],
                          reb=query_results.iloc[i]['reb']))
    return render_template('team.html',stats=stats)
