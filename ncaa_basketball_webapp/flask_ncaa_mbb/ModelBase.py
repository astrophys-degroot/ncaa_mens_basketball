#import our modules
#import sys
import pandas as pd
import numpy as np
import scipy as sp
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import plotly.plotly as py
import plotly.graph_objs as go

#import urllib
#import re
import pickle

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2

#import statsmodels.api as sm
#from patsy import dmatrices
#from sklearn import metrics
#from sklearn.linear_model import LogisticRegression
#from sklearn.cross_validation import train_test_split
#from sklearn.cross_validation import cross_val_score


#==============================================================================
#function to return the yearly stats
def calc_full_year(season_sql, season_sql_tags):
    
    full_year = {}
    for season_sql_tag in season_sql_tags:
    
        if season_sql_tag == 'pts':
            bdict = {'grmin':30.0, 'grmax':100, 'grbins':70, 
                     'grtitle':'Frequency of Total Points Scored', 
                     'grxlab':'Total Points Scored',
                     'cdfmin':0.0, 'cdfmax':130, 'cdfbins':130*5}
        elif season_sql_tag == 'reb':
            bdict = {'grmin':0.0, 'grmax':60, 'grbins':60, 
                     'grtitle':'Frequency of Total Rebounds', 
                     'grxlab':'Total Rebounds',
                     'cdfmin':0.0, 'cdfmax':70, 'cdfbins':70*5}
        elif season_sql_tag == 'oreb':
            bdict = {'grmin':0.0, 'grmax':20, 'grbins':20, 
                     'grtitle':'Frequency of Offensive Rebounds', 
                     'grxlab':'Offensive Rebounds',
                     'cdfmin':0.0, 'cdfmax':30, 'cdfbins':30*5}
        elif season_sql_tag == 'dreb':
            bdict = {'grmin':0.0, 'grmax':40, 'grbins':40, 
                     'grtitle':'Frequency of Defensive Rebounds', 
                     'grxlab':'Defensive Rebounds',
                     'cdfmin':0.0, 'cdfmax':50, 'cdfbins':50*5}
        elif season_sql_tag == 'to':
            bdict = {'grmin':0.0, 'grmax':30, 'grbins':30, 
                     'grtitle':'Frequency of Turnovers', 
                     'grxlab':'Total Turnovers',
                     'cdfmin':0.0, 'cdfmax':40, 'cdfbins':40*5}
        elif season_sql_tag == 'ast':
            bdict = {'grmin':0.0, 'grmax':30, 'grbins':30, 
                     'grtitle':'Frequency of Assists', 
                     'grxlab':'Total Assists',
                     'cdfmin':0.0, 'cdfmax':50, 'cdfbins':50*5}
        elif season_sql_tag == 'fga':
            bdict = {'grmin':30, 'grmax':100, 'grbins':70, 
                     'grtitle':'Frequency of Field Goals Attempted', 
                     'grxlab':'Total Field Goals Attempted',
                     'cdfmin':20.0, 'cdfmax':110, 'cdfbins':90*5}
        elif season_sql_tag == 'fgm':
            bdict = {'grmin':10, 'grmax':50, 'grbins':40, 
                     'grtitle':'Frequency of Field Goals Made', 
                     'grxlab':'Total Field Goals Made',
                     'cdfmin':0.0, 'cdfmax':70, 'cdfbins':70*5}
        elif season_sql_tag == 'fgper':
            bdict = {'grmin':0.0, 'grmax':1.0, 'grbins':100, 
                     'grtitle':'Frequency of Field Goals Percent', 
                     'grxlab':'Total Field Goals Percent',
                     'cdfmin':0.0, 'cdfmax':1.0, 'cdfbins':100*5}
        elif season_sql_tag == 'fta':
            bdict = {'grmin':0, 'grmax':30, 'grbins':30, 
                     'grtitle':'Frequency of Free Throws Attempted', 
                     'grxlab':'Total Free Throws Attempted',
                     'cdfmin':0.0, 'cdfmax':40, 'cdfbins':40*5}
        elif season_sql_tag == 'ftm':
            bdict = {'grmin':0, 'grmax':30, 'grbins':30, 
                     'grtitle':'Frequency of Free Throws Made', 
                     'grxlab':'Total Free Throws Made',
                     'cdfmin':0.0, 'cdfmax':30, 'cdfbins':30*5}
        elif season_sql_tag == 'ftper':
            bdict = {'grmin':0.0, 'grmax':1.0, 'grbins':100, 
                     'grtitle':'Frequency of Free Throw Percent', 
                     'grxlab':'Total Free Throw Percent',
                     'cdfmin':0.0, 'cdfmax':1.0, 'cdfbins':100*5}
        elif season_sql_tag == 'tpa':
            bdict = {'grmin':0.0, 'grmax':40, 'grbins':40, 
                     'grtitle':'Frequency of Three Pointers Attempted', 
                     'grxlab':'Total Three Pointers Attempted',
                     'cdfmin':0.0, 'cdfmax':50, 'cdfbins':50*5}
        elif season_sql_tag == 'tpm':
            bdict = {'grmin':0, 'grmax':20, 'grbins':20, 
                     'grtitle':'Frequency of Three Pointers Made', 
                     'grxlab':'Total Three Pointers Made',
                     'cdfmin':0.0, 'cdfmax':20, 'cdfbins':20*5}
        elif season_sql_tag == 'tpper':
            bdict = {'grmin':0.0, 'grmax':1.0, 'grbins':100, 
                     'grtitle':'Frequency of Three Pointer Percent', 
                     'grxlab':'Total Three Pointer Percent',
                     'cdfmin':0.0, 'cdfmax':1.0, 'cdfbins':100*5}
        elif season_sql_tag == 'stl':
            bdict = {'grmin':0, 'grmax':20, 'grbins':20, 
                     'grtitle':'Frequency of Steals', 
                     'grxlab':'Total Steals',
                     'cdfmin':0.0, 'cdfmax':30, 'cdfbins':30*10}
        elif season_sql_tag == 'pf':
            bdict = {'grmin':10, 'grmax':30, 'grbins':20, 
                     'grtitle':'Frequency of Personal Fouls', 
                     'grxlab':'Total Personal Fouls',
                     'cdfmin':0.0, 'cdfmax':50, 'cdfbins':250*5}
        elif season_sql_tag == 'blk':
            bdict = {'grmin':0.0, 'grmax':20.0, 'grbins':20.0, 
                     'grtitle':'Frequency of Blocked Shots', 
                     'grxlab':'Total Blocked Shots',
                     'cdfmin':0.0, 'cdfmax':30, 'cdfbins':30*10}

        else:
            bdict = {'grmin':0.0, 'grmax':50, 'grbins':50, 
                     'grtitle':'Frequency of Variable', 
                     'grxlab':'Variable',
                     'cdfmin':0.0, 'cdfmax':50, 'cdfbins':50*5}
 
        #calculate some bin arrays and add them 
        bins = ((bdict['grmax']-bdict['grmin']) / bdict['grbins']) 
        bins = bins * np.arange(bdict['grbins']+1) + bdict['grmin'] 
        bdict['bins'] = bins
        cdf_bins = ((bdict['cdfmax']-bdict['cdfmin']) / bdict['cdfbins']) 
        cdf_bins = cdf_bins * np.arange(bdict['cdfbins']+1) + bdict['cdfmin'] 
        bdict['cdf_bins'] = cdf_bins
        
        #fit some lognormal functions and add those parameters
        cur_samp = list(season_sql.loc[:,season_sql_tag])
        param = sp.stats.lognorm.fit(cur_samp) # fit the sample data
        bdict['param'] = param



        full_year[season_sql_tag] = bdict
    
    return full_year
#==============================================================================


#==============================================================================
def sample_plots(cur_samp, bins, bdict, pdf_fitted, 
                 first_name, first, second_name, second,
                var):

    #print bins
    n, bins, patches = plt.hist(cur_samp, bins, normed=1, facecolor='green', alpha=0.30)
    plt.title(bdict['grtitle'])
    plt.xlabel(bdict['grxlab'])
    plt.ylabel('Frequency')
    plt.grid(True)


    plt.plot(bins,pdf_fitted, 'b-', linewidth=2)
    plt.axvline(first, color='red', linewidth=2, label=first_name)
    plt.axvline(second, color='black', linewidth=2, label=second_name)

    plt.legend()
    #plt.show()
    filename = 'ncaa_basketball_webapp/flask_ncaa_mbb/static/images/????_plot.png'
    filename = filename.replace('????', var)
    plt.savefig(filename)
    plt.clf()
#==============================================================================


#==============================================================================
def final_plot(XS_first, XS_second, season_sql_tags, team1, team2):

    my_df_first = pd.DataFrame({'factors':XS_first, "index":np.arange(len(XS_first)), 'names':season_sql_tags})
    #print my_df_first
    #my_df_first['factors'] = -1.0*my_df_first['factors']
    #print my_df_first

    my_df_second = pd.DataFrame({'factors':XS_second, "index":np.arange(len(XS_second)), 'names':season_sql_tags})
    #print my_df_second



    trace0 = go.Bar(
        x=my_df_first['names'],
        y=my_df_first['factors'],
        name=team1,
        marker=dict(color='rgb(119,136,153)'),
        text=dict(
            family='sans serif',
            size=18,
            color='#ffffff'
        ),
        )
    
    trace1 = go.Bar(
        x=my_df_second['names'],
        y=my_df_second['factors'],
        name=team2,
        marker=dict(color='rgb(5,184,204)'),
        )
    
    data = [trace0, trace1]
    layout = go.Layout(
        #autosize=False, 
        #height=200,
        #width=300,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)', 
        xaxis=dict(tickangle=-45),
        yaxis=dict(range=[0.0,1.0],ticktext=[0.0,0.2,0.4,0.6,0.8,1.0]),
        font=dict(family='Courier New, monospace', size=18, color='#ffffff'),
        legend=dict(x=0.0,y=1.25),
        barmode='group',
    )
    fig = go.Figure(data=data, layout=layout)


    file_team1 = team1.replace(' ', '_')
    file_team2 = team2.replace(' ', '_')
    plotname =  file_team1 + '_' + file_team2 + '.png'
    filename = 'flask_ncaa_mbb/static/images/160206/' + plotname
    py.image.save_as({'data': data}, filename)

    
    try:
        plot_url = py.plot(fig, filename='ncaa_mbb_output', auto_open=False)
    except:
        plot_url = 'https://plot.ly/~astrophys.degroot/20/ohio-state-buckeyes-vs-michigan-state-spartans/'
    #print plot_url

    #plot_html = urllib.urlencode(fig)
    #print plot_html
    
    #filename = 'flask_ncaa_mbb/static/images/test_plot.jpeg'
    #plt.savefig(filename)
    #plt.clf()

    
    return plot_url
#==============================================================================



#==============================================================================
def translate_keys(keys):

    new_keys = []
    for key in keys:
        if key == 'pts':
            new_keys.append('Points')
        elif key == 'reb':
            new_keys.append('Rebounds')
        elif key == 'oreb':
            new_keys.append('Offensive Rebounds')
        elif key == 'dreb':
            new_keys.append('Defensive Rebounds')
        elif key == 'ast':
            new_keys.append('Assists')
        elif key == 'stl':
            new_keys.append('Steals')
        elif key == 'to':
            new_keys.append('Turnovers')
        elif key == 'pf':
            new_keys.append('Personal Fouls')
        elif key == 'blk':
            new_keys.append('Blocked Shots')
        elif key == 'fga':
            new_keys.append('Field Goal Attempts')
        elif key == 'fgm':
            new_keys.append('Field Goals Made')
        elif key == 'fgper':
            new_keys.append('Field Goal %')
        elif key == 'tpa':
            new_keys.append('3-Point Attemps')
        elif key == 'tpm':
            new_keys.append('3-Pointers Made')
        elif key == 'tpper':
            new_keys.append('3-Pointer %')
        elif key == 'ftper':
            new_keys.append('Free Throw %')
        else:
            new_keys.append('Unknown')

    
    return new_keys
#==============================================================================



#==============================================================================
def base_model(team1_name, team2_name):

    stat_allowed = ['ast', 'blk', 'dreb', 'fga', 'fgm', 'fta', 'ftm', 
                'min', 'oreb', 'to', 'tpa', 'tpm', 'ftper', 'fgper', 
                'tpper', 'pf', 'pts', 'reb', 'stl']
    #print stat_allowed
    stats_to_drop = ['index', 'ha', 'wl', 'game_id', 'team_name']
    #print stats_to_drop
    stats_to_use = ['pts', 'reb', 'dreb', 'oreb', 'to', 'ast', 'fga', 'fgm', 
                    'tpa', 'tpm', 
                    'fgper', 'ftper', 'tpper', 'stl', 'pf', 'blk', ]
    #print stats_to_use
    the_table = 'teams1516'

    print team1_name
    print team2_name
    #team1_name = 'Kentucky Wildcats'
    #team2_name = 'Akron Zips'

    username = 'smaug'
    dbname = 'ncaa_mbb_db'
    con = None
    con = psycopg2.connect(database=dbname, user=username)


    ###for the parameters requested get the full year statistics summaries and fits
    sql_cols = ''
    for ii in np.arange(len(stats_to_use)):
        if ii == 0:
            sql_cols = sql_cols + '"' + stats_to_use[ii] + '"'
        else:
            sql_cols = sql_cols + ', "' + stats_to_use[ii] + '"'
    
    sql_query = '''
                SELECT %s 
                FROM %s LEFT JOIN %s ON (%s.wl=%s.wl)
                ''' % (sql_cols, the_table, 'winloss', the_table, 'winloss')
    #print sql_query
    try:
        season_sql = pd.read_sql_query(sql_query, con)
        season_sql_tags = list(season_sql.columns.values)
    except:
        print '  team1 stats not obtained'
    #print season_sql.head(8)
    full_year_stats = calc_full_year(season_sql, season_sql_tags)
    #print full_year_stats
    
    
    ### get all unique game ids and dates of those games
    sql_query = '''
                SELECT DISTINCT(game_id), date
                FROM %s LEFT JOIN %s ON (CAST(%s.game_id as INT) = CAST(%s.id as INT)) 
                ''' % (the_table, 'games', the_table, 'games')
    #print sql_query
    #try:
    game_dates = pd.read_sql_query(sql_query, con)
    #except:
    #    print '  team stats table, %s, does not exist' % (the_table)
    #print game_dates.head(8)


    new_sql_cols = sql_cols + ', "date", "wl_int"'    
    sql_query = '''
                SELECT %s
                 FROM %s LEFT JOIN %s ON (CAST(%s.game_id as INT) = CAST(%s.id as INT)) 
                         LEFT JOIN %s ON (%s.wl=%s.wl)
                 WHERE team_name IN ('%s')
                 ORDER BY date DESC;
                ''' % (new_sql_cols, the_table, 'games', the_table, 
                       'games', 'winloss', the_table, 'winloss', team1_name)
    #print sql_query
    #try:
    team1_sql = pd.read_sql_query(sql_query, con)
    #print team1_sql.head(10)
    team1_ngames = len(team1_sql.iloc[:,0])
    if team1_ngames < 5:
        team1_use = team1_sql[0:]
    else:
        team1_use = team1_sql[0:5]


    sql_query = '''
                SELECT %s
                 FROM %s LEFT JOIN %s ON (CAST(%s.game_id as INT) = CAST(%s.id as INT)) 
                         LEFT JOIN %s ON (%s.wl=%s.wl)
                 WHERE team_name IN ('%s')
                 ORDER BY date DESC;
                ''' % (new_sql_cols, the_table, 'games', the_table, 
                       'games', 'winloss', the_table, 'winloss', team2_name)
    #print sql_query
    #try:
    team2_sql = pd.read_sql_query(sql_query, con)
    #print team2_sql.head(10)
    team2_ngames = len(team2_sql.iloc[:,0])
    if team2_ngames < 6:
        team2_use = team2_sql[0:]
    else:
        team2_use = team2_sql[0:5]

    if team1_use is not None and team2_use is not None:
        #find mean of last 5 games
        team1_use = team1_use.mean()
        team2_use = team2_use.mean()        

        XS_entry = []
        XS_first = []
        XS_second = []
        for season_sql_tag in season_sql_tags:

            #grab values
            bdict = full_year_stats[season_sql_tag]
            cur_samp = list(season_sql.loc[:,season_sql_tag])

            bins = bdict['bins']
            cdf_bins = bdict['cdf_bins']
            param = bdict['param']

            #calculate the fitted PDF
            pdf_fitted = sp.stats.lognorm.pdf(bins, param[0], loc=param[1], scale=param[2]) # fitted distribution
            cdf_fitted = sp.stats.lognorm.cdf(cdf_bins, param[0], loc=param[1], scale=param[2]) # fitted distribution


            #find the percentile difference using the CDF
            #print team1_use[season_sql_tag], team2_use[season_sql_tag]
            cdf_diff = [abs(team1_use[season_sql_tag]-ii) for ii in cdf_bins]
            cdf_min_ind = cdf_diff.index(min(cdf_diff))
            first = cdf_fitted[cdf_min_ind]
            cdf_diff = [abs(team2_use[season_sql_tag]-ii) for ii in cdf_bins]
            cdf_min_ind = cdf_diff.index(min(cdf_diff))
            second = cdf_fitted[cdf_min_ind]
            #print first, second, first - second
            XS_first.append(first)
            XS_second.append(second)
            XS_entry.append(first - second)

            #chk = sample_plots(cur_samp, bins, bdict, pdf_fitted, 
            #                    team1_name, team1_use[season_sql_tag], 
            #                    team2_name, team2_use[season_sql_tag], 
            #                    season_sql_tag)

        #make a final plot
        my_url = final_plot(XS_first, XS_second, season_sql_tags,
                            team1_name, team2_name)
        #print XS_entry

        my_model_again = pickle.load(open( "../models/model_logreg_16feature_1415.p", "r" ) )
        #my_model_again = pickle.load(open( "../models/model_rfc2_16feature_1415.p", "r" ) )
        #print my_model_again.coef_

        predicted = my_model_again.predict(XS_entry)
        #print '  The label that the model predicts:', predicted
        probs = my_model_again.predict_proba(XS_entry)
        #print '  The probability that the model predicts:', probs
        #print ''


        #this part is for getting 3 keys to the prediction
        my_model_again = pickle.load(open( "../models/model_logreg_16feature_1415.p", "r" ) )
        coeffs = my_model_again.coef_
        keys_to_game = []
        for ii in np.arange(len(coeffs[0])):
            keys_to_game.append(coeffs[0][ii] * XS_entry[ii])

        keys_df = pd.DataFrame(zip(season_sql_tags,keys_to_game), columns=['stat','weight'])
        keys_df = keys_df.sort_values('weight', axis=0, ascending=False)
        if probs[0,1] >= 0.5:
            keys = list(keys_df.iloc[0:3,0])
        else:
            keys = list(keys_df.iloc[-3:,0])

        new_keys = translate_keys(keys)
        
    return {'prob':probs[0,1], 'url':my_url, 'keys':new_keys}
#==============================================================================
