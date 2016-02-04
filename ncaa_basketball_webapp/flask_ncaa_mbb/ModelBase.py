#import our modules
import sys
import pandas as pd
import numpy as np
import scipy as sp
import matplotlib.pyplot as plt
import re
import pickle

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2

#import statsmodels.api as sm
#from patsy import dmatrices
from sklearn import metrics
from sklearn.linear_model import LogisticRegression
from sklearn.cross_validation import train_test_split
from sklearn.cross_validation import cross_val_score


#==============================================================================
#function to return the yearly stats
def calc_full_year(season_sql, season_sql_tags):
    
    full_year = {}
    for season_sql_tag in season_sql_tags:
    
        if season_sql_tag == 'pts':
            bdict = {'grmin':30.0, 'grmax':100, 'grbins':70, 
                     'grtitle':'Total Points Scored', 
                     'grxlab':'Total Points Scored',
                     'cdfmin':0.0, 'cdfmax':130, 'cdfbins':130*5}
        elif season_sql_tag == 'reb':
            bdict = {'grmin':0.0, 'grmax':60, 'grbins':60, 
                     'grtitle':'Total Rebounds', 
                     'grxlab':'Total Rebounds',
                     'cdfmin':0.0, 'cdfmax':70, 'cdfbins':70*5}
        elif season_sql_tag == 'oreb':
            bdict = {'grmin':0.0, 'grmax':20, 'grbins':20, 
                     'grtitle':'Offensive Rebounds', 
                     'grxlab':'Offensive Rebounds',
                     'cdfmin':0.0, 'cdfmax':30, 'cdfbins':30*5}
        elif season_sql_tag == 'dreb':
            bdict = {'grmin':0.0, 'grmax':40, 'grbins':40, 
                     'grtitle':'Defensive Rebounds', 
                     'grxlab':'Defensive Rebounds',
                     'cdfmin':0.0, 'cdfmax':50, 'cdfbins':50*5}
        elif season_sql_tag == 'to':
            bdict = {'grmin':0.0, 'grmax':30, 'grbins':30, 
                     'grtitle':'Turnovers', 
                     'grxlab':'Total Turnovers',
                     'cdfmin':0.0, 'cdfmax':40, 'cdfbins':40*5}
        elif season_sql_tag == 'ast':
            bdict = {'grmin':0.0, 'grmax':30, 'grbins':30, 
                     'grtitle':'Assists', 
                     'grxlab':'Total Assists',
                     'cdfmin':0.0, 'cdfmax':50, 'cdfbins':50*5}
        elif season_sql_tag == 'fga':
            bdict = {'grmin':30, 'grmax':100, 'grbins':70, 
                     'grtitle':'Field Goals Attempted', 
                     'grxlab':'Total Field Goals Attempted',
                     'cdfmin':20.0, 'cdfmax':110, 'cdfbins':90*5}
        elif season_sql_tag == 'fgm':
            bdict = {'grmin':10, 'grmax':50, 'grbins':40, 
                     'grtitle':'Field Goals Made', 
                     'grxlab':'Total Field Goals Made',
                     'cdfmin':0.0, 'cdfmax':70, 'cdfbins':70*5}
        elif season_sql_tag == 'fgper':
            bdict = {'grmin':0.0, 'grmax':1.0, 'grbins':100, 
                     'grtitle':'Field Goals Percent', 
                     'grxlab':'Total Field Goals Percent',
                     'cdfmin':0.0, 'cdfmax':1.0, 'cdfbins':100*5}
        elif season_sql_tag == 'fta':
            bdict = {'grmin':0, 'grmax':30, 'grbins':30, 
                     'grtitle':'Free Throws Attempted', 
                     'grxlab':'Total Free Throws Attempted',
                     'cdfmin':0.0, 'cdfmax':40, 'cdfbins':40*5}
        elif season_sql_tag == 'ftm':
            bdict = {'grmin':0, 'grmax':30, 'grbins':30, 
                     'grtitle':'Free Throws Made', 
                     'grxlab':'Total Free Throws Made',
                     'cdfmin':0.0, 'cdfmax':30, 'cdfbins':30*5}
        elif season_sql_tag == 'ftper':
            bdict = {'grmin':0.0, 'grmax':1.0, 'grbins':100, 
                     'grtitle':'Free Throw Percent', 
                     'grxlab':'Total Free Throw Percent',
                     'cdfmin':0.0, 'cdfmax':1.0, 'cdfbins':100*5}
        elif season_sql_tag == 'tpa':
            bdict = {'grmin':0.0, 'grmax':40, 'grbins':40, 
                     'grtitle':'Three Pointers Attempted', 
                     'grxlab':'Total Three Pointers Attempted',
                     'cdfmin':0.0, 'cdfmax':50, 'cdfbins':50*5}
        elif season_sql_tag == 'tpm':
            bdict = {'grmin':0, 'grmax':20, 'grbins':20, 
                     'grtitle':'Three Pointers Made', 
                     'grxlab':'Total Three Pointers Made',
                     'cdfmin':0.0, 'cdfmax':20, 'cdfbins':20*5}
        elif season_sql_tag == 'tpper':
            bdict = {'grmin':0.0, 'grmax':1.0, 'grbins':100, 
                     'grtitle':'Three Pointer Percent', 
                     'grxlab':'Total Three Pointer Percent',
                     'cdfmin':0.0, 'cdfmax':1.0, 'cdfbins':100*5}
        elif season_sql_tag == 'stl':
            bdict = {'grmin':0, 'grmax':20, 'grbins':20, 
                     'grtitle':'Steals', 
                     'grxlab':'Total Steals',
                     'cdfmin':0.0, 'cdfmax':30, 'cdfbins':30*10}
        elif season_sql_tag == 'pf':
            bdict = {'grmin':10, 'grmax':30, 'grbins':20, 
                     'grtitle':'Personal Fouls', 
                     'grxlab':'Total Personal Fouls',
                     'cdfmin':0.0, 'cdfmax':50, 'cdfbins':250*5}
        elif season_sql_tag == 'blk':
            bdict = {'grmin':0.0, 'grmax':20.0, 'grbins':20.0, 
                     'grtitle':'Blocked Shots', 
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
    plt.figure(figsize=(4,3), dpi=200)
    #ax = plt.axes([0.15, 0.2, 0.9, 0.9])
    n, bins, patches = plt.hist(cur_samp, bins, normed=1, facecolor='green', alpha=0.25)
    plt.title(bdict['grtitle'], fontsize=12)
    plt.xlabel(bdict['grxlab'], fontsize=10)
    plt.ylabel('Frequency', fontsize=10)
    plt.grid(True)


    plt.plot(bins,pdf_fitted, 'b-', linewidth=2)
    plt.axvline(first, color='red', linewidth=3, label=first_name)
    plt.axvline(second, color='black', linewidth=3, label=second_name)

    plt.legend(prop={'size':6})
    #plt.show()
    filename = 'flask_ncaa_mbb/static/images/????_plot.png'
    filename = filename.replace('????', var)
    plt.savefig(filename)
    plt.clf()
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
                ''' % (sql_cols, 'teams1415', 'winloss', 'teams1415', 'winloss')
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
                ''' % ('teams1415', 'games', 'teams1415', 'games')
    #print sql_query
    #try:
    game_dates = pd.read_sql_query(sql_query, con)
    #except:
    #    print '  team stats table, %s, does not exist' % ('teams1415')
    #print game_dates.head(8)


    new_sql_cols = sql_cols + ', "date", "wl_int"'    
    sql_query = '''
                SELECT %s
                FROM %s LEFT JOIN %s ON (CAST(%s.game_id as INT) = CAST(%s.id as INT)) 
                     LEFT JOIN %s ON (%s.wl=%s.wl)
                WHERE team_name IN ('%s')
                ORDER BY date DESC;
                ''' % (new_sql_cols, 'teams1415', 'games', 'teams1415', 
                                     'games', 'winloss', 'teams1415', 'winloss', team1_name)
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
                ''' % (new_sql_cols, 'teams1415', 'games', 'teams1415', 
                                     'games', 'winloss', 'teams1415', 'winloss', team2_name)
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
            XS_entry.append(first - second)

            #chk = sample_plots(cur_samp, bins, bdict, pdf_fitted, 
            #                    team1_name, team1_use[season_sql_tag], 
            #                    team2_name, team2_use[season_sql_tag], 
            #                    season_sql_tag)

        #print XS_entry

        my_model_again = pickle.load(open( "saved_model.p", "r" ) )
        #print my_model_again.coef_

        predicted = my_model_again.predict(XS_entry)
        #print '  The label that the model predicts:', predicted
        probs = my_model_again.predict_proba(XS_entry)
        #print '  The probability that the model predicts:', probs
        #print ''

    return probs[0,1]
#==============================================================================
