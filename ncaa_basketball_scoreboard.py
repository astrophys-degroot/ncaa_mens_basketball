
# coding: utf-8

# 
# # ESPN Scoreboard Page Data Grab
# 
# 
# ## Available games back until 2002-2003 season
#   * url format is pretty standard, is straight forward to get
#   * formatting appears to be pretty standard
#   * queries database on files that we don't have in hand yet and loop through them to try and get
# 

# In[1]:

#import our libraries
import sys
import os
import time
import psycopg2
import pandas as pd
import numpy as np
import urllib2
import re
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database


# In[2]:

## class defintion for the NCAA basketball scoreboard object
'''
This class is intened to work with a ESPN scoreboard page including
obtaining it, saving it, scraping info from it. 
'''

class NcaaBballScoreboard():
    
    def __init__():
        a = 1


# In[2]:

def get_yester():
    '''
    funtion to get yesterday's date
    '''
    
    yesterday = str(int(time.strftime("%d")) - 1)
    alternate = time.strftime("%Y%m") + yesterday

    return alternate


# In[3]:

def get_yester_pandas(pandas_date):
    
    year_str = str(pandas_date.year)

    if pandas_date.month < 10:
        month_str = '0' + str(pandas_date.month)
    else:
        month_str = str(pandas_date.month)

    if pandas_date.day < 10:
        day_str = '0' + str(pandas_date.day)
    else:
        day_str = str(pandas_date.day)

    yester = year_str + month_str + day_str

    return yester


# In[4]:

def get_yester_url(baseurl, alter, yester):
    '''
    function that takes a template base url string and replaces
    the string bit indicated by alter with the string bit 
    indicated by yester
    '''
    print '  Now looking for games on date %s.' % yester
    yesterurl = baseurl.replace(alter, yester)

    return yesterurl


# In[5]:

def get_yester_filename(baseurl, alter, yester):
    
    yesterurl = baseurl.replace(alter, yester)

    return yesterurl


# In[6]:

def get_yester_meddir(base_dir, this_date):
    
    match = re.search('(\d\d\d\d)-(\d\d)-(\d\d)', str(this_date))
    my_year = (match.group(1))
    my_month = (match.group(2))
    my_day = (match.group(3))

    if int(my_month) > 7:
        bit2 = str(my_year) + '-' + str(int(my_year)+1) + '/'
    else:
        bit2 = str(int(my_year)-1) + '-' + str(my_year) + '/'
    bit2 = base_dir + bit2
 
    return bit2


# In[7]:

def scoreboard_db_open(dbname, username):
    
    print 'username:', username
    print 'dbname:', dbname
    con = None
    con = psycopg2.connect(database=dbname, user=username)
    #print '  ', con

    return con


# In[8]:

def scoreboard_db_query(my_con, scoreboard_table_name):

    print 'table name:', scoreboard_table_name
    sql_query = "SELECT date FROM " + scoreboard_table_name + " WHERE in_hand = 'no'; "
    #print sql_query
    try:
        from_sql_query = pd.read_sql_query(sql_query, my_con)
        #print from_sql_query
    except:
        print '  scoreboard_table does not exist' 

    return from_sql_query


# In[60]:

def scoreboard_db_query_most_recent(my_con, scoreboard_table_name):

    handval = 'yes'
    sql_query = '''
                SELECT date 
                  FROM %s 
                  WHERE in_hand = %r
                  ORDER BY date DESC
                  LIMIT 1; 
                ''' % (scoreboard_table_name, handval)
    
    #print sql_query
    try:
        from_sql_query = pd.read_sql_query(sql_query, my_con)
        recent_date = from_sql_query['date'][0]
        #print from_sql_query
    except:
        print '  scoreboard_table does not exist' 

    return recent_date


# In[61]:

def get_yester_gamepage(yesterurl, my_con, udate, write=None, writedir=None):

    if writedir is not None:
        writedir = str(writedir)
    else:
        writedir = ''
 
    headers = {'User-Agent':'Mozilla/5.0'}
    req = urllib2.Request(yesterurl, None, headers)
    try:
        html = urllib2.urlopen(req).read()
        soup = BeautifulSoup(html,'lxml')
        #print '    ', soup[0:100]
        daypage = soup.prettify(encoding='utf-8')
        
        try:
            cur = my_con.cursor()
            #print cur
            #print udate
            cur.execute("UPDATE scoreboard SET in_hand=%s WHERE date=%s", ('yes', udate))        
            my_con.commit()
        except psycopg2.DatabaseError, e:
            if my_con:
                my_con.rollback()
            print 'Error %s' % e    
        find = 1
    except:
        find = 0
    
    if (write is not None) and (find == 1):
        target = open(writedir + write, 'w')
        target.write(daypage)
        target.close()
   
    return find


# In[64]:

def main(most_recent=False):
   
    print 'Now running: ', sys.argv[0]
   
    ###input parameters
    #baseurl = 'http://espn.go.com/mens-college-basketball/scoreboard/_/group/50/date/YYYYMMDD' #top 25 only
    baseurl = 'http://scores.espn.go.com/mens-college-basketball/scoreboard/_/group/50/date/YYYYMMDD' # all games
    #basefile = 'ncaa_mbb_scoreboard_YYYYMMDD.txt'  #top 25 only
    basedir = 'scoreboard_pages/'
    basefile = 'ncaa_mbb_scoreboard_full_YYYYMMDD.txt' # all games
    alter = 'YYYYMMDD'

    username = 'smaug'
    dbname = 'ncaa_mbb_db'
    scoreboard_table_name = 'scoreboard'

    #open connection to database
    my_con = scoreboard_db_open(dbname, username)
    remaining_dates = scoreboard_db_query(my_con, scoreboard_table_name)
    if most_recent is not False:
        rec_date = scoreboard_db_query_most_recent(my_con, scoreboard_table_name)
        print '  Most recent date (YYYY-MM-DD) in %s table is %s' % (scoreboard_table_name, rec_date)



    ##try to get the remaining dates
    print ''
    #print remaining_dates['date']
    remaining_dates = remaining_dates['date']
    print '  %i dates need to be pulled.' % len(remaining_dates)
    for remaining_date in reversed(remaining_dates):
        this_dir = get_yester_meddir(basedir, remaining_date)
        #print this_dir
        yester = remaining_date.replace('-','')
        url_yester_games = get_yester_url(baseurl, alter, yester)
        #print '  ', url_yester_games
        file_yester_gamepage = get_yester_filename(basefile, alter, yester)
        #print file_yester_gamepage
        yester_gamepage = get_yester_gamepage(url_yester_games, my_con, remaining_date, write=file_yester_gamepage, writedir=this_dir)
        print '  Length of previous day page:', yester_gamepage



        #sql_query = "SELECT in_hand FROM %s WHERE date='%s';" % (scoreboard_table_name, remaining_date)
        #print sql_query
        #from_sql_query = pd.read_sql_query(sql_query, my_con)
        #print from_sql_query


# In[65]:

# boilerplate to execute call to main() function
if __name__ == '__main__':
    main(most_recent=True)


# In[ ]:




# In[ ]:



