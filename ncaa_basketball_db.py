
# coding: utf-8

# # Notebook to make and test Postgres databases for NCAA_MBB project
# 
# 1. scoreboard table - table with all possible dates of men's college basketball games and whether that webpage has been obtained from ESPN
# 2. games table - created in ncaa_basketball_games notebook currently but tests still offered here
# 3. winloss table - simple table to make wins (1) and losses (-1) numerical

# In[94]:

#import packages

#basic packages
import sys
import re
import os

#data analysis packages
import numpy as np
import pandas as pd

#data visiualization packages
import seaborn as sns

#database packages
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2



# In[95]:

## class definition for the NCAA basketball database collection
'''
This class if for organizing, access, setting and evaluating
the databases that will be made for the NCAA basketball project
to predict winners of games from past peformances
'''

class NcaaBballDb():
    
    
    def __init__(self, find_tables=None, peek_tables=None):
        self.dbname = 'ncaa_mbb_db'
        self.username = 'smaug'

        if find_tables:
            self.find_tables = find_tables
        else:
            self.find_tables = False
        if peek_tables:
            self.peek_tables = peek_tables
        else:
            self.peek_tables = False
  


# In[96]:

## get attribute functions

def getDbName(self):
    '''
    function to return the database name
    '''    
    return self.dbname

def getUserName(self):
    '''
    function to return the database username
    '''    
    return self.username

def getTableNames(self):
    '''
    function to return the available database table names
    '''    
    return self.table_names

def getDbExist(self):
    '''
    function to return the status of the database engine
    '''
    return self.db_exist

def getDbEngine(self):
    '''
    function to return the actual database engine
    '''
    return self.db_engine


# In[97]:

## set attribute functions

def setTableNames(self, table_names):
    '''
    function to set the available database table names
    into the class object
    '''
    self.table_names = table_names

    
def setDbEngine(self, engine):
    '''
    function to set the database engine in the class attributes
    '''
    self.db_engine = engine
    
def setDbExist(self, exists):
    '''
    function to set whether the database exists or not
    '''
    self.db_exist = exists


# In[98]:

## print attribute functions

def printTableNames(self):
    '''
    function to nicely print out database table names 
    '''
    table_names = getTableNames(self)
    print '    Tables available:'
    for table_name in table_names:
        print '      ', table_name


def printEngineStatus(self):
    engine_exist = getDbExist(self)
    print '    Engine exists: %s' % (engine_exist)
    engine = getDbEngine(self)
    print '      The little engine that could: %s' % (engine) 



# In[99]:

def connectDb(self):
    '''
    function to establish connection with the PostgreSQL
    database.
    
    Note: all connections should be made through this
    function to ensure smooth usage
    '''
    
    dbname = getDbName(self)
    username = getUserName(self)
    
    con = None
    con = psycopg2.connect(database=dbname, user=username)

    return con


# In[100]:

def makeDbEngine(self):
    '''
    function to establish engine with PostgreSQl database
    so that additional tables can be made 
    '''
    
    try:
        ## connect to Postgres
        dbname = getDbName(self)
        username = getUserName(self)

        ## create and set 
        engine = create_engine('postgres://%s@localhost/%s'%(username, dbname))
        setDbEngine(self, engine)

        ## test if it exists
        db_exist = database_exists(engine.url)
        if not db_exist:
            create_database(engine.url)
        db_exist = database_exists(engine.url)
        setDbExist(self, db_exist)
        return 1
    except:
        return 0


# In[101]:

def findTables(self):
    '''
    function to store and print all tables in the database 
    '''

    try:
        #get values
        con = connectDb(self)
        dbname = getDbName(self)

        #do the SQL query
        sql_query = '''
                    SELECT table_schema, table_name
                      FROM %s.information_schema.tables
                      WHERE table_schema LIKE 'public'
                      ORDER BY table_schema,table_name;
                    ''' % (dbname)
        #print sql_query
        try:
            tables_sql = pd.read_sql_query(sql_query, con)
            if tables_sql is not None:
                exists = True
                setTableNames(self, tables_sql['table_name'])
        except:
            exists = False
        return 1
    except:
        return 0
    


# In[102]:

def peekTables(self, nhead=False):
    '''
    function to return the head of the SQL tables that exist
    
    '''
    
    ## set values
    if nhead is False:
        nhead = 5
    else:
        nhead = nhead
    
    try:
        ## database stuff
        dbname = getDbName(self)
        username = getUserName(self)
        table_names = getTableNames(self)
        con = None
        con = psycopg2.connect(database=dbname, user=username)

        ## print stuff
        for table_name in table_names:
            sql_query = '''
                        SELECT *
                          FROM %s
                          LIMIT %i;
                        ''' % (table_name, nhead)
            try:
                table_peek = pd.read_sql_query(sql_query, con)
                if table_peek is not None:
                    exists = True
                print '    Peek at the table named %r ' % table_name
                print table_peek
            except:
                exists = False
        return 1
    except:
        return 0


# In[ ]:




# In[ ]:




# In[103]:

### items between here and __main__() have not be brought into the class definition yet


# In[ ]:




# In[ ]:




# ## For creating, testing the base scoreboard database
#     * scoreboard database: info regarding the various ESPN scoreboard pages such as data and whether obtained
# 
# 

# In[104]:

def scoreboard_table(username, dbname, engine, lastdate):
        
    scoreboard_table_name = 'scoreboard'
    scoreboard_dir = 'scoreboard_pages/'
    scoreboard_file = 'ncaa_mbb_scoreboard_full_YYYYMMDD.txt'
    scoreboard_table_range = [['20021101','20030430'], 
                              ['20031101','20040430'], 
                              ['20041101','20050430'],
                              ['20051101','20060430'],
                              ['20061101','20070430'],
                              ['20071101','20080430'],
                              ['20081101','20090430'],
                              ['20091101','20100430'],
                              ['20101101','20110430'],
                              ['20111101','20120430'],
                              ['20121101','20130430'],
                              ['20131101','20140430'],
                              ['20141101','20150430'],
                              ['20151101',lastdate],
                               ] 
    
    try:
        #fire up the database engine
        engine = create_engine('postgres://%s@localhost/%s'%(username, dbname))
        db_exist = database_exists(engine.url)
        if not db_exist:
            create_database(engine.url)

        #create empty arrays to fill
        my_dates = []
        my_years = []
        my_months = []
        my_days = []

        #loop over the date ranges
        for scoreboard in scoreboard_table_range:
            dates_range = pd.date_range(start=scoreboard[0], end=scoreboard[1], freq='D')
            for date_range in dates_range:
                match = re.search('(\d\d\d\d)-(\d\d)-(\d\d)', str(date_range))
                my_dates.append(match.group(0))
                my_years.append(match.group(1))
                my_months.append(match.group(2))
                my_days.append(match.group(3))

        #find which files have already been downloaded
        in_hand = []
        for ii in np.arange(len(my_dates)):
            bit1 = scoreboard_file
            bit1 = bit1.replace('YYYY', my_years[ii])
            bit1 = bit1.replace('MM', my_months[ii])
            bit1 = bit1.replace('DD', my_days[ii])
            #print bit1

            bit2 = scoreboard_dir  
            if int(my_months[ii]) > 7:
                bit2 = str(my_years[ii]) + '-' + str(int(my_years[ii])+1) + '/'
            else:
                bit2 = str(int(my_years[ii])-1) + '-' + str(my_years[ii]) + '/'
            #print bit2

            line = 'ls ' + scoreboard_dir + bit2 + bit1
            #print line
            f = os.popen(line)
            try:
                f.readlines()[0]
                in_hand.append('yes')
            except:
                #print f.readlines()
                in_hand.append('no')

        #make a data frame of our info
        scoreboard_df = pd.DataFrame({'date':my_dates, 
                                      'year':my_years, 
                                      'month':my_months,
                                      'day':my_days,
                                      'in_hand':in_hand,        
                                    })
        ##################################################################
        ###are you really sure you want to rebuild the entire scoreboard database???
        scoreboard_df.to_sql(scoreboard_table_name, engine, if_exists='replace')
        ##################################################################
        created = 1
    except:
        created = 0
        
    return created


# In[105]:

def do_test_scoreboard(username, dbname):

    con = None
    con = psycopg2.connect(database=dbname, user=username)
    known_table = 'scoreboard'
    
    sql_query = "SELECT COUNT(*) FROM %s;" % (known_table)
    try:
        count_sql = pd.read_sql_query(sql_query, con)
        if count_sql is not None:
            exists = True
    except:
        exists = False
    print '    Table, %s, exists: %s' % (known_table, exists)

    if exists is True:
        print '      Total number of entries in %s: %i' % (known_table, count_sql.loc[0])

        sql_query = "SELECT * FROM %s;" % (known_table)
        try:
            all_sql = pd.read_sql_query(sql_query, con)
        except:
            a = 1
        print '      First 5 entries of %s: ' % (known_table)
        print all_sql.head(5)

        sql_query = "SELECT * FROM %s;" % (known_table)
        try:
            all_sql = pd.read_sql_query(sql_query, con)
        except:
            a = 1
        print '      Last 10 entries of %s: ' % (known_table)
        print all_sql.tail(10)

        sql_query = "SELECT DISTINCT(date) FROM %s;" % (known_table)
        try:
            distinct_sql = pd.read_sql_query(sql_query, con)
            ndistinct_date = len(distinct_sql)
        except:
            a = 1
        print '     %s distinct entries in %s.' % (ndistinct_date, known_table)
        #if count_sql.loc[0] == ndistinct_date:
        #    print '        All entries appear to be unique'


    print ''


# ## For creating, testing the games database
# * currently performed in the ncaa_basketball_games notebook
# * database with info regarding all games played
#         -filled with data scraped from scoreboard pages 

# In[106]:

def do_test_games(username, dbname):
    
    con = None
    con = psycopg2.connect(database=dbname, user=username)
    known_table = 'games'

    sql_query = "SELECT COUNT(*) FROM %s;" % (known_table)
    try:
        count_sql = pd.read_sql_query(sql_query, con)
        if count_sql is not None:
            exists = True
    except:
        exists = False
    print '    Table, %s, exists: %s' % (known_table, exists)

    sql_query = "SELECT COUNT(id) FROM %s WHERE in_hand='%s' AND id >320000000;" % (known_table, 'no')
    try:
        count_to_get = pd.read_sql_query(sql_query, con)
    except:
        print '  games table, %s, does not exist' % boxscore_table_name
    print '    Still have %i game pages left to download.' % (count_to_get.iloc[0])
        

    print ''


# ## For creating, testing the gamestats databases

# In[107]:

def do_test_gamestats(username, dbname, year):
        
    con = None
    con = psycopg2.connect(database=dbname, user=username)
    known_table = 'teams' + year 
        
    sql_query = "SELECT COUNT(*) FROM %s;" % (known_table)
    try:
        count_sql = pd.read_sql_query(sql_query, con)
        if count_sql is not None:
            exists = True
    except:
        exists = False
    print '    Table, %s, exists: %s' % (known_table, exists)

    sql_query = "SELECT DISTINCT(game_id) FROM %s;" % (known_table)
    try:
        count_to_get = pd.read_sql_query(sql_query, con)
        print '    There are %s distinct games in the %s table' % (len(count_to_get), known_table)
    except:
        print '  games table, %s, does not exist' % known_table
     
       
    sql_query = "SELECT * FROM %s;" % (known_table)
    try:
        all_sql = pd.read_sql_query(sql_query, con)
        print '      First 5 entries of %s: ' % (known_table)
        print all_sql.head(5)
        print '      Last 10 entries of %s: ' % (known_table)
        print all_sql.tail(10)
    except:
        a = 1
 
    print ''


# ## For creating, testing the winloss database
# * a simple table to turn string values of win(w) and loss(l) to intergers win(1) and loss(-1)

# In[108]:

def winloss_table(username, dbname, engine):
    
    winloss_table_name = 'winloss'
    
    try:
        #fire up the database engine
        engine = create_engine('postgres://%s@localhost/%s'%(username, dbname))
        db_exist = database_exists(engine.url)
        if not db_exist:
            create_database(engine.url)

        #the simple table that we need to make wins, losses numerical
        winloss_df = pd.DataFrame({'wl':['w','l'], 
                                   'wl_int':[1,-1],        
                                  }, index=[0,1])
        ##################################################################
        ###are you really sure you want to rebuild the winloss numerical database???
        winloss_df.to_sql(winloss_table_name, engine, if_exists='replace')
        ##################################################################
        created = 1
    except:
        created = 0
        
    return created


# In[109]:

def do_test_winloss(username, dbname):

    con = None
    con = psycopg2.connect(database=dbname, user=username)
    known_table = 'winloss'
    
    sql_query = "SELECT COUNT(*) FROM %s;" % (known_table)
    try:
        count_sql = pd.read_sql_query(sql_query, con)
        if count_sql is not None:
            exists = True
    except:
        exists = False
    print '    Table, %s, exists: %s' % (known_table, exists)

    if exists is True:
        print '      Total number of entries in %s: %i' % (known_table, count_sql.loc[0])

        sql_query = "SELECT * FROM %s;" % (known_table)
        try:
            all_sql = pd.read_sql_query(sql_query, con)
        except:
            a = 1
        print '      First 5 entries of %s: ' % (known_table)
        print all_sql.head(5)
    print ''


# In[110]:

def main(find_tables=False, peek_tables=False, 
         make_scoreboard=False, make_games=None, 
         make_gamestats=False, make_winloss=False, 
         make_test=False, 
         lastdate=None, year=None):
    
    

    myncaabball = NcaaBballDb(find_tables=find_tables, peek_tables=peek_tables)
    
    chk = makeDbEngine(myncaabball)
    if chk !=0:
        print printEngineStatus(myncaabball)
    else:
        print 'Make sure you have Postgres started!!'
        sys.exit(0)

    if myncaabball.find_tables:
        chk = findTables(myncaabball)
        if chk != 0:
            printTableNames(myncaabball)
    
    if myncaabball.peek_tables:
        chk = peekTables(myncaabball, nhead=5)
        
        
    
    
    ### task: make a function for table peeks
    
    sys.exit(0)
    ### below this line has not been migrated into the class definition yet
    
    
    if lastdate is None:
        lastdate = '20160204'
    else:
        if len(lastdate) != 8:
            print 'Variable lastdate must be a string of form YYYYMMDD'
        else:
            lastdate = str(lastdate)
    if year is None:
        year = '1516'
    else:
        if len(lastdate) != 4:
            print 'Variable year must be a string of form Y1Y1Y2Y2'
        else:
            year = str(year)

    
    
    #get available tables in the database
    if which_tables:
        avail_tables = my_tables(username, dbname)
        print avail_tables
        
    #get a peek at available tables
    if peek_tables:
        chk = peek_at_tables(avail_tables, username, dbname)
        
    #whether to work on scoreboard table
    if make_scoreboard:
        chk = scoreboard_table(username, dbname, engine, lastdate)
        if chk == 1:
            print '    Table, scoreboard, successfully created!'
        else:
            print '    Table, scoreboard, NOT created!'
    if make_test:
        chk = do_test_scoreboard(username, dbname)

       
    #whether to work on the games table
    if make_test:
        chk = do_test_games(username, dbname)
        
        
    #whether to work on the gamestats tables
    if make_test:
        chk = do_test_gamestats(username, dbname, year)

    
    #whether to work on winloss table
    if make_winloss:
        chk = winloss_table(username, dbname, engine)
        if chk == 1:
            print '    Table, winloss, successfully created!'
        else:
            print '    Table, winloss, NOT created!'
    if make_test:
        chk = do_test_winloss(username, dbname)



# In[111]:

# boilerplate to execute call to main() function
if __name__ == '__main__':
    main(find_tables=True, peek_tables=True, 
         make_scoreboard=False, make_winloss=False, 
         make_games=False, make_gamestats=False, 
         make_test=False, 
         lastdate='20160301', year='1516')


# In[ ]:




# In[ ]:




# # Everything below this line is a stand alone bit of code
# # but should be run with caution. 

# '''
# credential and the three main databases info 
#     2. games: 
#     3. stats: database with info from all games played
#         -filled with data scrapped from games pages
# 
# '''
# 
# 
# username = 'smaug'
# dbname = 'ncaa_mbb_db'
# 
# 
# boxscore_dir = 'boxscore_pages/'
# boxscore_file = 'ncaa_mbb_boxscore_DDDDDDDDD.txt'
# boxscore_table_name = 'games'
# 
# stats_table_name = 'stats'
# stats_table_name2 = 'stats2'
# stats_table_name1415 = 'stats1415'
# 
# teams_1415 = 'teams1415'
# 
# 

# con = None
# con = psycopg2.connect(database=dbname, user=username)
# print '  ', con
# 
# 
# sql_query = "SELECT COUNT(*) FROM %s;" % (scoreboard_table_name)
# print sql_query
# try:
#     from_sql_query = pd.read_sql_query(sql_query, con)
#     print from_sql_query
# except:
#     print '  scoreboard_table does not exist' 
# 
# sql_query = "SELECT COUNT(*) FROM %s WHERE in_hand='%s';" % (scoreboard_table_name, 'no')
# print sql_query
# try:
#     from_sql_query = pd.read_sql_query(sql_query, con)
#     print from_sql_query
# except:
#     print '  scoreboard_table does not exist' 
# 
# 
# 
# 

# # For testing out the gamestats database

# con = None
# con = psycopg2.connect(database=dbname, user=username)
# print '  ', con
# 
# 
# sql_query = "SELECT COUNT(*) FROM %s;" % (stats_table_name)
# print sql_query
# try:
#     from_sql_query = pd.read_sql_query(sql_query, con)
#     print from_sql_query.head(5)
# except:
#     print '  stats table, %s, does not exist' % stats_table_name
# 
# 
# #sql_query = "select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='%s';" % (stats_table_name)
# #sql_query = "SELECT player, player_url, team_name, player_pos FROM %s;" % (stats_table_name)
# sql_query = "SELECT player, team_name, pf, pts, ftm, fta, fgm FROM %s;" % (stats_table_name)
# try:
#     from_sql_query = pd.read_sql_query(sql_query, con)
#     print from_sql_query.head(10)
# except:
#     print '  stats table, %s, does not exist' % stats_table_name
# 
# 
#     
# 
# sql_query = "SELECT COUNT(*) FROM %s;" % (stats_table_name2)
# try:
#     from_sql_query = pd.read_sql_query(sql_query, con)
#     print from_sql_query.head(5)
# except:
#     print '  stats table, %s, does not exist' % stats_table_name2
# 
# 
# #sql_query = "select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='%s';" % (stats_table_name)
# #sql_query = "SELECT player, player_url, team_name, player_pos FROM %s;" % (stats_table_name)
# sql_query = "SELECT player, team_name, pf, pts, ftm, fta, fgm FROM %s;" % (stats_table_name2)
# 
# 
# print sql_query
# try:
#     from_sql_query = pd.read_sql_query(sql_query, con)
#     print from_sql_query.head(30)
# except:
#     print '  stats table, %s, does not exist' % stats_table_name2
# 
# 
# 
# 

# # For testing out the teams database(s)

# con = None
# con = psycopg2.connect(database=dbname, user=username)
# print '  ', con
# 
# 
# sql_query = "SELECT * FROM %s WHERE team_name = 'Texas Longhorns';" % (teams_1415)
# print sql_query
# try:
#     from_sql_query = pd.read_sql_query(sql_query, con)
#     print from_sql_query.head(45)
# except:
#     print '  teams table, %s, does not exist' % teams_1415
# 
# 

# con = None
# con = psycopg2.connect(database=dbname, user=username)
# print '  ', con
# 
# 
# sql_query = "SELECT * FROM %s;" % ('winloss')
# print sql_query
# try:
#     from_sql_query = pd.read_sql_query(sql_query, con)
#     print from_sql_query.head(5)
# except:
#     print '  stats table, %s, does not exist' % stats_table_name
# 
# 

# In[ ]:




# In[ ]:


