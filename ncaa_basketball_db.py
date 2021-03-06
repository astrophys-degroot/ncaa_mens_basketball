# 1. scoreboard table - table with all possible dates of men's college basketball games and whether
# that webpage has been obtained from ESPN
# 2. games table - created in ncaa_basketball_games notebook currently but tests still offered here
# 3. winloss table - simple table to make wins (1) and losses (-1) numerical

# import appropriate packages
import sys
import re
import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
# import seaborn as sns
# from sqlalchemy_utils import database_exists, create_database


# class definition for the NCAA basketball database collection
class NcaaBballDb(object):
    """
    This class if for organizing, access, setting and evaluating
    the databases that will be made for the NCAA basketball project
    to predict winners of games from past performances
    """
        
    def __init__(self, find_tables=None, peek_tables=None, make_scoreboard=None,
                 scoreboard_name=None):
        self.dbname = 'ncaa_mbb_db'
        self.username = 'smaug'
        self.con = None
        self.db_engine = None
        self.table_names = None

        if find_tables:
            self.find_tables = find_tables
        else:
            self.find_tables = False
        if peek_tables:
            self.peek_tables = peek_tables
        else:
            self.peek_tables = False
        if make_scoreboard:
            self.make_scoreboard = make_scoreboard
        else:
            self.make_scoreboard = False

        if scoreboard_name:
            self.scoreboard_name = scoreboard_name
        else:
            self.scoreboard_name = 'scoreboard'

    # get attribute functions
    def getdbname(self):
        """
        method to return the value stored in the attribute for the database name
        :return: a string, the data base name
        """
        return self.dbname

    def getusername(self):
        """
        method to return the value stored in the attribute for the database username
        :return: a string, the username for the database
        """
        return self.username

    def get_connect_db(self):
        """
        method to return the value stored in the attribute for the database connection
        :return: a connection object
        """
        return self.con

    def get_engine_db(self):
        """
        method to return the value stored in the attribute for the database engine
        :return: an engine object
        """
        return self.db_engine

    def get_table_names(self):
        """
        method to return the value stored in the attribute table_names for the database class
        :return: a list of strings where each entry is a table name
        """
        return self.table_names

    # set attribute methods
    def set_connect_db(self):
        """
        method to set the value of a PostgreSQL database connection into the attribute con
        Note: all connections should be made through this function to ensure smooth usage
        :return: None
        """
        try:
            self.con = psycopg2.connect(database=self.getdbname(), user=self.getusername())
            passing = True
        except:
            passing = False

        return passing

    def set_engine_db(self):
        """
        method to set the engine object in the attribute for database engine for later usage
        :return: a boolean
        """
        try:
            self.db_engine = create_engine('postgres://%s@localhost/%s' % (self.getusername(), self.getdbname()))
            passing = True
        except:
            passing = False

        return passing

    def set_table_names(self, table_names):
        """
        method to set the value for the attribute table_names as the available database table names into the class
        object
        :param table_names: a list of strings to be stored
        :return: None
        """
        self.table_names = table_names

    def names_of_tables(self):
        """
        method to query the names of all tables in the database
        """
        try:
            # SQL query text
            sql_query = """
                        SELECT table_schema, table_name
                          FROM %s.information_schema.tables
                          WHERE table_schema LIKE 'public'
                          ORDER BY table_schema,table_name;
                        """ % (self.getdbname())
            try:
                # do the SQL query
                tables_sql = pd.read_sql_query(sql_query, self.get_connect_db())
                if tables_sql is not None:
                    exists = True
                    self.set_table_names(tables_sql['table_name'])
            except:
                exists = False
        except:
            exists = False

        return exists

    def table_head(self, tablename, nhead=5):

        # SQL query text
        sql_query = """
                     SELECT *
                            FROM %s
                        LIMIT %i;
                     """ % (tablename, nhead)
        try:
            # do the SQL query
            queryrslt = pd.read_sql_query(sql_query, self.get_connect_db())
            if queryrslt is not None:
                passed = True
        except:
            passed = False

        return queryrslt

    def test_exists_db(self):
        # test if it exists
        db_exist = database_exists(engine.url)
        if not db_exist:
            create_database(engine.url)
        db_exist = database_exists(engine.url)
        self.setDbExist(db_exist)
        return passing
        # except:
        #     return 1


    def getLastDate(self):
        """
        function to return the last date to include in the
        various databases
        """    
        return self.lastdate

    def getYear(self):
        """
        function to return the year to do when building the
        various databases
        """    
        return self.year

    def getNhead(self):
        """
        function to return the (max) number of rows when looking
        at a SQL table. 
        """    
        return self.nhead

    def getTableNames(self):
        """
        function to return the available database table names
        """    
        return self.table_names

    def getDbExist(self):
        """
        function to return the status of the database engine
        """
        return self.db_exist
    
    def getDbEngine(self):
        """
        function to return the actual database engine
        """
        return self.db_engine
    
    def getDbCon(self):
        """
        function to return the connection to the database
        """
        return self.con

    def getScoreboardName(self):
        """
        function to return the database table name for scoreboards
        """
        return self.scoreboard_name

    
    ### set attribute functions
    def setDefaults(self, lastdate=None, year=None, nhead=None):
        """
        function to set some default values for this class
        """
        
        try:
            if lastdate is None:
                self.lastdate = '20160204'
            else:
                if len(lastdate) != 8:
                    print('Variable lastdate must be a string of form YYYYMMDD')
                else:
                    self.lastdate = str(lastdate)

            if year is None:
                self.year = '1516'
            else:
                if len(year) != 4:
                    print('Variable year must be a string of form Y1Y1Y2Y2')
                else:
                    self.year = str(year)

            if nhead is None:
                self.nhead = 6
            else:
                self.nhead = nhead
            return 0
        except:
            return 1
    

    def setDbExist(self, exists):
        """
        function to set whether the database exists or not
        """
        self.db_exist = exists

    # print attribute functions
    def printTableNames(self):
        """
        function to nicely print out database table names 
        """
        table_names = self.getTableNames()
        print('    Tables available:')
        for table_name in table_names:
            print('      ', table_name)

    def printEngineStatus(self):
        engine_exist = self.getDbExist()
        print('    Engine exists: %s' % (engine_exist))
        engine = self.getDbEngine()
        print('       The little engine that could: %s' % (engine))


    def peekTables(self, nhead=False):
        """
        function to return the head of the SQL tables that exist
        
        """
        ## set values
        if nhead is False:
            nhead = self.getNhead()
        else:
            nhead = nhead
        
        try:
            ## database stuff
            dbname = self.getDbName()
            username = self.getUserName()
            table_names = self.getTableNames()
            con = None
            con = psycopg2.connect(database=dbname, user=username)

            ## print stuff
            for table_name in table_names:
                sql_query = """
                            SELECT *
                              FROM %s
                              LIMIT %i;
                            """ % (table_name, nhead)
                try:
                    table_peek = pd.read_sql_query(sql_query, con)
                    if table_peek is not None:
                        exists = True
                    print('    Peek at the table named %r ' % table_name)
                    print(table_peek)
                    print('')
                except:
                    exists = False
            return 0
        except:
            return 1


# ## For creating, saving, testing the base scoreboard database
#     * scoreboard database: info regarding the various ESPN scoreboard pages such as data and whether obtained

def scoreboardTable(self):
    
    
    scoreboard_table_name = getScoreboardName(self)
    print(scoreboard_table_name)
    sys.exit(0)
    
    # needs to go to scoreboard class: scoreboard_dir = 'scoreboard_pages/'
    # needs to go to scoreboard class: scoreboard_file = 'ncaa_mbb_scoreboard_full_YYYYMMDD.txt'
    # needs to go to scoreboard class: scoreboard_table_range = [['20021101','20030430'], 
                              #['20031101','20040430'], 
                              #['20041101','20050430'],
                              #['20051101','20060430'],
                              #['20061101','20070430'],
                              #['20071101','20080430'],
                              #['20081101','20090430'],
                              #['20091101','20100430'],
                              #['20101101','20110430'],
                              #['20111101','20120430'],
                              #['20121101','20130430'],
                              #['20131101','20140430'],
                              #['20141101','20150430'],
                              #['20151101',lastdate],
                              # ] 

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


# In[81]:

### items between here and __main__() have not be brought into the class definition yet



# In[ ]:



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
# In[82]:

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
    print('    Table, %s, exists: %s' % (known_table, exists))

    if exists is True:
        print('      Total number of entries in %s: %i' % (known_table, count_sql.loc[0]))

        sql_query = "SELECT * FROM %s;" % (known_table)
        try:
            all_sql = pd.read_sql_query(sql_query, con)
        except:
            a = 1
        print('      First 5 entries of %s: ' % (known_table))
        print(all_sql.head(5))

        sql_query = "SELECT * FROM %s;" % (known_table)
        try:
            all_sql = pd.read_sql_query(sql_query, con)
        except:
            a = 1
        print('      Last 10 entries of %s: ' % (known_table))
        print(all_sql.tail(10))

        sql_query = "SELECT DISTINCT(date) FROM %s;" % (known_table)
        try:
            distinct_sql = pd.read_sql_query(sql_query, con)
            ndistinct_date = len(distinct_sql)
        except:
            a = 1
        print('     %s distinct entries in %s.' % (ndistinct_date, known_table))
        #if count_sql.loc[0] == ndistinct_date:
        #    print '        All entries appear to be unique'


    print('')


# ## For creating, testing the games database
# * currently performed in the ncaa_basketball_games notebook
# * database with info regarding all games played
#         -filled with data scraped from scoreboard pages 

# In[83]:

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
    print('    Table, %s, exists: %s' % (known_table, exists))

    sql_query = "SELECT COUNT(id) FROM %s WHERE in_hand='%s' AND id >320000000;" % (known_table, 'no')
    try:
        count_to_get = pd.read_sql_query(sql_query, con)
    except:
        print('  games table, %s, does not exist' % boxscore_table_name)
    print('    Still have %i game pages left to download.' % (count_to_get.iloc[0]))
        

    print('')


# ## For creating, testing the gamestats databases

# In[84]:

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
    print('    Table, %s, exists: %s' % (known_table, exists))

    sql_query = "SELECT DISTINCT(game_id) FROM %s;" % (known_table)
    try:
        count_to_get = pd.read_sql_query(sql_query, con)
        print('    There are %s distinct games in the %s table' % (len(count_to_get), known_table))
    except:
        print('  games table, %s, does not exist' % known_table)
     
       
    sql_query = "SELECT * FROM %s;" % (known_table)
    try:
        all_sql = pd.read_sql_query(sql_query, con)
        print('      First 5 entries of %s: ' % (known_table))
        print(all_sql.head(5))
        print('      Last 10 entries of %s: ' % (known_table))
        print(all_sql.tail(10))
    except:
        a = 1
 
    print('')


# ## For creating, testing the winloss database
# * a simple table to turn string values of win(w) and loss(l) to intergers win(1) and loss(-1)

# In[85]:

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


# In[86]:

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
    print('    Table, %s, exists: %s' % (known_table, exists))

    if exists is True:
        print('      Total number of entries in %s: %i' % (known_table, count_sql.loc[0]))

        sql_query = "SELECT * FROM %s;" % (known_table)
        try:
            all_sql = pd.read_sql_query(sql_query, con)
        except:
            a = 1
        print('      First 5 entries of %s: ' % (known_table))
        print(all_sql.head(5))
    print('')


# In[14]:

def main(find_tables=False, peek_tables=False, 
         make_scoreboard=False, scoreboard_name=None,
         make_games=None, 
         make_gamestats=False, make_winloss=False, 
         make_test=False, 
         lastdate=None, year=None, nhead=None):
    
    
    myncaabball = NcaaBballDb(find_tables=find_tables, peek_tables=peek_tables, 
                             make_scoreboard=make_scoreboard, scoreboard_name=scoreboard_name)
    
    chk = myncaabball.setDefaults(lastdate=lastdate, year=year,
                     nhead=nhead)
    if chk !=0:
        print('Defaults may not be set correctly!!')
    
    
    chk = myncaabball.makeDbEngine()
    print(chk)
    if chk == 0:
        print(myncaabball.printEngineStatus())
    else:
        print('Make sure you have Postgres started!!')
        sys.exit(0)

    if myncaabball.find_tables:
        chk = myncaabball.findTables()
        if chk == 0:
            myncaabball.printTableNames()
    
    if myncaabball.peek_tables:
        chk = myncaabball.peekTables()
          
    #whether to work on scoreboard table    
    #if myncaabball.make_scoreboard:
    #    chk = myncaabball.scoreboardTable()
    #    if chk == 1:
    #        print '    Table, scoreboard, successfully created!'
    #    else:
    #        print '    Table, scoreboard, NOT created!'
 
    
    ### task: add function to make the scoreboard table database 
    ### this function needs to be evalutated on how some it fits here to make and access the db
    ###  and some of if needs to go into the scoreboard class so that is can be built etc and then handed over
    


# In[15]:

# boilerplate to execute call to main() function
if __name__ == '__main__':
    main(find_tables=True, peek_tables=True, 
         make_scoreboard=True, make_winloss=False, 
         make_games=False, make_gamestats=False, 
         make_test=False, 
         lastdate='20160323', year='1516')


# In[ ]:




# In[ ]:




# # below this line has not been migrated into the class definition yet
# 
# 

#     
#     #get available tables in the database
#     #if which_tables:
#     #    avail_tables = my_tables(username, dbname)
#     #    print avail_tables
#         
#     #get a peek at available tables
#     #if peek_tables:
#     #    chk = peek_at_tables(avail_tables, username, dbname)
#         
#     if make_test:
#         chk = do_test_scoreboard(username, dbname)
# 
#        
#     #whether to work on the games table
#     if make_test:
#         chk = do_test_games(username, dbname)
#         
#         
#     #whether to work on the gamestats tables
#     if make_test:
#         chk = do_test_gamestats(username, dbname, year)
# 
#     
#     #whether to work on winloss table
#     if make_winloss:
#         chk = winloss_table(username, dbname, engine)
#         if chk == 1:
#             print '    Table, winloss, successfully created!'
#         else:
#             print '    Table, winloss, NOT created!'
#     if make_test:
#         chk = do_test_winloss(username, dbname)
# 
# 

# # Everything below this line is a stand alone bit of code
# # but should be run with caution. 

# """
# credential and the three main databases info 
#     2. games: 
#     3. stats: database with info from all games played
#         -filled with data scrapped from games pages
# 
# """
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



