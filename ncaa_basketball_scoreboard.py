# import appropriate modules
# import sys
# import re
import time
import psycopg2
import pandas as pd
import urllib3
from bs4 import BeautifulSoup
from ncaa_basketball_db import *
# import os
# import numpy as np
# from sqlalchemy import create_engine
# from sqlalchemy_utils import database_exists, create_database


class NcaaBballScoreboard:
    """
    This class is intended to work with a ESPN scoreboard page including
    obtaining it, saving it, scraping info from it.

    Notes:
        * Available games back until 2002-2003 season
        * url format is pretty standard, is straight forward to get
        * formatting for individual scoreboards also appears to be pretty standard
        * queries database on files that we don't have in hand yet and loop through them to try and get

    """

    def __init__(self, baseurl=None, basedir=None, basefile=None, basealter=None):
        if baseurl is None:
            self.baseurl = 'http://scores.espn.go.com/mens-college-basketball/scoreboard/_/group/50/date/YYYYMMDD'
        else:
            self.baseurl = baseurl
        if basedir is None:
            self.basedir = 'scoreboard_pages/'
        else:
            self.basedir = basedir
        if basefile is None:
            self.basefile = 'ncaa_mbb_scoreboard_full_YYYYMMDD.txt'
        else:
            self.basefile = basefile   
        if basealter is None:
            self.basealter = 'YYYYMMDD'
        else:
            self.basealter = basealter

    # the class get functions
    def get_baseurl(self):
        """
        function to return the template url that needs to be slightly
        modified by entering a data to get to the boxscore page for a
        game.

        :return: string, the base url
        """
        return self.baseurl

    def get_basedir(self):
        """
        function to return the base directory for storing the raw
        scoreboard pages

        :return: string, the directory
        """
        return self.basedir
    
    def get_basefile(self):
        '''
        function to return the template file name for the stored
        raw scoreboard pages
        '''
        return self.basefile
    
    def get_basealter(self):
        '''
        function to return the string that needs to be altered in
        the basenames 
        '''
        return self.basealter

    def get_n_remaining(self):
        '''
        function to return the number of scoreboard webpages not
        yet obtained
        '''
        return self.remaining

    # other functions
    def yesterday_date():
        '''
        funtion to get yesterday's date in format YYYYMMDD

        It takes into account months with variable number of days.
        It takes into account leap years.
        It takes into account Jan 1st. 
        '''

        day = (time.strftime("%d"))
        #day = '01'
        month = (time.strftime("%m"))
        #month = '03'
        year = (time.strftime("%Y"))
        #year = '2016'

        if day != '01':
            if day <= '10':
                day = '0' + str(int(day) - 1)
            else:
                day = str(int(day) - 1)
        else:
            if month <= '10':
                month = '0' + str(int(month) - 1)
            else:
                month = str(int(month) - 1)
            if month in ['04','06','09','11']:
                day = '30'
            elif month in ['02']:
                if (int(year)-2000) % 4 == 0:
                    day = '29'
                else:
                    day = '28'
            else:
                if month == '00':
                    year = str(int(year) - 1)
                    month = '12'
                day = '31'

        alternate = year + month + day
        return alternate

    
    def sbDbQueryHm(self, dbobject):
    
        try:
            table_name = dbobject.getScoreboardName()
            print('  Table name:', table_name)
            sql_query = '''
                        SELECT COUNT(date) 
                          FROM %s
                          WHERE in_hand ILIKE 'no'
                        ''' % (table_name)

            try:
                from_sql = pd.read_sql_query(sql_query, dbobject.getDbCon())
                self.remaining = from_sql['count'].iloc[0]
                return 1
            except:
                print('  WARNING!! Error occured query number of dates not obtained')
                self.remaining = None
                return 0
        except:
            return 0

# items between here and __main__() have not be brought into the class definition yet

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


def get_yester_url(baseurl, alter, yester):
    '''
    function that takes a template base url string and replaces
    the string bit indicated by alter with the string bit 
    indicated by yester
    '''
    print('  Now looking for games on date %s.' % yester)
    yesterurl = baseurl.replace(alter, yester)

    return yesterurl


def get_yester_filename(baseurl, alter, yester):
    
    yesterurl = baseurl.replace(alter, yester)

    return yesterurl


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

def scoreboard_db_open(dbname, username):
    
    print('username:', username)
    print('dbname:', dbname)
    con = None
    con = psycopg2.connect(database=dbname, user=username)
    #print('  ', con)

    return con

def scoreboard_db_query_most_recent(my_con, scoreboard_table_name):

    handval = 'yes'
    sql_query = '''
                SELECT date 
                  FROM %s 
                  WHERE in_hand = %r
                  ORDER BY date DESC
                  LIMIT 1; 
                ''' % (scoreboard_table_name, handval)
    
    #print(sql_query)
    try:
        from_sql_query = pd.read_sql_query(sql_query, my_con)
        recent_date = from_sql_query['date'][0]
        #print(from_sql_query)
    except:
        print('  scoreboard_table does not exist')

    return recent_date


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
        #print('    ', soup[0:100])
        daypage = soup.prettify(encoding='utf-8')
        
        try:
            cur = my_con.cursor()
            #print(cur)
            #print(udate)
            cur.execute("UPDATE scoreboard SET in_hand=%s WHERE date=%s", ('yes', udate))        
            my_con.commit()
        except:  # psycopg2.DatabaseError, e:
            if my_con:
                my_con.rollback()
            print('Error %s' % e)
        find = 1
    except:
        find = 0
    
    if (write is not None) and (find == 1):
        target = open(writedir + write, 'w')
        target.write(daypage)
        target.close()
   
    return find



