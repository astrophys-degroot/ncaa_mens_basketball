

import logging
import sys
import os
import time
import re

import numpy as np
import pandas as pd

# import urllib2
import psycopg2
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

from ncaa_basketball_db import NcaaBballDb


class GatherESPNData:

    def __init__(self, ):
        self.a = 1

    def get_med_dir(self, year, month):
        """
        method to grab the middle part of the directory structure based on year and month
        :param year:
        :param month:
        :return:
        """
        if int(month) > 7:
            med_dir = str(year) + '-' + str(int(year)+1) + '/'
        else:
            med_dir = str(int(year)-1) + '-' + str(year) + '/'

        return med_dir

# def start_games_db(dbname, username):
#     print '  Firing up the data base.'
#     engine = create_engine('postgres://%s@localhost/%s'%(username,dbname))
#     print '    DB url:', engine.url
#     db_exist = database_exists(engine.url)
#     if not db_exist:
#         create_database(engine.url)
#     print '    DB exists? %s' % db_exist
#     out_dict = 'dbname':dbname, 'username':username,
#                 'exists':db_exist, 'engine_url':engine.url, 'engine':engine
  #   return out_dict
  #
  #  cell_type: code,
  #  execution_count: 34,
  #  metadata:
  #   collapsed: true
  #  ,
  #  outputs: [],
  #  source: [
  #   def make_games_db(reset, dataframe, db_connect, boxscore_dict):,
  #       ,
  #       #print reset,
  #       #print dataframe,
  #       #print db_connect,
  #       #print boxscore_dict,
  #       ,
  #       if reset == 1:,
  #           my_if_exists = 'replace',
  #       else:,
  #           my_if_exists = 'append',
  #       #print my_if_exists,
  #       ,
  #       #print db_connect['engine'],
  #       dataframe.to_sql(boxscore_dict['table'], db_connect['engine'], if_exists=my_if_exists),
  #   ,
  #   ,
  #       #sys.exit(0) ,
  #       reset = 0 #now it will append instead of replacing,
  #       return reset
  #  ]
  # ,
  #
  #  cell_type: code,
  #  execution_count: 35,
  #  metadata:
  #   collapsed: true
  #  ,
  #  outputs: [],
  #  source: [
  #   def read_scoreboard(dir_file):,
  #       ,
  #       target = open(dir_file, 'r'),
  #       file_text = target.read(),
  #       target.close(),
  #       ,
  #       return file_text
  #  ]
  # ,
  #
  #  cell_type: code,
  #  execution_count: 36,
  #  metadata:
  #   collapsed: true
  #  ,
  #  outputs: [],
  #  source: [
  #   def get_games_link(xscore_board):,
  #       ,
  #       espn_links = [],
  #       soup = BeautifulSoup(xscore_board, 'lxml'),
  #       #finds = soup.find_all('a'),
  #   ,
  #       finds = soup.find_all('script'),
  #       for find in finds:,
  #           try:,
  #               text = find.get_text(),
  #               match = re.search('window.espn.scoreboardData.*', text),
  #               if match is not None:,
  #                   found_it = match.group(0),
  #           except:,
  #               a = 1,
  #               ,
  #       #print found_it,
  #       matches = re.findall('(http://espn.*?.com/.*?college.*?/.*?)\\\\\,', found_it),
  #       for match in matches:,
  #           next_match = re.search('boxscore', match),
  #           if next_match is not None:,
  #               espn_links.append(match),
  #               ,
  #       return espn_links
  #  ]
  # ,
  #
  #  cell_type: code,
  #  execution_count: 37,
  #  metadata:
  #   collapsed: true
  #  ,
  #  outputs: [],
  #  source: [
  #   def games_db_query(my_con, scoreboard_table_name):,
  #       print '    Now we query the table...',
  #
  #  ]
  # ,
  #
  #  cell_type: code,
  #  execution_count: 38,
  #  metadata:
  #   collapsed: false
  #  ,
  #  outputs: [],
  #  source: [
  #   def get_games_pages(xgames_link, my_con, xfile, xid, ,
  #                       write=False, ,
  #                       writedir=None, meddir=None):,
  #   #def get_yester_gamepage(yesterurl, my_con, udate, write=None, writedir=None):,
  #   ,
  #   ,
  #       if writedir == None:,
  #           writedir = 'boxscore_pages/',
  #       else:,
  #           writedir = str(writedir),
  #   ,
  #       if meddir == None:,
  #           meddir = '',
  #       else:,
  #           meddir = str(meddir),
  #       ,
  #       ,
  #       headers = 'User-Agent':'Mozilla/5.0',
  #       req = urllib2.Request(xgames_link, None, headers),
  #       #time.sleep(0.29),
  #       try:,
  #           html = urllib2.urlopen(req).read()  #request the page,
  #           soup = BeautifulSoup(html,'lxml'),
  #           score_page = soup.prettify(encoding='utf-8'),
  #   ,
  #           try:,
  #               cur = my_con.cursor(),
  #               cur.execute(\UPDATE games SET in_hand=%s WHERE id=%s\, ('yes', xid))  ,
  #               my_con.commit(),
  #           except psycopg2.DatabaseError, e:,
  #               if my_con:,
  #                   my_con.rollback(),
  #               print 'Error %s' % e    ,
  #           find = 1,
  #       except:,
  #           find = 0,
  #   ,
  #       if (write is True) and (find == 1):,
  #           file_name = writedir + meddir + 'ncaa_mbb_boxscore_' + str(xid) + '.txt',
  #           #print file_name,
  #           target = open(file_name, 'w'),
  #           target.write(score_page)  #save to file,
  #           target.close,
  #       return find


    def execute(self, indir=None, remake_db=False, scoreboard_table_name='scoreboard', get_games=False):
        """
        program to create the full games database from the full complement of available games.
        Data scraped from the scoreboard pages, after querying the scoreboard database.
        :param remake_db:
        :param get_games:
        :return:
        """

        myncaabball = NcaaBballDb()
        myncaabball.make_database_engine()
        myncaabball.check_database_engine()
        myncaabball.connect_database()

        if indir is None:
            indir = 'scoreboard_pages/'
        else:
            indir = str(indir)

        scoreboard_file = 'ncaa_mbb_scoreboard_full_YYYYMMDD.txt'
        boxscore_dir = 'boxscore_pages/'
        boxscore_file = 'ncaa_mbb_boxscore_DDDDDDDDD.txt',
        boxscore_table_name = 'games'
        boxscore_dict = {'dir': boxscore_dir, 'file': boxscore_file, 'table': boxscore_table_name}

        if remake_db:
            print('  Now remaking games database.')

            reset = 1  # do we append (0) or restart (1) the database
            cnt = 1  # silly database index counter since we give it one at a time




  #           db_connect = start_games_db(dbname, username),
  #   ,
  #           #connect to the database and query the scoreboard table,
  #           con = None,
  #           con = psycopg2.connect(database=dbname, user=username),
  #   ,
  #           sql_query = \SELECT date, year, month, day FROM %s WHERE in_hand='%s';\ % (scoreboard_table_name, 'yes'),
  #           #print sql_query,
  #           try:,
  #               dates_from_query = pd.read_sql_query(sql_query, con),
  #               #print dates_from_query,
  #           except:,
  #               print '  scoreboard_table does not exist' ,
  #   ,
  #           for ii in np.arange(len(dates_from_query)):,
  #               date_from_query = dates_from_query['date'][ii],
  #               date_from_query = date_from_query.replace('-', ''),
  #               #print date_from_query,
  #               ,
  #               med_dir = get_med_dir(dates_from_query['year'][ii], dates_from_query['month'][ii]),
  #               file_to_check = indir + med_dir + scoreboard_file.replace('YYYYMMDD',date_from_query),
  #               print '    Checking file: %s.' % (file_to_check),
  #               ,
  #               file_text = read_scoreboard(file_to_check),
  #               #print len(file_text),
  #               game_links = get_games_link(file_text),
  #               print '      There were %i games on this day.' % (len(game_links)),
  #               if len(game_links) != 0:,
  #                   for game_link in game_links:,
  #                       game_id = re.search('(\\d.*\\d)', game_link),
  #                       ,
  #                       game_file = boxscore_dir + med_dir + boxscore_file.replace('DDDDDDDDD', game_id.group(1)),
  #                       #print game_file,
  #                       #print 'boxscore_pages/2002-2003/ncaa_mbb_boxscore_',
  #                       f = os.path.exists(game_file),
  #                       #help(f),
  #                       #print f,
  #                       ,
  #                       if f:,
  #                           #f.readlines()[0],
  #                           in_hand = 'yes',
  #                       else:,
  #                           #print f.readlines(),
  #                           in_hand = 'no',
  #                       #print in_hand,
  #                       ,
  #                       my_dict = 'id':int(game_id.group(1)),,
  #                                 'date':dates_from_query['date'][ii], ,
  #                                 'year':dates_from_query['year'][ii], ,
  #                                 'month':dates_from_query['month'][ii],,
  #                                 'day':dates_from_query['day'][ii], ,
  #                                 'team1':'unknown',,
  #                                 'coach1':'unknown',,
  #                                 'team2':'unknown',,
  #                                 'coach2':'unknown',,
  #                                 'in_hand':in_hand, ,
  #                                 'url':game_link,
  #                                ,
  #                       my_df = pd.DataFrame(my_dict, index=[cnt]),
  #                       #print my_df,
  #   ,
  #                       #print '**********', reset,
  #                       reset = make_games_db(reset, my_df, db_connect, boxscore_dict),
  #                       #print reset,
  #                       cnt = cnt + 1,
  #   ,
  #                       #if cnt >= 25:,
  #                       #    sys.exit(0),
  #           ,
  #       if get_games:,
  #           print '  Now retrieve individual game pages.',
  #           cnt = 1 #a counter to stop the for loop,
  #   ,
  #           #connect to the database and query the scoreboard table,
  #           print '    Connecting to the database...' ,
  #           con = None,
  #           con = psycopg2.connect(database=dbname, user=username),
  #   ,
  #           sql_query = \SELECT url, year, month, id FROM %s WHERE in_hand='%s';\ % (boxscore_table_name, 'no'),
  #           #print sql_query,
  #           try:,
  #               data_from_query = pd.read_sql_query(sql_query, con),
  #               print '    We still need %i game pages.' % (len(data_from_query)),
  #           except:,
  #               print '  games_table, games, does not exist' ,
  #           print '',
  #   ,
  #   ,
  #           ,
  #           #print dates_from_query,
  #           urls_from_query = data_from_query['url'],
  #           for ii in np.arange(len(urls_from_query)):,
  #               print '      Getting game: ', urls_from_query[ii],
  #               med_dir = get_med_dir(data_from_query['year'][ii], data_from_query['month'][ii]),
  #               #print med_dir,
  #               my_file = boxscore_file.replace('DDDDDDDDD', str(data_from_query['id'][ii])),
  #               #print my_file,
  #               found = get_games_pages(urls_from_query[ii], con, ,
  #                                       my_file, ,
  #                                       data_from_query['id'][ii], ,
  #                                       write=True, meddir=med_dir),
  #               cnt = cnt + 1,
  #               if cnt >= 100000:,
  #                   sys.exit(0),


# boilerplate to execute call to main() function
if __name__ == '__main__':
    instance = GatherESPNData()
    instance.execute()
    # main(remake_db=True, get_games=True)
