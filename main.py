# import appropriate modules
import sys
from ncaa_basketball_db import NcaaBballDb
from ncaa_basketball_scoreboard import NcaaBballScoreboard


def main(baseurl=None, basedir=None, basefile=None, basealter=None,
         find_most_recent=False, scoreboard_table_name=None):
    """
    function to call the individual parts of the project as necessary and specified by the person
    running the call

    :param baseurl:
    :param basedir:
    :param basefile:
    :param basealter:
    :param find_most_recent:
    :param scoreboard_table_name:
    :return:
    """
    print('Now running: ', sys.argv[0])

    # create an instance of the database interaction class
    mydb = NcaaBballDb(scoreboard_name=scoreboard_table_name)
    print(mydb)
    print(mydb.getdbname())
    print(mydb.getusername())
    passed = mydb.set_connect_db()
    print("Status of making the database connection: %s" % passed)
    print(mydb.get_connect_db())
    passed = mydb.set_engine_db()
    print("Status of creating the database engine: %s", passed)
    print(mydb.get_engine_db())

    print(mydb.names_of_tables())
    tablenames = mydb.get_table_names()
    print(tablenames[1])

    for tablename in tablenames:
        print(tablename)
        print(mydb.table_head(tablename))
    # print(mydb.getScoreboardName())

    # create a scoreboard object
    # myscoreboard = NcaaBballScoreboard(baseurl=baseurl, basedir=basedir,
    #                                    basefile=basefile, basealter=basealter)
    #
    # value = myscoreboard.get_baseurl()
    # print(value)
    # value = myscoreboard.get_basedir()
    # print(value)
    # value = myscoreboard.get_basefile()
    # print(value)
    # value = myscoreboard.get_basealter()
    # print(value)

    # do some checking on what we have already
    # nremaining_dates = myscoreboard.sbDbQueryHm(mydb)
    # print('    Number of dates not yet obtained: %s' % myscoreboard.getNRemaining())


# boilerplate to execute call to main() function
if __name__ == '__main__':
    print("Now starting the run...")

    main(find_most_recent=False)

# input parameters
# baseurl = 'http://espn.go.com/mens-college-basketball/scoreboard/_/group/50/date/YYYYMMDD' #top 25 only
# baseurl = 'http://scores.espn.go.com/mens-college-basketball/scoreboard/_/group/50/date/YYYYMMDD' # all games
# basefile = 'ncaa_mbb_scoreboard_YYYYMMDD.txt'  #top 25 only
# basedir = 'scoreboard_pages/'
# basefile = 'ncaa_mbb_scoreboard_full_YYYYMMDD.txt' # all games
# alter = 'YYYYMMDD'

# username = 'smaug'
# dbname = 'ncaa_mbb_db'
# scoreboard_table_name = 'scoreboard'

# open connection to database
# my_con = scoreboard_db_open(dbname, username)
# remaining_dates = scoreboard_db_query(my_con, scoreboard_table_name)
# if most_recent is not False:
#     rec_date = scoreboard_db_query_most_recent(my_con, scoreboard_table_name)
#     print('  Most recent date (YYYY-MM-DD) in %s table is %s' % (scoreboard_table_name, rec_date))

# try to get the remaining dates
# print remaining_dates['date']
# remaining_dates = remaining_dates['date']
# print('  %i dates need to be pulled.' % len(remaining_dates))
# for remaining_date in reversed(remaining_dates):
#     this_dir = get_yester_meddir(basedir, remaining_date)
#     # print(this_dir)
#     yester = remaining_date.replace('-', '')
#     url_yester_games = get_yester_url(baseurl, alter, yester)
#     # print('  ', url_yester_games)
#     file_yester_gamepage = get_yester_filename(basefile, alter, yester)
#     # print(file_yester_gamepage)
#     yester_gamepage = get_yester_gamepage(url_yester_games, my_con, remaining_date, write=file_yester_gamepage,
#                                           writedir=this_dir)
#     print('  Length of previous day page:', yester_gamepage)



    # sql_query = "SELECT in_hand FROM %s WHERE date='%s';" % (scoreboard_table_name, remaining_date)
    # print(sql_query)
    # from_sql_query = pd.read_sql_query(sql_query, my_con)
    # print(from_sql_query)
