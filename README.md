# NCAA Men's Basketball analytics tool

A repository to use past games of both teams to predict how one team should prepare and what aspects of their game to emphasize in order to maximize winning. 


## Steps to run
1. Execute ncaa_basketball_db with make_scoreboard=True,all
   else=False, and set lastdate=yesterday
2. Execute ncaa_basketball_scoreboard to pull the not yet obtained
   webpages and save them. no options to set
3. Execute ncaa_basketball_games with remake_db=True and get_games=True
4. Execute ncaa_basketball_games with remake_db=True and year= 'Y1Y1Y2Y2' set
