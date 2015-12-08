"""
Pipelines. Two big classes will be Build and Update? Process looks like...
1. Load data from stats.nba.com/
2. Process and clean data
3. Ship processed data up to S3 (CSV, JSON...? CSV will allow direct imports into Postgres if/when we need to rebuild)
4. Load into Postgres
"""
import os
import psycopg2
import json
import requests
import logging

from datetime import datetime
from nbadb.conf import settings
from nbadb.pipeline.queries import Queries
logger = logging.getLogger('nbadb')


class Pipeline(object):
    def __init__(self, pipeline_type='build', league=settings.LEAGUE_ID, season=settings.NBA_SEASON):
        self.pipeline_type = pipeline_type
        self.schema_file = os.path.join(settings.BASE_DIR, 'schemas/nba.sql')
        self.league = league
        self.season = season
        self.dsn = "host={0} dbname={1} user={2} password={3}".format(settings.database['postgres']['host'],
                                                                      settings.database['postgres']['name'],
                                                                      settings.database['postgres']['user'],
                                                                      settings.database['postgres']['password'])

    @staticmethod
    def _requests_json(fmt_url, jsonp=False):
        """
        Small helper function to take in a URL and return a json.loads() object from its text.
        :param fmt_url:
        :return json.loads():
        """
        if jsonp:
            return json.loads(requests.get(fmt_url).text.replace('callbackWrapper(', '').replace(');', ''))
        else:
            return json.loads(requests.get(fmt_url).text)

    @staticmethod
    def _ingest_game_log(game_log, season):
        """
        We need to clean up the NBA's dates before handing them off to Postgres
        :param game_log:
        :param season:
        :return tuple:
        """

        nba_dt_fmt = '%b %d, %Y'
        game_dt = datetime.strptime(game_log[2], nba_dt_fmt).date()
        return tuple([game_log[1], game_dt, season])

    def _grab_teams(self):
        query = """SELECT team_id FROM nba.team"""
        with psycopg2.connect(self.dsn) as conn:
            return Queries.retrieve_query(conn, query, ())

    def _grab_player_age(self, player_id):
        age_url = 'http://stats.nba.com/stats/playerprofilev2/?PlayerID={}&PerMode=Totals'.format(player_id)
        age_json = self._requests_json(age_url)
        try:
            return int(age_json['resultSets'][0]['rowSet'][len(age_json['resultSets'][0]['rowSet']) - 1][5])
        except IndexError:
            return 0

    def _grab_games_list(self, pipeline_type='build'):
        if pipeline_type == 'build':
            query = """SELECT DISTINCT(game_id) FROM nba.game"""
        elif pipeline_type == 'update':
            query = """SELECT DISTINCT(game_id)
                       FROM nba.game
                       WHERE game_id > (SELECT MAX(game_id) FROM nba.teams_games)"""
        else:
            exit(1)
        games_list = []
        with psycopg2.connect(self.dsn) as conn:
            games = Queries.retrieve_query(conn, query, ())
            for game in games:
                games_list.append(int(game[0]))
        conn.close()
        return games_list

    def _max_game_id(self):
        query = """SELECT MAX(game_id) FROM nba.game;"""
        with psycopg2.connect(self.dsn) as conn:
            max_game = Queries.retrieve_query(conn, query, ())[0][0]
        conn.close()
        return max_game

    def _game_from_boxscore(self, game_id):
        logger.info('Starting processing of {}'.format(game_id))
        # Need to do a bunch of setup for this method...
        boxscore_url = "http://stats.nba.com/stats/boxscoreadvanced/?StartPeriod=1&EndPeriod=10&GameID=00{0}" \
                       "&RangeType=0&StartRange=0&EndRange=10".format(game_id)
        game_dict = dict()
        players = {}
        master_dict = self._requests_json(boxscore_url)['resultSets']
        fd_score_multipliers = settings.dfs_scores['fanduel']
        dk_score_multipliers = settings.dfs_scores['draftkings']

        teams_games_query = """INSERT INTO nba.teams_games
                                     (team_id, game_id, field_goals_made, field_goals_attempted,
                                      field_goal_percentage, field_goals_three_pt_made, field_goals_three_pt_attempted,
                                      field_goal_three_pt_percentage, free_throws_made, free_throws_attempted,
                                      free_throw_percentage, offensive_rebounds, defensive_rebounds, rebounds,
                                      assists, steals, blocks, turnovers, points, possessions, off_efficiency,
                                      off_rating,
                                      def_rating, oreb_pct, efg_pct, ts_pct, pace, opponent)
                                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                         %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

        players_games_query = """INSERT INTO nba.players_games
                                     (player_id, game_id, team_id, minutes, field_goals_made, field_goals_attempted,
                                      field_goal_percentage,
                                      field_goals_three_pt_made, field_goals_three_pt_attempted,
                                      field_goal_three_pt_percentage, free_throws_made, free_throws_attempted,
                                      free_throw_percentage, offensive_rebounds, defensive_rebounds, rebounds,
                                      assists, steals, blocks, turnovers, points, efg_pct, ts_pct, usg_pct,
                                      pace, fd_fp, dk_fp)
                                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                         %s, %s, %s, %s, %s, %s, %s);"""

        # Home team
        home_team_id = master_dict[0]['rowSet'][0][6]
        game_dict[home_team_id] = {}

        # Away team
        away_team_id = master_dict[0]['rowSet'][0][7]
        game_dict[away_team_id] = {}

        # Parsing team data
        for team in master_dict[5]['rowSet']:
            for i in range(len(team)):
                if team[i] is None:
                    team[i] = 0
            game_dict[team[1]]['fgm'] = int(team[6])
            game_dict[team[1]]['fga'] = int(team[7])
            game_dict[team[1]]['fg_pct'] = float(team[8])
            game_dict[team[1]]['fg3m'] = int(team[9])
            game_dict[team[1]]['fg3a'] = int(team[10])
            game_dict[team[1]]['fg3_pct'] = float(team[11])
            game_dict[team[1]]['ftm'] = int(team[12])
            game_dict[team[1]]['fta'] = int(team[13])
            game_dict[team[1]]['ft_pct'] = float(team[14])
            game_dict[team[1]]['oreb'] = int(team[15])
            game_dict[team[1]]['dreb'] = int(team[16])
            game_dict[team[1]]['reb'] = int(team[17])
            game_dict[team[1]]['ast'] = int(team[17])
            game_dict[team[1]]['stl'] = int(team[18])
            game_dict[team[1]]['blk'] = int(team[19])
            game_dict[team[1]]['tov'] = int(team[20])
            game_dict[team[1]]['pts'] = int(team[21])
            possessions = int(team[7]) + (0.44 * int(team[13])) + int(team[20]) - int(team[15])
            game_dict[team[1]]['possessions'] = possessions
            try:
                off_efficiency = 100 * (int(team[21]) / possessions)
            except ZeroDivisionError:
                off_efficiency = 0
            game_dict[team[1]]['off_efficiency'] = off_efficiency

        # Grabbing advanced data, like offensive rating, eFG%, pace, etc
        for team in master_dict[14]['rowSet']:
            for i in range(len(team)):
                if team[i] is None:
                    team[i] = 0
            game_dict[team[1]]['off_rating'] = float(team[6])
            game_dict[team[1]]['def_rating'] = float(team[7])
            game_dict[team[1]]['oreb_pct'] = float(team[12])
            game_dict[team[1]]['efg_pct'] = float(team[16])
            game_dict[team[1]]['ts_pct'] = float(team[17])
            game_dict[team[1]]['pace'] = float(team[19])

        for player in master_dict[4]['rowSet']:
            player_id = int(player[4])
            for i in range(len(player)):
                if player[i] is None:
                    player[i] = 0

            players[player_id] = {}
            players[player_id]['player_id'] = player_id
            players[player_id]['team_id'] = int(player[1])
            if player[8] == 0:
                players[player_id]['minutes'] = 0
            else:
                players[player_id]['minutes'] = int(player[8].split(':')[0])
            players[player_id]['fgm'] = int(player[9])
            players[player_id]['fga'] = int(player[10])
            players[player_id]['fgp'] = float(player[11])
            players[player_id]['fg3m'] = int(player[12])
            players[player_id]['fg3a'] = int(player[13])
            players[player_id]['fg3p'] = float(player[14])
            players[player_id]['ftm'] = int(player[15])
            players[player_id]['fta'] = int(player[16])
            players[player_id]['ftp'] = float(player[17])
            players[player_id]['oreb'] = int(player[18])
            players[player_id]['dreb'] = int(player[19])
            players[player_id]['rebs'] = int(player[20])
            players[player_id]['ast'] = int(player[21])
            players[player_id]['stl'] = int(player[22])
            players[player_id]['blk'] = int(player[23])
            players[player_id]['tov'] = int(player[24])
            players[player_id]['pts'] = int(player[26])

            double_stats = (players[player_id]['pts'], players[player_id]['rebs'], players[player_id]['ast'],
                            players[player_id]['stl'], players[player_id]['tov'], )

            # Figure out if a player hit a double-double or triple-double
            i = 0
            doub_doub = 0
            trip_doub = 0
            for stat in double_stats:
                if stat >= 10:
                    i += 1
            if i == 2:
                doub_doub = 1
            elif i == 3:
                doub_doub = 1
                trip_doub = 1
            else:
                pass

            fd_fp = (players[player_id]['pts'] + ((players[player_id]['rebs'] * fd_score_multipliers['reb']) +
                                                  (players[player_id]['ast'] * fd_score_multipliers['ast']) +
                                                  (players[player_id]['blk'] * fd_score_multipliers['blk']) +
                                                  (players[player_id]['stl'] * fd_score_multipliers['stl']))) + \
                    (players[player_id]['tov'] * fd_score_multipliers['tov'])
            dk_fp = (players[player_id]['pts'] + ((players[player_id]['rebs'] * dk_score_multipliers['reb']) +
                                                  (players[player_id]['fg3m'] * dk_score_multipliers['fg3m']) +
                                                  (players[player_id]['ast'] * dk_score_multipliers['ast']) +
                                                  (players[player_id]['stl'] * dk_score_multipliers['stl']) +
                                                  (players[player_id]['blk'] * dk_score_multipliers['blk']) +
                                                  (doub_doub * dk_score_multipliers['dd']) +
                                                  (trip_doub * dk_score_multipliers['td']))) + \
                    (players[player_id]['tov'] * dk_score_multipliers['tov'])
            players[player_id]['fd_fp'] = float(fd_fp)
            players[player_id]['dk_fp'] = float(dk_fp)

        for player in master_dict[13]['rowSet']:
            player_id = int(player[4])
            for i in range(len(player)):
                if player[i] is None:
                    player[i] = 0

            if players[player_id]:
                if len(str(player[8]).split(':')) > 1:
                    players[player_id]['efg_pct'] = float(player[19])
                    players[player_id]['ts_pct'] = float(player[20])
                    players[player_id]['usg_pct'] = float(player[21])
                    players[player_id]['pace'] = float(player[22])
                else:
                    players[player_id]['efg_pct'] = 0
                    players[player_id]['ts_pct'] = 0
                    players[player_id]['usg_pct'] = 0
                    players[player_id]['pace'] = 0
            else:
                players[player_id]['efg_pct'] = 0
                players[player_id]['ts_pct'] = 0
                players[player_id]['usg_pct'] = 0
                players[player_id]['pace'] = 0

        home_team_tup = (home_team_id, game_id, game_dict[home_team_id]['fgm'], game_dict[home_team_id]['fga'],
                         game_dict[home_team_id]['fg_pct'], game_dict[home_team_id]['fg3m'],
                         game_dict[home_team_id]['fg3a'], game_dict[home_team_id]['fg3_pct'],
                         game_dict[home_team_id]['ftm'], game_dict[home_team_id]['fta'],
                         game_dict[home_team_id]['ft_pct'], game_dict[home_team_id]['oreb'],
                         game_dict[home_team_id]['dreb'], game_dict[home_team_id]['reb'],
                         game_dict[home_team_id]['ast'], game_dict[home_team_id]['stl'], game_dict[home_team_id]['blk'],
                         game_dict[home_team_id]['tov'], game_dict[home_team_id]['pts'],
                         game_dict[home_team_id]['possessions'], game_dict[home_team_id]['off_efficiency'],
                         game_dict[home_team_id]['off_rating'], game_dict[home_team_id]['def_rating'],
                         game_dict[home_team_id]['oreb_pct'], game_dict[home_team_id]['efg_pct'],
                         game_dict[home_team_id]['ts_pct'], game_dict[home_team_id]['pace'], away_team_id, )

        away_team_tup = (away_team_id, game_id, game_dict[away_team_id]['fgm'], game_dict[away_team_id]['fga'],
                         game_dict[away_team_id]['fg_pct'], game_dict[away_team_id]['fg3m'],
                         game_dict[away_team_id]['fg3a'], game_dict[away_team_id]['fg3_pct'],
                         game_dict[away_team_id]['ftm'], game_dict[away_team_id]['fta'],
                         game_dict[away_team_id]['ft_pct'], game_dict[away_team_id]['oreb'],
                         game_dict[away_team_id]['dreb'], game_dict[away_team_id]['reb'],
                         game_dict[away_team_id]['ast'], game_dict[away_team_id]['stl'], game_dict[away_team_id]['blk'],
                         game_dict[away_team_id]['tov'], game_dict[away_team_id]['pts'],
                         game_dict[away_team_id]['possessions'], game_dict[away_team_id]['off_efficiency'],
                         game_dict[away_team_id]['off_rating'], game_dict[away_team_id]['def_rating'],
                         game_dict[away_team_id]['oreb_pct'], game_dict[away_team_id]['efg_pct'],
                         game_dict[away_team_id]['ts_pct'], game_dict[away_team_id]['pace'], home_team_id, )

        with psycopg2.connect(self.dsn) as conn:
            Queries.insert_query(conn, teams_games_query, home_team_tup)
        conn.close()

        with psycopg2.connect(self.dsn) as conn:
            Queries.insert_query(conn, teams_games_query, away_team_tup)
        conn.close()

        with psycopg2.connect(self.dsn) as conn:
            for player in players:
                player_tup = (players[player]['player_id'], game_id, players[player]['team_id'],
                              players[player]['minutes'], players[player]['fgm'], players[player]['fga'],
                              players[player]['fgp'], players[player]['fg3m'], players[player]['fg3a'],
                              players[player]['fg3p'], players[player]['ftm'], players[player]['fta'],
                              players[player]['ftp'], players[player]['oreb'], players[player]['dreb'],
                              players[player]['rebs'], players[player]['ast'], players[player]['stl'],
                              players[player]['blk'], players[player]['tov'], players[player]['pts'],
                              players[player]['efg_pct'], players[player]['ts_pct'], players[player]['usg_pct'],
                              players[player]['pace'], players[player]['fd_fp'], players[player]['dk_fp'])
                Queries.insert_query(conn, players_games_query, player_tup)
        conn.close()
        logger.info('Finished processing of {}'.format(game_id))

    def _build_schema(self):
        with open(self.schema_file, 'r') as schema:
            sql = schema.read()
            sql_commands = sql.split(';')
            del sql_commands[len(sql_commands) - 1]
            with psycopg2.connect(self.dsn) as conn:
                for command in sql_commands:
                    Queries.build_query(conn, command)

    def _build_teams(self):
        teams_url = "http://stats.nba.com/stats/commonteamyears/?LeagueID={0}".format(settings.LEAGUE_ID)
        teams_json = self._requests_json(teams_url)['resultSets'][0]['rowSet']
        query = """INSERT INTO nba.team (team_id, team_abbv) VALUES (%s, %s)"""
        with psycopg2.connect(self.dsn) as conn:
            for team in teams_json:
                if team[4]:
                    team_id = int(team[1])
                    team_abbv = str(team[4])
                    Queries.insert_query(conn, query, (team_id, team_abbv))
                else:
                    pass

    def _build_games(self, pipeline_type):
        games_url = "http://stats.nba.com/stats/teamgamelog?TeamId={0}&Season={1}&SeasonType=Regular%20Season"
        query = """INSERT INTO nba.game (game_id, game_date, season_id) VALUES (%s, %s, %s);"""
        game_id_set = set()
        if pipeline_type == 'update':
            max_game = self._max_game_id()
        else:
            max_game = 0
        with psycopg2.connect(self.dsn) as conn:
            for team in self._grab_teams():
                game_logs = self._requests_json(games_url.format(int(team[0]),
                                                                 settings.NBA_SEASON))['resultSets'][0]['rowSet']
                if pipeline_type == 'build':
                    for game in game_logs:
                        if game[1] not in game_id_set:
                            Queries.insert_query(conn, query, (self._ingest_game_log(game, settings.NBA_SEASON)))
                            game_id_set.add(game[1])
                elif pipeline_type == 'update':
                    for game in game_logs:
                        if game[1] not in game_id_set and int(game[1]) > max_game:
                            logger.info('Adding game_id {} to database'.format(game[1]))
                            Queries.insert_query(conn, query, (self._ingest_game_log(game, settings.NBA_SEASON)))
                            game_id_set.add(game[1])
        conn.close()

    def _build_players(self):
        pos_dict = {
            'Small Forward': 'SF', 'Point Guard': 'PG', 'Center': 'C',
            'Power Forward': 'PF', 'Shooting Guard': 'SG', 'None': 'N'
        }

        query = """INSERT INTO nba.player (player_id, first_name, last_name, position, age)
                      VALUES (%s, %s, %s, %s, %s)"""
        players_url = 'http://stats.nba.com/stats/commonallplayers/?LeagueID={0}&Season={1}&IsOnlyCurrentSeason=1'
        players_json = self._requests_json(players_url.format(settings.LEAGUE_ID, settings.NBA_SEASON))
        players_list = players_json['resultSets'][0]['rowSet']
        with psycopg2.connect(self.dsn) as conn:
            for player in players_list:
                pos_url = 'http://data.nba.com/jsonp/5s/json/cms/noseason/players/{0}/playercard.json'
                pos_json = self._requests_json(pos_url.format(player[5]), jsonp=True)
                first_name = pos_json['sports_content']['player']['meta']['first_name']
                last_name = pos_json['sports_content']['player']['meta']['last_name']
                if first_name == 'Ish' and last_name == 'Smith':
                    first_name = 'Ishmael'
                position = pos_json['sports_content']['player']['meta']['position_granular_full']
                pos_split = position.split(' ')
                if ' ' in position:
                    if len(pos_split) > 3:
                        position = '{0} {1}'.format(pos_split[0], pos_split[1])
                    elif len(pos_split) == 2 or len(pos_split) == 3:
                        if pos_split[0] == pos_split[1]:
                            position = pos_split[0]
                        else:
                            position = '{0} {1}'.format(pos_split[0], pos_split[1])
                elif position is '':
                    position = 'None'
                position = pos_dict[position]
                Queries.insert_query(conn, query, (player[0], first_name, last_name, position,
                                                   self._grab_player_age(player[0])))
        conn.close()

    def _build_teams_players_logs(self, pipeline_type):
        try_again_list = []
        for game in self._grab_games_list(pipeline_type=pipeline_type):
            try:
                self._game_from_boxscore(game)
            except ConnectionError:
                logger.error('Issue connecting to NBA stats API for game_id {}'.format(game))
                try_again_list.append(game)

        if len(try_again_list):
            for game in try_again_list:
                self._game_from_boxscore(game)

    def build(self):
        logger.info('Building the schema.')
        self._build_schema()
        logger.info('Building the teams table.')
        self._build_teams()
        logger.info('Building the games table.')
        self._build_games(pipeline_type='build')
        logger.info('Building the players table.')
        self._build_players()
        logger.info('Building the players_games and teams_games tables.')
        self._build_teams_players_logs(pipeline_type='build')
        logger.info('Finished building the database.')

    def _update_games(self):
        self._build_games(pipeline_type='update')

    def _update_teams_players_logs(self):
        self._build_teams_players_logs(pipeline_type='update')

    def update(self, table_list):
        """
        Takes *args as a parameter, then matches an arg back to a dictionary to figure out which update function to
        run.
        :param list table_list:
        :return:
        """
        upd_dict = {
            'games': self._update_games,
            'game_logs': self._update_teams_players_logs
        }
        for table in table_list:
            logger.info('Started updating {}.'.format(table))
            upd_dict[table]()
            logger.info('Finished updating {}'.format(table))
