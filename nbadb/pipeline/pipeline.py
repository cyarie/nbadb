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

from configparser import ConfigParser
from nbadb.conf import settings


class Pipeline(object):
    def __init__(self, config_file, pipeline_type='build', league=settings.LEAGUE_ID, season=settings.NBA_SEASON):
        self.pipeline_type = pipeline_type
        self.config_file = config_file
        self.league = league
        self.season = season
        self.db_config = None
        self.s3_config = None
        self.dsn = None

    def _read_config(self):
        config = ConfigParser()
        config.read(os.path.join(settings.BASE_DIR, 'settings.ini'))
        self.db_config = config['db']
        self.s3_config = config['s3']
        self.dsn = "host={0} dbname={1} user={2} password={3}".format(self.db_config['DATABASE_HOST'],
                                                                      self.db_config['DATABASE_NAME'],
                                                                      self.db_config['DATABASE_USER'],
                                                                      self.db_config['DATABASE_PW'])

    @staticmethod
    def _run_query(conn, query, params):
        """
        Takes a given query and a tuple of parameters and then executes it and returns the cursor.fetchall() object,
        if applicable.
        :param psycopg2.connection conn:
        :param str query:
        :param tuple params:
        :return:
        """

        with conn.cursor as cur:
            cur.execute(query, params)
            data = cur.fetchall()
        return data

    @staticmethod
    def _requests_json(fmt_url):
        return json.loads(requests.get(fmt_url).text)

    def _build_teams(self):
        teams_url = "http://stats.nba.com/stats/commonteamyears/?LeagueID={0}".format(settings.LEAGUE_ID)
        teams_json = self._requests_json(teams_url)['resultSets'][0]['rowSet']

    def build(self):
        pass

    def update(self):
        pass
