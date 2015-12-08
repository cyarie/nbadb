import sys
import logging
from nbadb.pipeline import api
from nbadb.conf import settings
logger = logging.getLogger('nbadb')

if __name__ == "__main__":
    if sys.argv[1]:
        pipeline_type = sys.argv[1]
    else:
        pipeline_type = 'build'

    if sys.argv[2]:
        league_id = sys.argv[2]
    else:
        league_id = settings.LEAGUE_ID

    if sys.argv[3]:
        season = sys.argv[3]
    else:
        season = settings.NBA_SEASON

    pipeline = api.Pipeline(pipeline_type=pipeline_type, league=league_id, season=season)

    if pipeline_type == 'build':
        pipeline.build()
    elif pipeline_type == 'update':
        pipeline.update('games', 'game_logs')
