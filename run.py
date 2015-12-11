import argparse
import logging
from nbadb.pipeline import api
from nbadb.conf import settings

logger = logging.getLogger('nbadb')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Builds a pipeline for processing NBA data.')
    parser.add_argument('--pipeline', '-p', dest='pipeline_type', type=str, help='Choose to build a new database'
                                                                                 'or update the existing database.')
    parser.add_argument('--season', '-s', dest='season', default=settings.NBA_SEASON, type=str,
                        help='Choose which season to build data for.')
    parser.add_argument('--league', '-l', dest='league', default=settings.LEAGUE_ID, type=str,
                        help='Choose which league to gather data on.')
    parser.add_argument('--tables', '-t', metavar='T', type=str, nargs='+', dest='tables')
    args = parser.parse_args()

    pipeline = api.Pipeline(pipeline_type=args.pipeline_type, league=args.league, season=args.season)

    if args.pipeline_type == 'build':
        logger.info('Beginning to build database for league {0} and season {1}'.format(args.league,
                                                                                       args.season))
        pipeline.build()
        logger.info('Finished building database for league {0} and season {1}'.format(args.league,
                                                                                      args.season))
    elif args.pipeline_type == 'update':
        logger.info('Updating tables {0} in {1}'.format(['games', 'game_logs'],
                                                        settings.database['postgres']['name']))
        pipeline.update(args.tables)
        logger.info('Finished updating {0} in {1}'.format(['games', 'game_logs'],
                                                          settings.database['postgres']['name']))
