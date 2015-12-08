"""
Contains settings needed for the project to run.
"""
import os
import logging
import logging.config
from configparser import ConfigParser

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Project is setup to use .ini configs by default.
# https://docs.python.org/3/library/configparser.html
CONFIG_FILE = 'settings.ini'
config = ConfigParser()
config.read(os.path.join(BASE_DIR, CONFIG_FILE))

# Project is written to use PostgreSQL by default, and all the SQL is written assuming Postgres. This could be changed
# in the future to use SQLAlchemy to make it more DB agnostic.
database = {
    'postgres': {
        'name': config['db']['DATABASE_NAME'],
        'host': config['db']['DATABASE_HOST'],
        'user': config['db']['DATABASE_USER'],
        'password': config['db']['DATABASE_PW'],
        'port': config['db']['DATABASE_PORT'],
        }
    }

# Scoring data for the two big DFS sites.
dfs_scores = {
    'fanduel': {
        'fg3m': 3,
        'fgm': 2,
        'ftm': 1,
        'reb': 1.2,
        'ast': 1.5,
        'blk': 2,
        'stl': 2,
        'tov': -1
    },
    'draftkings': {
        'pts': 1,
        'fg3m': 0.5,
        'reb': 1.25,
        'ast': 1.5,
        'stl': 2,
        'blk': 2,
        'tov': -0.5,
        'dd': 1.5,
        'td': 3
    }
}

# Project is currently setup to build a database of only the current season's data. You can build more by turning the
# NBA_SEASON constant into a tuple and looping through the seasons.
NBA_SEASON = '2015-16'
LEAGUE_ID = '00'

# Let's setup some project-wide logging
# https://docs.python.org/3/library/logging.html
LOGGING = {
    'version': 1,
    'formatters': {
        'db_op': {
            'format': "[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s",
            'datefmt': "%d/%b/%Y %H:%M:%S"
        }
    },
    'handlers': {
        'file': {
            'level': 'INFO',
            'class': 'logging.FileHandler',
            'filename': 'nbadb.out.log',
            'formatter': 'db_op'
        }
    },
    'loggers': {
        'nbadb': {
            'handlers': ['file'],
            'level': 'INFO'
        }
    }
}
logging.config.dictConfig(LOGGING)
