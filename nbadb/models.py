from sqlalchemy import Column, Integer, String, ForeignKey, Sequence, DateTime, Date, Float
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class DateMixin(object):
    create_dt = Column(DateTime, default=datetime.now())
    modified_dt = Column(DateTime, default=datetime.now())


class Team(Base, DateMixin):
    __tablename__ = 'team'

    team_id = Column(Integer, Sequence('team_id_seq'), primary_key=True)
    team_abbv = Column(String(3))

    def __repr__(self):
        return ""


class Player(Base, DateMixin):
    __tablename__ = 'player'

    player_id = Column(Integer, Sequence('player_id_seq'), primary_key=True)
    first_name = Column(String(255))
    last_name = Column(String(255))
    position = Column(String(2))
    age = Column(Integer)

    def __repr__(self):
        return ""


class Game(Base, DateMixin):
    __tablename__ = 'game'

    game_id = Column(Integer, Sequence('game_id_seq'), primary_key=True)
    game_date = Column(Date)
    season_id = Column(String(10))

    def __repr__(self):
        return ""


class TeamsGames(Base, DateMixin):
    __tablename__ = 'teams_games'

    teams_games_id = Column(Integer, Sequence('teams_games_id_seq'), primary_key=True)
    team_id = Column(Integer, ForeignKey('team.team_id'), nullable=False)
    game_id = Column(Integer, ForeignKey('game.game_id'), nullable=False)
    opponent = Column(Integer, ForeignKey('team.team_id'), nullable=False)
    field_goals_made = Column(Integer)
    field_goals_attempted = Column(Integer)
    field_goal_percentage = Column(Float)
