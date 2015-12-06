DROP SCHEMA nba CASCADE;
CREATE SCHEMA nba;

DROP TABLE IF EXISTS nba.team;
CREATE TABLE nba.team (
  team_id INTEGER PRIMARY KEY,
  team_abbv VARCHAR(3),
  create_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  modified_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS nba.player;
CREATE TABLE nba.player (
  player_id INTEGER PRIMARY KEY,
  first_name VARCHAR(255),
  last_name VARCHAR(255),
  position VARCHAR(2),
  age INTEGER,
  create_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  modified_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS nba.game;
CREATE TABLE nba.game (
  game_id INTEGER PRIMARY KEY,
  game_date DATE,
  season_id VARCHAR(10),
  create_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  modified_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS nba.teams_games;
CREATE TABLE nba.teams_games (
  teams_games_id SERIAL PRIMARY KEY,
  team_id INTEGER REFERENCES nba.team (team_id),
  game_id INTEGER REFERENCES nba.game (game_id),
  opponent INTEGER REFERENCES nba.team (team_id),
  field_goals_made INTEGER,
  field_goals_attempted INTEGER,
  field_goal_percentage NUMERIC(4, 2),
  field_goals_three_pt_made INTEGER,
  field_goals_three_pt_attempted INTEGER,
  field_goal_three_pt_percentage NUMERIC(4, 2),
  free_throws_made INTEGER,
  free_throws_attempted INTEGER,
  free_throw_percentage NUMERIC(4, 2),
  offensive_rebounds INTEGER,
  defensive_rebounds INTEGER,
  rebounds INTEGER,
  assists INTEGER,
  steals INTEGER,
  blocks INTEGER,
  turnovers INTEGER,
  points INTEGER,
  off_efficiency NUMERIC(10, 2),
  off_rating NUMERIC(10, 2),
  def_rating NUMERIC(10, 2),
  oreb_pct NUMERIC(10, 2),
  efg_pct NUMERIC(10, 2),
  ts_pct NUMERIC(10, 2),
  possessions NUMERIC(10, 2),
  pace NUMERIC(10, 2),
  create_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  modified_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX teams_games_idx ON nba.teams_games (teams_games_id, team_id, game_id);

DROP TABLE IF EXISTS nba.players_games;
CREATE TABLE nba.players_games (
  players_games_id SERIAL PRIMARY KEY,
  player_id INTEGER REFERENCES nba.player (player_id),
  game_id INTEGER REFERENCES nba.game (game_id),
  team_id INTEGER REFERENCES nba.team (team_id),
  minutes INTEGER,
  field_goals_made INTEGER,
  field_goals_attempted INTEGER,
  field_goal_percentage NUMERIC(4, 2),
  field_goals_three_pt_made INTEGER,
  field_goals_three_pt_attempted INTEGER,
  field_goal_three_pt_percentage NUMERIC(4, 2),
  free_throws_made INTEGER,
  free_throws_attempted INTEGER,
  free_throw_percentage NUMERIC(4, 2),
  offensive_rebounds INTEGER,
  defensive_rebounds INTEGER,
  rebounds INTEGER,
  assists INTEGER,
  steals INTEGER,
  blocks INTEGER,
  turnovers INTEGER,
  points INTEGER,
  personal_fouls INTEGER,
  plus_minus INTEGER,
  efg_pct NUMERIC(5, 2),
  ts_pct NUMERIC(5, 2),
  usg_pct NUMERIC(5, 2),
  pace NUMERIC(5, 2),
  fd_fp NUMERIC(5, 2),
  dk_fp NUMERIC(5, 2),
  create_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  modified_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE nba.players_games ADD COLUMN team_possessions NUMERIC(10, 2);
CREATE INDEX players_games_idx ON nba.players_games (players_games_id, player_id, game_id);