DROP DATABASE IF EXISTS college_football;
CREATE DATABASE college_football;
USE college_football;


CREATE TABLE teams (
    team_id INT PRIMARY KEY,
    team_name VARCHAR(64),
    stadium_name VARCHAR(128),
    lat FLOAT,
    lon FLOAT,
    conference VARCHAR(40)
);


CREATE TABLE games (
    game_id BIGINT PRIMARY KEY,
    season INT,
    week INT,
    home_team_id INT,
    away_team_id INT,
    home_points INT,
    away_points INT,
    venue VARCHAR(128),
    date DATE,
    neutral_site BOOLEAN,
    FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
    FOREIGN KEY (away_team_id) REFERENCES teams(team_id)
);


CREATE TABLE team_stats (
    team_id INT NOT NULL,
    season INT NOT NULL,
    week INT NOT NULL,
    -- Offensive Efficiency
    games_played INT,
    points_per_game FLOAT,
    yards_per_play FLOAT,
    explosiveness FLOAT,
    success_rate FLOAT,
    redzone_pct FLOAT,
    -- Defensive Efficiency
    points_allowed_per_game FLOAT,
    yards_allowed_per_play FLOAT,
    havoc_rate FLOAT,
    third_down_defense_pct FLOAT,
    redzone_defense_pct FLOAT,
    -- Turnover / Situational Metrics
    turnover_margin FLOAT,
    fumble_rate FLOAT,
    interception_rate FLOAT,
    home_away_ppg_diff FLOAT,
    recent_form_ppg FLOAT,
    PRIMARY KEY (team_id, season, week),
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);


CREATE TABLE elo_ratings (
    team_id INT NOT NULL,
    season INT NOT NULL,
    week INT NOT NULL,
    elo FLOAT,
    PRIMARY KEY (team_id, season, week),
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);


CREATE TABLE vegas_lines (
    game_id BIGINT PRIMARY KEY,
    spread_close FLOAT,
    over_under_close FLOAT,
    moneyline_home INT,
    moneyline_away INT,
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);


CREATE TABLE line_movement (
    game_id BIGINT NOT NULL,
    timestamp DATETIME NOT NULL,
    sportsbook VARCHAR(50) NOT NULL,
    spread_home FLOAT,
    spread_away FLOAT,
    moneyline_home INT,
    moneyline_away INT,
    over_under FLOAT,
    PRIMARY KEY (game_id, timestamp, sportsbook),
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);


CREATE TABLE weather (
    game_id BIGINT PRIMARY KEY,
    temperature FLOAT,
    wind_speed FLOAT,
    precipitation FLOAT,
    humidity FLOAT,
    conditions VARCHAR(64),
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);


CREATE TABLE matchup_features (
    game_id BIGINT NOT NULL,
    season INT NOT NULL,
    week INT NOT NULL,
    -- Team strength features (difference: home - away)
    elo_diff FLOAT,
    points_per_game_diff FLOAT,
    points_allowed_per_game_diff FLOAT,
    yards_per_play_diff FLOAT,
    yards_allowed_per_play_diff FLOAT,
    explosiveness_diff FLOAT,
    success_rate_diff FLOAT,
    redzone_pct_diff FLOAT,
    havoc_rate_diff FLOAT,
    turnover_margin_diff FLOAT,
    -- Travel
    travel_distance FLOAT,
    -- Weather (home team perspective)
    temperature FLOAT,
    wind_speed FLOAT,
    precipitation FLOAT,
    humidity FLOAT,
    weather_conditions_score FLOAT, -- e.g., encoded or categorical numeric feature
    -- Vegas / line features
    vegas_spread_close FLOAT,
    vegas_over_under_close FLOAT,
    spread_change FLOAT,           -- from line_movement table
    spread_volatility FLOAT,
    line_slope FLOAT,
    sharp_move FLOAT,              -- weighted by sportsbook
    -- Target variable
    home_win BOOLEAN,
    PRIMARY KEY (game_id, season, week),
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);


CREATE TABLE weekly_predictions (
    game_id BIGINT NOT NULL,
    season INT NOT NULL,
    week INT NOT NULL,
    pred_prob_logreg FLOAT,
    pred_prob_gbm FLOAT,
    model_edge_logreg FLOAT,
    model_edge_gbm FLOAT,
    confidence_rank_logreg INT,
    confidence_rank_gbm INT,
    PRIMARY KEY (game_id, season, week),
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);

CREATE INDEX idx_team_stats_team_season_week ON team_stats(team_id, season, week);
CREATE INDEX idx_elo_team_season_week ON elo_ratings(team_id, season, week);
CREATE INDEX idx_line_movement_game_time ON line_movement(game_id, timestamp);
CREATE INDEX idx_games_season_week ON games(season, week);
