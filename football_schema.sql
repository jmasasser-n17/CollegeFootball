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


CREATE TABLE game_advanced_stats (
    game_id BIGINT NOT NULL,
    season INT NOT NULL,
    week INT NOT NULL,
    team_id INT NOT NULL,
    opponent_id INT NOT NULL,

    -- Offense: passing plays
    offense_passing_plays_explosiveness FLOAT,
    offense_passing_plays_success_rate FLOAT,
    offense_passing_plays_total_ppa FLOAT,
    offense_passing_plays_ppa FLOAT,

    -- Offense: rushing plays
    offense_rushing_plays_explosiveness FLOAT,
    offense_rushing_plays_success_rate FLOAT,
    offense_rushing_plays_total_ppa FLOAT,
    offense_rushing_plays_ppa FLOAT,

    -- Offense: passing downs
    offense_passing_downs_explosiveness FLOAT,
    offense_passing_downs_success_rate FLOAT,
    offense_passing_downs_ppa FLOAT,

    -- Offense: standard downs
    offense_standard_downs_explosiveness FLOAT,
    offense_standard_downs_success_rate FLOAT,
    offense_standard_downs_ppa FLOAT,

    -- Offense: summary
    offense_explosiveness FLOAT,
    offense_success_rate FLOAT,
    offense_total_ppa FLOAT,
    offense_ppa FLOAT,
    offense_drives INT,
    offense_plays INT,
    offense_open_field_yards_total FLOAT,
    offense_open_field_yards FLOAT,
    offense_second_level_yards_total FLOAT,
    offense_second_level_yards FLOAT,
    offense_line_yards_total FLOAT,
    offense_line_yards FLOAT,
    offense_stuff_rate FLOAT,
    offense_power_success FLOAT,

    -- Defense: passing plays
    defense_passing_plays_explosiveness FLOAT,
    defense_passing_plays_success_rate FLOAT,
    defense_passing_plays_total_ppa FLOAT,
    defense_passing_plays_ppa FLOAT,

    -- Defense: rushing plays
    defense_rushing_plays_explosiveness FLOAT,
    defense_rushing_plays_success_rate FLOAT,
    defense_rushing_plays_total_ppa FLOAT,
    defense_rushing_plays_ppa FLOAT,

    -- Defense: passing downs
    defense_passing_downs_explosiveness FLOAT,
    defense_passing_downs_success_rate FLOAT,
    defense_passing_downs_ppa FLOAT,

    -- Defense: standards downs
    defense_standard_downs_explosiveness FLOAT,
    defense_standard_downs_success_rate FLOAT,
    defense_standard_downs_ppa FLOAT,

    -- Defense: summary
    defense_explosiveness FLOAT,
    defense_success_rate FLOAT,
    defense_total_ppa FLOAT,
    defense_ppa FLOAT,
    defense_drives INT,
    defense_plays INT,
    defense_open_field_yards_total FLOAT,
    defense_open_field_yards FLOAT,
    defense_second_level_yards_total FLOAT,
    defense_second_level_yards FLOAT,
    defense_line_yards_total FLOAT,
    defense_line_yards FLOAT,
    defense_stuff_rate FLOAT,
    defense_power_success FLOAT,

    PRIMARY KEY (game_id, team_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    FOREIGN KEY (team_id) REFERENCES teams(team_id),
    FOREIGN KEY (opponent_id) REFERENCES teams(team_id)
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

CREATE TABLE matchup_features (
    game_id BIGINT NOT NULL,
    season INT NOT NULL,
    week INT NOT NULL,
    -- Team strength features (difference: home - away)
    elo_diff FLOAT,
    points_per_game_diff FLOAT,
    points_allowed_per_game_diff FLOAT,
    margin_of_victory_diff FLOAT,
    win_rate_diff FLOAT,
    yards_per_play_diff FLOAT,
    yards_allowed_per_play_diff FLOAT,
    explosiveness_diff FLOAT,
    success_rate_diff FLOAT,
    travel_distance FLOAT,
    rest_days_diff FLOAT,
    neutral_site BOOLEAN,
    -- Vegas / line features
    vegas_spread_close FLOAT,
    vegas_over_under_close FLOAT,
    -- Target Variable
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

CREATE TABLE injuries (
    id INT AUTO_INCREMENT PRIMARY KEY,
    team_id INT NOT NULL,
    team VARCHAR(100),
    player VARCHAR(100),
    position VARCHAR(20),
    injury_status VARCHAR(100),
    game_date DATE,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);


