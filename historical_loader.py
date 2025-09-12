# historical_loader.py

import os
import pandas as pd
from utils import (
    fetch_teams_api,
    fetch_games_api,
    fetch_vegas_lines_api,
    fetch_game_advanced_stats_api,
    fetch_elo_api
)
from sqlalchemy import create_engine, text
from dynaconf import Dynaconf

# -------------------------------
# Config
# -------------------------------


settings = Dynaconf(
    envvar_prefix="MYAPP",
    load_dotenv=True
)
API_KEY = settings.CFDB_API_KEY
SEASONS = list(range(2015, 2025))  # adjust historical range
DATABASE_URL = settings.DB_ENGINE_URL


def main():
    engine = create_engine(DATABASE_URL) #type: ignore

    # ------------------------------
    # Ensure Clear Working Conditions
    # ------------------------------
    with engine.begin() as conn:
        #conn.execute(text("DELETE FROM game_advanced_stats"))
        #conn.execute(text("DELETE FROM vegas_lines"))
        #conn.execute(text("DELETE FROM games"))
        #conn.execute(text("DELETE FROM teams"))
        conn.execute(text("DELETE FROM elo_ratings"))

    # -------------------------------
    # Load Teams
    # -------------------------------
    print("Fetching teams...")
    teams_df = fetch_teams_api(API_KEY) #type: ignore
    #teams_df.to_sql("teams", engine, if_exists="append", index=False)

    # -------------------------------
    # Load Games
    # -------------------------------
    print("Fetching games...")
    games_df = fetch_games_api(API_KEY, SEASONS) #type: ignore

    #Eliminate games with Non-FBS teams
    
    valid_team_ids = set(teams_df['team_id'])
    games_df = games_df[
        games_df['home_team_id'].isin(valid_team_ids) &
        games_df['away_team_id'].isin(valid_team_ids)
    ]

    #games_df.to_sql("games", engine, if_exists="append", index=False)
    
    # -------------------------------
    # Load Vegas Lines
    # -------------------------------
    """
    print("Fetching Vegas lines...")
    lines_df = fetch_vegas_lines_api(API_KEY, SEASONS) #type: ignore
    #Eliminate games with Non-FBS teams
    valid_game_ids = set(games_df['game_id'])
    lines_df = lines_df[lines_df['game_id'].isin(valid_game_ids)]
    lines_df.to_sql("vegas_lines", engine, if_exists="append", index=False)

    # -------------------------------
    # Load Team Stats
    # -------------------------------
    print("Fetching team stats...")
    stats_df = fetch_game_advanced_stats_api(API_KEY, SEASONS, teams_df, games_df) #type: ignore
    #Ensure no invalid games make it through
    stats_df = stats_df[stats_df['game_id'].isin(valid_game_ids)]
    stats_df.to_sql("game_advanced_stats", engine, if_exists="append", index=False)
    """

    # -------------------------------
    # Load ELO Data
    # -------------------------------
    print("Fetching ELO Data...")
    elo_df = fetch_elo_api(API_KEY, SEASONS, teams_df, games_df)
    print("Loading elo_df")
    elo_df = elo_df[elo_df['week'].notnull()]
    elo_df.to_sql("elo_ratings", engine, if_exists="append", index=False)



if __name__ == "__main__":
    main()

