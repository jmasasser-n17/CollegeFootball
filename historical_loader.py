# historical_loader.py

import os
import pandas as pd
from utils import (
    fetch_teams_api,
    fetch_games_api,
    fetch_vegas_lines_api,
    fetch_team_stats_api,
    populate_historical_weather_safe
)
from sqlalchemy import create_engine
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
    engine = create_engine(DATABASE_URL)

    # -------------------------------
    # Load Teams
    # -------------------------------
    print("Fetching teams...")
    teams_df = fetch_teams_api(API_KEY)
    teams_df.to_sql("teams", engine, if_exists="replace", index=False)

    # -------------------------------
    # Load Games
    # -------------------------------
    print("Fetching games...")
    games_df = fetch_games_api(API_KEY, SEASONS)

    # Merge in team lat/lon for weather
    teams_subset = teams_df[['team_id', 'lat', 'lon']].rename(
        columns={'lat': 'home_lat', 'lon': 'home_lon'}
    )
    games_df = games_df.merge(
        teams_subset,
        left_on='home_team_id',
        right_on='team_id',
        how='left'
    ).drop(columns=['team_id'])

    games_df.to_sql("games", engine, if_exists="replace", index=False)

    # -------------------------------
    # Load Vegas Lines
    # -------------------------------
    print("Fetching Vegas lines...")
    lines_df = fetch_vegas_lines_api(API_KEY, SEASONS)
    lines_df.to_sql("vegas_lines", engine, if_exists="replace", index=False)

    # -------------------------------
    # Load Team Stats
    # -------------------------------
    print("Fetching team stats...")
    stats_df = fetch_team_stats_api(API_KEY, SEASONS, teams_df)
    stats_df.to_sql("team_stats", engine, if_exists="replace", index=False)

    # -------------------------------
    # Load Historical Weather
    # -------------------------------
    print("Fetching historical weather (safe)...")
    weather_df = populate_historical_weather_safe(games_df)
    weather_df.to_sql("weather", engine, if_exists="replace", index=False)

    print("Historical load complete!")


if __name__ == "__main__":
    main()

