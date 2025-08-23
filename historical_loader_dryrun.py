# historical_loader_dryrun_simple.py

import pandas as pd
from sqlalchemy import create_engine
from utils import fetch_teams_api, fetch_games_api
from dynaconf import Dynaconf

# -------------------------------
# Config
# -------------------------------
settings = Dynaconf(envvar_prefix="MYAPP", load_dotenv=True)
API_KEY = settings.CFDB_API_KEY
DATABASE_URL = settings.DB_ENGINE_URL
TEST_SEASONS = [2022]  # only one season for dry run

# -------------------------------
# Create DB engine
# -------------------------------
engine = create_engine(DATABASE_URL)

# -------------------------------
# Fetch teams
# -------------------------------
print("Fetching teams...")
teams_df = fetch_teams_api(API_KEY)
print(f"Teams fetched: {len(teams_df)}")

# -------------------------------
# Fetch games
# -------------------------------
print(f"Fetching games for seasons: {TEST_SEASONS}")
games_df = fetch_games_api(API_KEY, TEST_SEASONS)
print(f"Games fetched: {len(games_df)}")

# -------------------------------
# Merge team lat/lon for home team
# -------------------------------
games_df = games_df.merge(
    teams_df[['team_id', 'lat', 'lon']],
    left_on='home_team_id',
    right_on='team_id',
    how='left'
).rename(columns={'lat': 'home_lat', 'lon': 'home_lon'})

# Check for missing coordinates
missing = games_df[['home_lat', 'home_lon']].isna().sum()
print("Missing lat/lon:", missing)

# -------------------------------
# Load into DB (dry run)
# -------------------------------
print("Writing games to DB (append)...")
games_df.to_sql("games", con=engine, if_exists="append", index=False)

print("Dry run complete! Check your database tables.")