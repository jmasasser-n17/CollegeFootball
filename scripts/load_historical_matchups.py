import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from utils import calculate_matchup_features
from sqlalchemy import create_engine
from dynaconf import Dynaconf

settings = Dynaconf(envvar_prefix="MYAPP",
                    load_dotenv=True)

DATABASE_URL = settings.DB_ENGINE_URL

def main():
    engine = create_engine(DATABASE_URL) #type: ignore

    with engine.begin() as conn:
        games_df = pd.read_sql("SELECT * FROM games", conn)
        games_adv_stats_df = pd.read_sql("SELECT * FROM game_advanced_stats", conn)
        teams_df = pd.read_sql("SELECT * FROM teams", conn)
        elo_df = pd.read_sql("SELECT * FROM elo_ratings", conn)
        lines_df = pd.read_sql("SELECT * FROM vegas_lines", conn)

    matchup_features_df = calculate_matchup_features(games_df, games_adv_stats_df, teams_df, elo_df, lines_df)

    with engine.begin() as conn:
        matchup_features_df.to_sql("matchup_features", conn, if_exists='append', index=False)

if __name__ == "__main__":
    main()