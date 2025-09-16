import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from utils import (
    calculate_all_teams_elo
)
from sqlalchemy import create_engine, text
from dynaconf import Dynaconf

settings = Dynaconf(
    envvar_prefix="MYAPP",
    load_dotenv=True
)

DATABASE_URL = settings.DB_ENGINE_URL
def main():
    engine = create_engine(DATABASE_URL) #type: ignore
    
    with engine.begin() as conn:
        games = pd.read_sql("SELECT * FROM games", conn)
        team_ids = pd.read_sql("SELECT team_id FROM teams", conn)
        team_ids = team_ids['team_id'].tolist()

    elo_df = calculate_all_teams_elo(games, team_ids)

    dupes = elo_df[elo_df.duplicated(subset=["team_id", "season", "week"], keep=False)]
    print("Duplicates found:\n", dupes)
    with engine.begin() as conn:
        elo_df.to_sql("elo_ratings", conn, if_exists="append", index=False)

if __name__ == "__main__":
    main()



