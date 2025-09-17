import pandas as pd
from sqlalchemy import create_engine, text
import requests
from dynaconf import Dynaconf


CFBD_BASE_URL = "https://api.collegefootballdata.com"
TEAMS_ENDPOINT = f"{CFBD_BASE_URL}/teams/fbs"


settings = Dynaconf(
    envvar_prefix="MYAPP",
    load_dotenv=True
)
API_KEY = settings.CFDB_API_KEY
SEASONS = list(range(2015, 2025))  # adjust historical range
DATABASE_URL = settings.DB_ENGINE_URL

def fetch_team_lon(api_key: str) -> pd.DataFrame:
    headers = {"Authorization": f"Bearer {api_key}"}
    url = f"{TEAMS_ENDPOINT}"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    teams_json = response.json()

    records = []

    for t in teams_json:
        team_id = t.get("id")
        lon = t.get("location", {}).get("longitude", None)

        records.append({
            "team_id": team_id,
            "lon": lon
        })
    return pd.DataFrame(records)

def main():
    teams_df = fetch_team_lon(API_KEY)
    engine = create_engine(DATABASE_URL)

    with engine.begin() as conn:
        for _, row in teams_df.iterrows():
            if pd.isna(row["lon"]):
                continue
            conn.execute(
                text(
                    "UPDATE teams set lon = :lon WHERE team_id = :team_id"
                ),
                {"lon": row["lon"], "team_id": row["team_id"]}
            )
if __name__ == "__main__":
    main()
