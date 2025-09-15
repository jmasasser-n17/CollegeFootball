from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
from rapidfuzz import process, fuzz
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import os

def scrape_and_store_injuries():
    # --- Chrome options ---
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    service = Service("/opt/homebrew/bin/chromedriver")  # Adjust path as needed

    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.get("https://www.covers.com/sport/football/ncaaf/injuries")
    time.sleep(5)

    teams_data = []
    team_blocks = driver.find_elements(By.CLASS_NAME, "covers-CoversSeasonInjuries-blockContainer")
    for team in team_blocks:
        try:
            team_name = team.find_element(By.CLASS_NAME, "covers-CoversMatchups-teamName").text.strip()
            rows = team.find_elements(By.CSS_SELECTOR, "table.covers-CoversMatchups-Table tbody tr:not(.collapse)")
            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 3:
                    player = cols[0].text.strip()
                    position = cols[1].text.strip()
                    status = cols[2].text.strip()
                    teams_data.append({
                        "team": team_name,
                        "player": player,
                        "position": position,
                        "status": status
                    })
        except Exception as e:
            print(f"Skipping a team due to error: {e}")
    driver.quit()

    df = pd.DataFrame(teams_data)
    if df.empty:
        print("No injury data found.")
        return

    df['team'] = df['team'].str.replace('\n', ' ', regex=False).str.strip().str.title()
    df['player'] = df['player'].str.strip().str.title()
    df['status'] = df['status'].str.replace('\n', ' ', regex=False).str.strip()
    df[['injury_status', 'game_date']] = df['status'].str.extract(r'^(.*?)\s*\(\s*(.*?)\s*\)$')
    df = df.drop(columns=['status'])
    current_year = datetime.now().year
    df['game_date'] = pd.to_datetime(df['game_date'] + f' {current_year}', format='%a, %b %d %Y', errors='coerce')

    # --- Fuzzy match team to team_id ---
    mysql_user = os.environ.get("MYAPP_DB_USER", "root")
    mysql_pass = os.environ.get("MYAPP_DB_PASSWORD", "")
    mysql_host = os.environ.get("MYAPP_DB_HOST", "localhost")
    mysql_db = os.environ.get("MYAPP_DB_DATABASE", "college_football")
    engine = create_engine(f"mysql+mysqlconnector://{mysql_user}:{mysql_pass}@{mysql_host}/{mysql_db}")
    teams = pd.read_sql('SELECT team_id, team_name FROM teams', engine)
    def match_team_id(scraped_team, team_names, team_ids):
        result = process.extractOne(scraped_team, team_names, scorer=fuzz.token_sort_ratio)
        if result is None:
            return None
        match, score, idx = result
        if score > 80:
            return team_ids[idx]
        else:
            return None
    df['team_id'] = df['team'].apply(lambda x: match_team_id(x, teams['team_name'].tolist(), teams['team_id'].tolist()))
    df = df[df['team_id'].notnull()]

    # --- Write to MySQL ---
    df.to_sql("injuries", engine, if_exists="replace", index=False)
    print(f"Wrote {len(df)} injury records to MySQL.")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "injury_scraper_dag",
    default_args=default_args,
    description="Scrape NCAAF injuries and store in MySQL",
    schedule_interval="0 11 * * *",  # Every day at 11am
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    scrape_and_store = PythonOperator(
        task_id="scrape_and_store_injuries",
        python_callable=scrape_and_store_injuries,
    )

    