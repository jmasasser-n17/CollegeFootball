from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
from airflow.hooks.base import BaseHook
import requests
from airflow.models import Variable

# --- CONFIG ---
CFBD_API_KEY = 'your_cfbd_api_key'  # Store securely in Airflow Variables or Connections in production

def fetch_new_games(**kwargs):
    # Get current season and week from Airflow Variables
    season = Variable.get("cfbd_season", default_var=datetime.now().year)
    week = Variable.get("cfbd_week", default_var=1)
    url = f"https://api.collegefootballdata.com/games?year={season}&week={week}"
    headers = {'Authorization': f'Bearer {CFBD_API_KEY}'}
    response = requests.get(url, headers=headers)
    games = response.json()
    df_games = pd.DataFrame(games)
    context = kwargs['ti']
    context.xcom_push(key='games_df', value=df_games.to_dict())

def fetch_new_stats(**kwargs):
    season = Variable.get("cfbd_season", default_var=datetime.now().year)
    week = Variable.get("cfbd_week", default_var=1)
    url = f"https://api.collegefootballdata.com/advanced-stats/games?year={season}&week={week}"
    headers = {'Authorization': f'Bearer {CFBD_API_KEY}'}
    response = requests.get(url, headers=headers)
    stats = response.json()
    df_stats = pd.DataFrame(stats)
    context = kwargs['ti']
    context.xcom_push(key='stats_df', value=df_stats.to_dict())

def fetch_new_lines(**kwargs):
    season = Variable.get("cfbd_season", default_var=datetime.now().year)
    week = Variable.get("cfbd_week", default_var=1)
    url = f"https://api.collegefootballdata.com/lines?year={season}&week={week}"
    headers = {'Authorization': f'Bearer {CFBD_API_KEY}'}
    response = requests.get(url, headers=headers)
    lines = response.json()
    df_lines = pd.DataFrame(lines)
    context = kwargs['ti']
    
def append_to_db(table_name, df_dict):
    conn = BaseHook.get_connection("mysql_local")
    engine = create_engine(conn.get_uri())
    df = pd.DataFrame(df_dict)
    df.to_sql(table_name, engine, if_exists='append', index=False)

def append_games(**kwargs):
    df_dict = kwargs['ti'].xcom_pull(key='games_df')
    append_to_db('games', df_dict)

def append_stats(**kwargs):
    df_dict = kwargs['ti'].xcom_pull(key='stats_df')
    append_to_db('game_advanced_stats', df_dict)

def append_lines(**kwargs):
    df_dict = kwargs['ti'].xcom_pull(key='lines_df')
    append_to_db('vegas_lines', df_dict)

def update_elo_ratings(**kwargs):
    # Placeholder: implement your elo calculation logic here
    # You can fetch games from DB, calculate new ratings, and update the elo_ratings table
    pass

def update_matchup_features(**kwargs):
    # Placeholder: implement your matchup feature calculation logic here
    # Use games, stats, lines, and elo_ratings tables to generate features
    pass

default_args = {
    'owner': 'you',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'load_new_games_dag',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
) as dag:
    fetch_games_task = PythonOperator(
        task_id='fetch_new_games',
        python_callable=fetch_new_games,
        provide_context=True,
    )
    fetch_stats_task = PythonOperator(
        task_id='fetch_new_stats',
        python_callable=fetch_new_stats,
        provide_context=True,
    )
    fetch_lines_task = PythonOperator(
        task_id='fetch_new_lines',
        python_callable=fetch_new_lines,
        provide_context=True,
    )

    append_games_task = PythonOperator(
        task_id='append_games',
        python_callable=append_games,
        provide_context=True,
    )
    append_stats_task = PythonOperator(
        task_id='append_stats',
        python_callable=append_stats,
        provide_context=True,
    )
    append_lines_task = PythonOperator(
        task_id='append_lines',
        python_callable=append_lines,
        provide_context=True,
    )

    update_elo_task = PythonOperator(
        task_id='update_elo_ratings',
        python_callable=update_elo_ratings,
        provide_context=True,
    )
    update_features_task = PythonOperator(
        task_id='update_matchup_features',
        python_callable=update_matchup_features,
        provide_context=True,
    )

    # DAG dependencies
    [fetch_games_task, fetch_stats_task, fetch_lines_task] >> [append_games_task, append_stats_task, append_lines_task]
    [append_games_task, append_stats_task, append_lines_task] >> update_elo_task >> update_features_task