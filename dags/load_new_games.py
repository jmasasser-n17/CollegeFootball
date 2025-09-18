from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text, bindparam
from airflow.hooks.base import BaseHook
import requests
from airflow.models import Variable
import os
from utils import parse_api_date, calculate_all_teams_elo, calculate_matchup_features
import pyarrow as pa
import pyarrow.parquet as pq
import glob
import joblib
import shap

CFBD_BASE_URL = "https://api.collegefootballdata.com"

TEAMS_ENDPOINT = f"{CFBD_BASE_URL}/teams/fbs"
GAMES_ENDPOINT = f"{CFBD_BASE_URL}/games"
VEGAS_LINES_ENDPOINT = f"{CFBD_BASE_URL}/lines"
SCHEDULE_ENDPOINT = f"{CFBD_BASE_URL}/schedule"
ADVANCED_STATS_ENDPOINT = f"{CFBD_BASE_URL}/stats/game/advanced"
ELO_ENDPOINT = f"{CFBD_BASE_URL}/ratings/elo"

# --- CONFIG ---
CFBD_API_KEY = os.getenv('MYAPP_CFDB_API_KEY')

def fetch_new_games(**kwargs):
    # Get current season and week from Airflow Variables
    season = Variable.get("cfbd_season", default_var=datetime.now().year)
    week = Variable.get("cfbd_week", default_var=1)
    url = f"https://api.collegefootballdata.com/games?year={season}&week={week}&classification=fbs"
    headers = {'Authorization': f'Bearer {CFBD_API_KEY}'}
    response = requests.get(url, headers=headers)
    games_json = response.json()
    
    records = []
    for g in games_json:
        game_id = g.get("id")
        season = g.get("season")
        week = g.get("week")
        home_team_id = g.get("homeId")
        away_team_id = g.get("awayId")
        home_points = g.get("homePoints")
        away_points = g.get("awayPoints")
        venue = g.get("venue")
        date = parse_api_date(g.get("startDate"))
        neutral_site = g.get("neutralSite")
        
        records.append({
            "game_id": game_id,
            "season": season,
            "week": week,
            "home_team_id": home_team_id,
            "away_team_id": away_team_id,
            "home_points": home_points,
            "away_points": away_points,
            "venue": venue,
            "date": date,
            "neutral_site": neutral_site
        })

    games_df = pd.DataFrame(records)
    file_path = '/tmp/games.parquet'
    table_pa = pa.Table.from_pandas(games_df)
    pq.write_table(table_pa, file_path)
    context = kwargs['ti']
    context.xcom_push(key='games_path', value=file_path)


def fetch_new_stats(**kwargs):
    season = Variable.get("cfbd_season", default_var=datetime.now().year)
    week = Variable.get("cfbd_week", default_var=1)
    url = f"https://api.collegefootballdata.com/stats/game/advanced?year={season}&week={week}"
    headers = {'Authorization': f'Bearer {CFBD_API_KEY}'}
    response = requests.get(url, headers=headers)
    stats_json = response.json()
    
    conn = BaseHook.get_connection("mysql_local")
    engine = create_engine(conn.get_uri())
    
    with engine.begin() as conn:
        teams_df = pd.read_sql('SELECT team_id, team_name FROM teams', conn)
    team_map = dict(zip(teams_df['team_name'], teams_df['team_id']))


    records = []

    for g in stats_json:
        game_id = g.get("gameId")
        season_val = g.get("season")
        week = g.get("week")
        team_name = g.get("team")
        opponent_name = g.get("opponent")

        offense = g.get("offense", {})
        defense = g.get("defense", {})

        # Flatten offense
        off_pass_plays = offense.get("passingPlays", {})
        off_rush_plays = offense.get("rushingPlays", {})
        off_pass_downs = offense.get("passingDowns", {})
        off_stand_downs = offense.get("standardDowns", {})

        # Flatten defense
        def_pass_plays = defense.get("passingPlays", {})
        def_rush_plays = defense.get("rushingPlays", {})
        def_pass_downs = defense.get("passingDowns", {})
        def_stand_downs = defense.get("standardDowns", {})

        team_id = team_map.get(team_name)
        opponent_id = team_map.get(opponent_name)
        if team_id is None or opponent_id is None:
            continue  # Only keep games where both teams are FBS

        records.append({
            "game_id": game_id,
            "season": season_val,
            "week": week,
            "team_id": team_id,
            "opponent_id": opponent_id,

            # Offense: passing plays
            "offense_passing_plays_explosiveness": off_pass_plays.get("explosiveness"),
            "offense_passing_plays_success_rate": off_pass_plays.get("successRate"),
            "offense_passing_plays_total_ppa": off_pass_plays.get("totalPPA"),
            "offense_passing_plays_ppa": off_pass_plays.get("ppa"),

            # Offense: rushing plays
            "offense_rushing_plays_explosiveness": off_rush_plays.get("explosiveness"),
            "offense_rushing_plays_success_rate": off_rush_plays.get("successRate"),
            "offense_rushing_plays_total_ppa": off_rush_plays.get("totalPPA"),
            "offense_rushing_plays_ppa": off_rush_plays.get("ppa"),

            # Offense: passing downs
            "offense_passing_downs_explosiveness": off_pass_downs.get("explosiveness"),
            "offense_passing_downs_success_rate": off_pass_downs.get("successRate"),
            "offense_passing_downs_ppa": off_pass_downs.get("ppa"),

            # Offense: standard downs
            "offense_standard_downs_explosiveness": off_stand_downs.get("explosiveness"),
            "offense_standard_downs_success_rate": off_stand_downs.get("successRate"),
            "offense_standard_downs_ppa": off_stand_downs.get("ppa"),

            # Offense: summary
            "offense_explosiveness": offense.get("explosiveness"),
            "offense_success_rate": offense.get("successRate"),
            "offense_total_ppa": offense.get("totalPPA"),
            "offense_ppa": offense.get("ppa"),
            "offense_drives": offense.get("drives"),
            "offense_plays": offense.get("plays"),
            "offense_open_field_yards_total": offense.get("openFieldYardsTotal"),
            "offense_open_field_yards": offense.get("openFieldYards"),
            "offense_second_level_yards_total": offense.get("secondLevelYardsTotal"),
            "offense_second_level_yards": offense.get("secondLevelYards"),
            "offense_line_yards_total": offense.get("lineYardsTotal"),
            "offense_line_yards": offense.get("lineYards"),
            "offense_stuff_rate": offense.get("stuffRate"),
            "offense_power_success": offense.get("powerSuccess"),

            # Defense: passing plays
            "defense_passing_plays_explosiveness": def_pass_plays.get("explosiveness"),
            "defense_passing_plays_success_rate": def_pass_plays.get("successRate"),
            "defense_passing_plays_total_ppa": def_pass_plays.get("totalPPA"),
            "defense_passing_plays_ppa": def_pass_plays.get("ppa"),

            # Defense: rushing plays
            "defense_rushing_plays_explosiveness": def_rush_plays.get("explosiveness"),
            "defense_rushing_plays_success_rate": def_rush_plays.get("successRate"),
            "defense_rushing_plays_total_ppa": def_rush_plays.get("totalPPA"),
            "defense_rushing_plays_ppa": def_rush_plays.get("ppa"),

            # Defense: passing downs
            "defense_passing_downs_explosiveness": def_pass_downs.get("explosiveness"),
            "defense_passing_downs_success_rate": def_pass_downs.get("successRate"),
            "defense_passing_downs_ppa": def_pass_downs.get("ppa"),

            # Defense: standard downs
            "defense_standard_downs_explosiveness": def_stand_downs.get("explosiveness"),
            "defense_standard_downs_success_rate": def_stand_downs.get("successRate"),
            "defense_standard_downs_ppa": def_stand_downs.get("ppa"),

            # Defense: summary
            "defense_explosiveness": defense.get("explosiveness"),
            "defense_success_rate": defense.get("successRate"),
            "defense_total_ppa": defense.get("totalPPA"),
            "defense_drives": defense.get("drives"),
            "defense_plays": defense.get("plays"),
            "defense_open_field_yards_total": defense.get("openFieldYardsTotal"),
            "defense_open_field_yards": defense.get("openFieldYards"),
            "defense_second_level_yards_total": defense.get("secondLevelYardsTotal"),
            "defense_second_level_yards": defense.get("secondLevelYards"),
            "defense_line_yards_total": defense.get("lineYardsTotal"),
            "defense_line_yards": defense.get("lineYards"),
            "defense_stuff_rate": defense.get("stuffRate"),
            "defense_power_success": defense.get("powerSuccess")
        })
    stats_df = pd.DataFrame(records)
    file_path = '/tmp/stats.parquet'
    table_pa = pa.Table.from_pandas(stats_df)
    pq.write_table(table_pa, file_path)
    context = kwargs['ti']
    context.xcom_push(key='stats_path', value=file_path)


def fetch_new_lines(**kwargs):
    season = Variable.get("cfbd_season", default_var=datetime.now().year)
    week = Variable.get("cfbd_week", default_var=1)
    url = f"https://api.collegefootballdata.com/lines?year={season}&week={week}"
    headers = {'Authorization': f'Bearer {CFBD_API_KEY}'}
    response = requests.get(url, headers=headers)
    lines_json = response.json()

    records = []
    for g in lines_json:

        lines = g.get("lines", [])
        if lines:
            line = lines[0]
        else:
            line = {}

        records.append({
            "season": g.get("season"),
            "week": g.get("week"),
            "home_team": g.get("homeTeam"),
            "away_team": g.get("awayTeam"),
            "spread_close": line.get("spread"),
            "over_under_close": line.get("overUnder"),
            "moneyline_home": line.get("homeMoneyline"),
            "moneyline_away": line.get("awayMoneyline")
        })
    lines_df = pd.DataFrame(records)
    file_path = '/tmp/lines.parquet'
    table_pa = pa.Table.from_pandas(lines_df)
    pq.write_table(table_pa, file_path)
    context = kwargs['ti']
    context.xcom_push(key='lines_path', value=file_path)
    
def append_to_db(table_name, df: pd.DataFrame):
    conn = BaseHook.get_connection("mysql_local")
    engine = create_engine(conn.get_uri())
    df.to_sql(table_name, engine, if_exists='append', index=False)

def append_games(**kwargs):
    file_path = kwargs['ti'].xcom_pull(key='games_path')
    table_pa = pa.Table.from_pandas(pd.read_parquet(file_path)) if hasattr(pa, 'Table') else pq.read_table(file_path)
    df = table_pa.to_pandas() if hasattr(table_pa, 'to_pandas') else pd.read_parquet(file_path)

    conn = BaseHook.get_connection("mysql_local")
    engine = create_engine(conn.get_uri())
    with engine.begin() as db_conn:
        season = int(df['season'].iloc[0])
        week = int(df['week'].iloc[0])
        teams_df = pd.read_sql('SELECT team_id FROM teams', db_conn)

        # Get game_ids for this week/season
        game_ids = df['game_id'].dropna().unique().tolist()

        if not df.empty:
            db_conn.execute(
                text("DELETE FROM game_advanced_stats WHERE season = :season AND week = :week"),
                {"season": season, "week": week}
            )
            if game_ids:
                db_conn.execute(
                    text("DELETE FROM vegas_lines WHERE game_id IN :game_ids").bindparams(
                        bindparam("game_ids", expanding=True)
                    ),
                    {"game_ids": game_ids}
                )

                db_conn.execute(
                    text("DELETE FROM matchup_features WHERE game_id IN :game_ids").bindparams(
                        bindparam("game_ids", expanding=True)
                    ),
                    {"game_ids": game_ids}
                )

                db_conn.execute(
                    text("DELETE FROM weekly_predictions WHERE game_id IN :game_ids").bindparams(
                        bindparam("game_ids", expanding=True)
                    ),
                    {"game_ids": game_ids}
                )
                db_conn.execute(
                    text("DELETE FROM line_movement WHERE game_id IN :game_ids").bindparams(
                        bindparam("game_ids", expanding=True)
                    ),
                    {"game_ids": game_ids}
                )
            db_conn.execute(
                text("DELETE FROM games WHERE season = :season AND week = :week"),
                {"season": season, "week": week}
            )
    valid_team_ids = set(teams_df['team_id'])
    df = df[
        df['home_team_id'].isin(valid_team_ids) &
        df['away_team_id'].isin(valid_team_ids)
    ]
    append_to_db('games', df)

def append_stats(**kwargs):
    file_path = kwargs['ti'].xcom_pull(key='stats_path')
    table_pa = pa.Table.from_pandas(pd.read_parquet(file_path)) if hasattr(pa, 'Table') else pq.read_table(file_path)
    df = table_pa.to_pandas() if hasattr(table_pa, 'to_pandas') else pd.read_parquet(file_path)
    append_to_db('game_advanced_stats', df)

def append_lines(**kwargs):
    file_path = kwargs['ti'].xcom_pull(key='lines_path')
    table_pa = pa.Table.from_pandas(pd.read_parquet(file_path)) if hasattr(pa, 'Table') else pq.read_table(file_path)
    lines_df = table_pa.to_pandas() if hasattr(table_pa, 'to_pandas') else pd.read_parquet(file_path)
    
    conn = BaseHook.get_connection("mysql_local")
    engine = create_engine(conn.get_uri())
    with engine.begin() as conn:
        games_df = pd.read_sql("SELECT game_id, season, week, home_team_id, away_team_id FROM games", conn)
        teams_df = pd.read_sql("SELECT team_id, team_name FROM teams", conn)
        team_name_to_id = dict(zip(teams_df['team_name'], teams_df['team_id']))

    lines_df['home_team_id'] = lines_df['home_team'].map(team_name_to_id)
    lines_df['away_team_id'] = lines_df['away_team'].map(team_name_to_id)

    merged_df = lines_df.merge(
        games_df,
        on=['season', 'week', 'home_team_id', 'away_team_id'],
        how='inner'
    )

    vegas_lines_df = merged_df[['game_id', 'spread_close', 'over_under_close', 'moneyline_home', 'moneyline_away']]

    append_to_db('vegas_lines', vegas_lines_df)

def update_elo_ratings(**kwargs):
    # Get current season from Airflow Variable
    season = int(Variable.get("cfbd_season", default_var=datetime.now().year))

    # Connect to DB and load games for this season
    conn = BaseHook.get_connection("mysql_local")
    engine = create_engine(conn.get_uri())
    with engine.begin() as db_conn:
        games_df = pd.read_sql(f"SELECT * FROM games WHERE season = {season}", db_conn)
        teams_df = pd.read_sql("SELECT team_id FROM teams", db_conn)
    team_ids = teams_df['team_id'].tolist()

    # Calculate ELOs for all teams for this season
    elo_df = calculate_all_teams_elo(games_df, team_ids, initial_elo=1500, k=20)

    # Upsert into elo_ratings table (replace existing for this season)
    with engine.begin() as db_conn:
        db_conn.execute(text(f"DELETE FROM elo_ratings WHERE season = {season}"))
        elo_df.to_sql('elo_ratings', db_conn, if_exists='append', index=False)

def update_matchup_features(**kwargs):
    # Get current season from Airflow Variable
    season = int(Variable.get("cfbd_season", default_var=datetime.now().year))

    # Connect to DB and load all relevant tables for this season
    conn = BaseHook.get_connection("mysql_local")
    engine = create_engine(conn.get_uri())
    with engine.begin() as db_conn:
        games_df = pd.read_sql(f"SELECT * FROM games WHERE season = {season}", db_conn)
        games_adv_stats_df = pd.read_sql(f"SELECT * FROM game_advanced_stats WHERE season = {season}", db_conn)
        teams_df = pd.read_sql("SELECT * FROM teams", db_conn)
        elo_df = pd.read_sql(f"SELECT * FROM elo_ratings WHERE season = {season}", db_conn)
        game_ids = games_df['game_id'].dropna().unique().tolist()
        if game_ids:
            lines_df = pd.read_sql(
                text("SELECT * FROM vegas_lines WHERE game_id IN :game_ids").bindparams(
                    bindparam("game_ids", expanding=True)
                ),
                db_conn,
                params={"game_ids": game_ids}
            )
        else:
            lines_df = pd.DataFrame()

    # Calculate matchup features
    matchup_features_df = calculate_matchup_features(
        games_df, games_adv_stats_df, teams_df, elo_df, lines_df
    )

    # Upsert into matchup_features table (replace existing for this season)
    with engine.begin() as db_conn:
        db_conn.execute(text(f"DELETE FROM matchup_features WHERE season = {season}"))
        matchup_features_df.to_sql('matchup_features', db_conn, if_exists='append', index=False)

def predict_weekly_games(**kwargs):
    import joblib
    from datetime import datetime

    # Get current season and week
    season = int(Variable.get("cfbd_season", default_var=datetime.now().year))
    week = int(Variable.get("cfbd_week", default_var=1))

    # Connect to DB
    conn = BaseHook.get_connection("mysql_local")
    engine = create_engine(conn.get_uri())
    with engine.begin() as db_conn:
        # Get games for this week
        games_df = pd.read_sql(
            f"SELECT * FROM games WHERE season = {season} AND week = {week}", db_conn
        )
        # Get matchup features for these games
        matchup_df = pd.read_sql(
            f"SELECT * FROM matchup_features WHERE season = {season} AND week = {week}", db_conn
        )
        matchup_df = matchup_df.merge(
            games_df[['game_id', 'home_team_id', 'away_team_id']],
            on='game_id',
            how='left'
        )
    #DEBUGGING
    print("matchup_df shape before dropna:", matchup_df.shape)
    print("matchup_df columns:", matchup_df.columns.tolist())
    print("Number of NaNs per column:\n", matchup_df.isna().sum())


    matchup_df = matchup_df.dropna().reset_index(drop=True)
    #DEBUGGING
    print("matchup_df shape after dropna:", matchup_df.shape)
    # Prepare features (drop columns not used for prediction)
    X_pred = matchup_df.drop(
        columns=['home_win', 'game_id', 'season', 'week', 'neutral_site', 'home_team_id', 'away_team_id'], errors='ignore'
    )

    print("X_pred shape:", X_pred.shape)

    if X_pred.shape[0] == 0:
        print("No games with complete features to predict this week.")
        return

    # Load model
    rf = joblib.load('random_forest_model.pkl')

    # Predict
    home_win_pred = rf.predict(X_pred)
    home_win_proba = rf.predict_proba(X_pred)[:, 1]

    explainer = shap.TreeExplainer(rf)
    shap_values = explainer.shap_values(X_pred)
    # debugging
    print("shap_values shape:", getattr(shap_values, "shape", type(shap_values)))
    if isinstance(shap_values, list):
        shap_values = shap_values[1]
    elif shap_values.ndim == 3:
        shap_values = shap_values[:,:,1]
    # debugging
    print("shap_values shape after slicing:", shap_values.shape)

    # Prepare records for DB
    records = []
    shap_records = []
    timestamp = datetime.now()
    for idx in range(len(matchup_df)):
        row = matchup_df.iloc[idx]
        records.append({
            "game_id": row["game_id"],
            "season": row["season"],
            "week": row["week"],
            "home_team_id": row["home_team_id"],
            "away_team_id": row["away_team_id"],
            "predicted_home_win": int(home_win_pred[idx]),
            "home_win_probability": float(home_win_proba[idx]),
            "timestamp": timestamp
        })
        for feature_idx, feature_name in enumerate(X_pred.columns):
            shap_records.append({
                "game_id": row["game_id"],
                "season": row["season"],
                "week": row["week"],
                "feature": feature_name,
                "shap_value": float(shap_values[idx][feature_idx]),
                "timestamp": timestamp
            })


    pred_df = pd.DataFrame(records)
    shap_df = pd.DataFrame(shap_records)

    # Upsert predictions for this week
    with engine.begin() as db_conn:
        db_conn.execute(
            text("DELETE FROM weekly_predictions WHERE season = :season AND week = :week"),
            {"season": season, "week": week}
        )
        pred_df.to_sql('weekly_predictions', db_conn, if_exists='append', index=False)

        db_conn.execute(
            text("DELETE FROM shap_feature_importances WHERE season = :season AND week = :week"),
            {"season": season, "week": week}
        )
        shap_df.to_sql('shap_feature_importances', db_conn, if_exists='append', index=False)

def cleanup_parquet_files(**kwargs):
    for f in glob.glob('/tmp/*.parquet'):
        try:
            os.remove(f)
        except Exception as e:
            print(f"Error deleting {f}: {e}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
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
    predict_task = PythonOperator(
        task_id='predict_weekly_games',
        python_callable=predict_weekly_games,
        provide_context=True,
    )
    cleanup_task = PythonOperator(
        task_id='cleanup_parquet_files',
        python_callable=cleanup_parquet_files,
        provide_context=True
    )


# Fetch games first, then fetch stats and lines
fetch_games_task >> fetch_stats_task
fetch_games_task >> fetch_lines_task

# Insert games first, then insert stats and lines
fetch_games_task >> append_games_task
fetch_stats_task >> append_stats_task
fetch_lines_task >> append_lines_task

append_games_task >> append_stats_task
append_games_task >> append_lines_task

[append_stats_task, append_lines_task] >> update_elo_task
update_elo_task >> update_features_task >> predict_task >> cleanup_task