from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import boto3
import pandas as pd
from sqlalchemy import create_engine, text
from airflow.hooks.base import BaseHook
import pyarrow as pa
import pyarrow.parquet as pq
import os

def fetch_dynamodb_data(**context):
    aws_access_key = os.getenv('MYAPP_AWS_ACCESS_KEY')
    aws_secret_key = os.getenv('MYAPP_AWS_SECRET_ACCESS_KEY')
    if not aws_access_key or not aws_secret_key:
        raise ValueError("AWS credentials not found in environment variables.")
    os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key
    os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_key
    
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('collegeFootballLinesSnapshots')
    items = []
    response = table.scan()
    items.extend(response['Items'])
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])
    print(f"Fetched {len(items)} items from DynamoDB")
    df = pd.DataFrame(items)
    print(f"Unique game_ids in DynamoDB: {df['gameId'].unique()[:5]}")
    df_line = pd.DataFrame({
        'game_id': df['gameId'],
        'timestamp': df['snapshotTime'],
        'sportsbook': df['provider'],
        'spread': df['spread'],
        'moneyline_home': df['homeMoneyline'],
        'moneyline_away': df['awayMoneyline'],
        'over_under': df['overUnder']
    })
    file_path = '/tmp/line_movement.parquet'
    table_pa = pa.Table.from_pandas(df_line)
    pq.write_table(table_pa, file_path)
    context['ti'].xcom_push(key='line_movement_path', value=file_path)

def insert_to_sql(**context):
    file_path = context['ti'].xcom_pull(key='line_movement_path')
    table_pa = pq.read_table(file_path)
    df = table_pa.to_pandas()
    print(f"Rows before filtering: {len(df)}")
    conn = BaseHook.get_connection("mysql_local")
    engine = create_engine(conn.get_uri())
    season = Variable.get("cfbd_season")
    with engine.begin() as conn_sql:
        conn_sql.execute(text("DELETE FROM line_movement"))
        games_df = pd.read_sql(f"SELECT * FROM games WHERE season = {season}", conn_sql)
    valid_game_ids = set(games_df['game_id'])
    print(f"Valid game)ids from games table: {list(valid_game_ids)[:5]}")
    df['game_id'] = df['game_id'].astype(int)
    df = df[
        df['game_id'].isin(valid_game_ids)
    ]
    print(f"Rows after filtering: {len(df)}")    
    df.to_sql('line_movement', engine, if_exists='append', index=False)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'load_line_movement_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    fetch_task = PythonOperator(
        task_id='fetch_dynamodb_data',
        python_callable=fetch_dynamodb_data,
        provide_context=True,
    )

    insert_task = PythonOperator(
        task_id='insert_to_sql',
        python_callable=insert_to_sql,
        provide_context=True,
    )

    fetch_task >> insert_task