# Get predicted probabilities for home win
confidences = pd.DataFrame({
    'game_index': X_test.index,
    'home_win_proba': rf.predict_proba(X_test)[:, 1],
    'actual_home_win': y_test
})

# Sort by confidence
confidences_sorted = confidences.sort_values('home_win_proba', ascending=False)

# Assign pick'em ranks (10 = most confident, 1 = least confident)
confidences_sorted['pickem_rank'] = range(len(confidences_sorted), 0, -1)

# Show top 10 most confident picks
print(confidences_sorted.head(10))


import dash
import pandas as pd
from sqlalchemy import create_engine
from dynaconf import Dynaconf
from dash import dcc, html, Input, Output, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px

def build_engine():
    settings = Dynaconf(envvar_prefix = "MYAPP", load_dotenv = True)
    return create_engine(settings.DB_ENGINE_URL, echo=False)
SEASON = 2025
WEEK = 4

engine = build_engine()
with engine.begin() as conn:
    teams_df = pd.read_sql("SELECT * FROM teams", conn)
    games_df = pd.read_sql(f"SELECT * FROM games WHERE season={SEASON} AND week={WEEK}", conn)
    injuries_df = pd.read_sql("SELECT * FROM injuries", conn)
    line_df = pd.read_sql("SELECT * FROM line_movement", conn)
    valid_game_ids = set(games_df['game_id'])
    line_df = line_df[line_df['game_id'].isin(valid_game_ids)]
    pred_df = pd.read_sql(f"SELECT * FROM weekly_predictions WHERE season={SEASON} AND week={WEEK}", conn)
    shap_df = pd.read_sql(f"SELECT * FROM shap_feature_importances WHERE season={SEASON} AND week={WEEK}", conn)
    games_df = games_df.merge(teams_df[['team_id', 'team_name']], left_on='home_team_id', right_on='team_id', how='left').rename(columns={'team_name': 'home_team_name'})
    games_df = games_df.merge(teams_df[['team_id', 'team_name']], left_on='away_team_id', right_on='team_id', how='left').rename(columns={'team_name': 'away_team_name'})

# Dropdown options: search by home or away team name
games_df['label'] = games_df.apply(lambda row: f"{row['home_team_name']} vs {row['away_team_name']}", axis=1)

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.MATERIA])

app.layout = dbc.Container([
    html.H2("College Football Predictive Dashboard"),
    dcc.Dropdown(
        id='game-dropdown',
        options=[{'label': l, 'value': gid} for l, gid in zip(games_df['label'], games_df['game_id'])],
        placeholder="Search by team name...",
        searchable=True,
        style={'marginBottom': '20px'}
    ),
    dbc.Row([
        dbc.Col(id='prediction-card', width=4),
        dbc.Col(dcc.Graph(id='line-movement-chart'), width=8)
    ]),
    html.Hr(),
    dbc.Row([
        dbc.Col([
            html.H4("Injuries"),
            html.Div(id='injury-table')
        ], width=6),
        dbc.Col([
            html.H4("Top SHAP Features"),
            dcc.Graph(id='shap-bar')
        ], width=6)
    ])
], fluid=True)

 d
@app.callback(
    Output('prediction-card', 'children'),
    Output('line-movement-chart', 'figure'),
    Output('injury-table', 'children'),
    Output('shap-bar', 'figure'),
    Input('game-dropdown', 'value')
)
def update_game_view(game_id):
    if not game_id:
        return dbc.Card("Select a game above."), go.Figure(), "Select a game.", go.Figure()

    game_row = games_df[games_df['game_id'] == game_id].iloc[0]
    home_team = game_row['home_team_name']
    away_team = game_row['away_team_name']

    # Prediction Card
    pred_row = pred_df[pred_df['game_id'] == game_id]
    if not pred_row.empty:
        winner = home_team if pred_row['predicted_home_win'].iloc[0] else away_team
        prob = pred_row['home_win_probability'].iloc[0] if pred_row['predicted_home_win'].iloc[0] else 1 - pred_row['home_win_probability'].iloc[0]
        card = dbc.Card([
            dbc.CardHeader("Prediction"),
            dbc.CardBody([
                html.H4(f"{winner}", className="card-title"),
                html.P(f"Win Probability: {prob:.2%}", className="card-text")
            ])
        ], color="primary", inverse=True)
    else:
        card = dbc.Card("No prediction available.")

    # Line Movement Chart
    line_game = line_df[line_df['game_id'] == game_id]
    if not line_game.empty:
        fig = px.line(
            line_game,
            x='snapshotTime',
            y='spread',
            color='provider',
            title='Vegas Line Movement (Spread)',
            labels={'spread': 'Spread', 'snapshotTime': 'Time'}
        )
        fig2 = px.line(
            line_game,
            x='snapshotTime',
            y='overUnder',
            color='provider',
            title='Vegas Line Movement (Over/Under)',
            labels={'overUnder': 'Over/Under', 'snapshotTime': 'Time'}
        )
        # Combine spread and over/under in one figure (optional)
        for trace in fig2.data:
            fig.add_trace(trace)
        fig.update_layout(legend_title_text='Provider')
    else:
        fig = go.Figure()

    # Injuries Table
    home_inj = injuries_df[(injuries_df['team_id'] == game_row['home_team_id']) & (injuries_df['week'] == WEEK)]
    away_inj = injuries_df[(injuries_df['team_id'] == game_row['away_team_id']) & (injuries_df['week'] == WEEK)]
    inj_table = html.Div([
        html.H5(f"{home_team}"),
        dash_table.DataTable(
            data=home_inj.to_dict('records'),
            columns=[{"name": i, "id": i} for i in home_inj.columns],
            s.sa.y
        ) if not home_inj.empty else html.P("No reported injuries."),
        html.H5(f"{away_team}"),
        dash_table.DataTable(
            data=away_inj.to_dict('records'),
            columns=[{"name": i, "id": i} for i in away_inj.columns],
            style_table={'overflowX': 'auto'}
        ) if not away_inj.empty else html.P("No reported injuries.")
    ])

    # SHAP Feature Bar Chart
    shap_game = shap_df[shap_df['game_id'] == game_id]
    if not shap_game.empty:
        shap_top = shap_game.nlargest(5, 'shap_value', keep='all')
        shap_top = shap_top.sort_values('shap_value', ascending=False)
        colors = ['blue' if v > 0 else 'red' for v in shap_top['shap_value']]
        fig_shap = go.Figure(go.Bar(
            x=shap_top['feature'],
            y=shap_top['shap_value'],
            marker_color=colors,
            orientation='h'
        ))
        fig_shap.update_layout(
            title="Top 5 SHAP Features (Blue: Home Win, Red: Away Win)",
            xaxis_title="Feature",
            yaxis_title="SHAP Value"
        )
    else:
        fig_shap = go.Figure()

    return card, fig, inj_table, fig_shap

if __name__ == "__main__":
    app.run_server(debug=True)