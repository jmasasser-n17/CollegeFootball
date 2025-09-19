import dash
import pandas as pd
from sqlalchemy import create_engine
from dynaconf import Dynaconf
from dash import dcc, html, Input, Output, Dash, dash_table
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px

# ------------------
# Read Database
# ------------------
def build_engine():
    settings = Dynaconf(envvar_prefix = "MYAPP", load_dotenv = True)
    return create_engine(settings.DB_ENGINE_URL, echo=False)
SEASON = 2025
WEEK = 4

engine = build_engine()
#Load Relevant Data for the week
with engine.begin() as conn:
    teams_df = pd.read_sql("SELECT * FROM teams", conn)
    games_df = pd.read_sql(f"SELECT * FROM games WHERE season={SEASON} AND week={WEEK}", conn)
    injuries_df = pd.read_sql("SELECT * FROM injuries", conn)
    line_df = pd.read_sql("SELECT * FROM line_movement", conn)
    valid_game_ids = set(games_df['game_id'])
    line_df = line_df[
        line_df['game_id'].isin(valid_game_ids)
    ]
    pred_df = pd.read_sql(f"SELECT * FROM weekly_predictions WHERE season={SEASON} AND week={WEEK}", conn)
    shap_df = pd.read_sql(f"SELECT * FROM shap_feature_importances WHERE season={SEASON} AND week={WEEK}", conn)

    #Add team names to games
    games_df = games_df.merge(teams_df[['team_id', 'team_name']], left_on='home_team_id', right_on='team_id', how='left').rename(columns={'team_name': 'home_team_name'})
    games_df = games_df.merge(teams_df[['team_id', 'team_name']], left_on='away_team_id', right_on='team_id', how='left').rename(columns={'team_name': 'away_team_name'})

# Dropdown options: search by home or away team name
games_df['label'] = games_df.apply(lambda row: f"{row['home_team_name']} vs {row['away_team_name']}", axis=1)

with open("feature_selection.html", "r", encoding="utf-8") as f:
    report_html = f.read()

# -------------------------------------- App -----------------------------------------------------------------

app = Dash(external_stylesheets=[dbc.themes.MATERIA, dbc.icons.FONT_AWESOME], suppress_callback_exceptions=True)

sidebar = html.Div(
    [
        html.Div(
            [
                html.H2(["Football", html.Br(), "Dashboard"], style={"color": "white"}),
            ],
            className="sidebar-header",
        ),
        html.Hr(),
        dbc.Nav(
            [
                dbc.NavLink(
                    [html.I(className="fas fa-home me-2"), html.Span("Dashboard")],
                    href='/',
                    active="exact"
                ),
                dbc.NavLink(
                    [
                        html.I(className="fa-solid fa-football"),
                        html.Span(" Match Ups")
                    ],
                    href='/matchups',
                    active="exact"
                ),
                dbc.NavLink(
                    [
                        html.I(className="fa-solid fa-ranking-star"),
                        html.Span("Rankings", style={"marginLeft": "8px"})
                    ],
                    href='/rankings',
                    active='exact'
                ),
                dbc.NavLink(
                    [
                        html.I(className="fas fa-envelope-open-text me-2"),
                        html.Span("Report")
                    ],
                    href='/report',
                    active='exact'
                ),
                dbc.NavLink(
                    [
                        html.I(className="fa-solid fa-database me-2"),
                        html.Span("Datasets"),
                    ],
                    href="/datasets",
                    active="exact",
                ),
            ],
            vertical=True,
            pills=True,
        ),
    ],
    className="sidebar",
)

app.layout = dbc.Container([
    dcc.Location(id='url', refresh=False),
    dbc.Row([
        dbc.Col(sidebar, width=2),
        dbc.Col(id='page-content', width=10)
    ])
], fluid=False)

# --------------------------------------- Page Routing Callback ------------------------------------------------
@app.callback(
    Output('page-content', 'children'),
    Input('url', 'pathname')
)
def display_name(pathname):
    if pathname == '/matchups':
        return [
            dbc.Row([
                html.H3("Select a Game", style={"marginBottom": "10px"})
            ]),
            dbc.Row([
                dcc.Dropdown(
                    id='game-dropdown',
                    options=[{'label': l, 'value': gid} for l, gid in zip(games_df['label'], games_df['game_id'])],
                    value=games_df['game_id'].iloc[0],
                    placeholder="Search by team name...",
                    searchable=True,
                    style={'marginBottom': '20px'}
                )
            ]),
            dbc.Row([
                dbc.Col(id='prediction-card', width=4),
                dbc.Col([
                    html.H4("Vegas Lines", style={"textAlign": "center"}),
                    dcc.Graph(id='line-movement-chart')], width=8)
            ], align="center"),
            html.Hr(),
            dbc.Row([
                dbc.Col([
                    html.H4("Injuries", style={"textAlign": "center"}),
                    html.Div(id='injury-table')
                ], width=6),
                dbc.Col([
                    html.H4('Top SHAP Features', style={"textAlign": "center"}),
                    dcc.Graph(id='shap-bar')
                ], width=6)
            ], style={"maxHeight": "500px"})
        ]
    elif pathname == '/rankings':
       return [
            dbc.Row([
                html.H3("Select Up to 10 Games", style={"marginBottom": "10px"})
            ]),
            dbc.Row([
                dcc.Dropdown(
                    id='rankings-game-dropdown',
                    options=[{'label': l, 'value': gid} for l, gid in zip(games_df['label'], games_df['game_id'])],
                    multi=True,
                    placeholder="Choose games...",
                    style={'marginBottom': '20px'}
                )
            ]),
            dbc.Row([
                dbc.Col([
                    html.H4("Predicted Winners (Ordered by Probability)", style={"textAlign": "center"}),
                    dash_table.DataTable(
                        id='rankings-table',
                        style_table={'overflowX': 'auto', 'margin': '0 auto', 'width': '80%'},
                        style_header={
                            'backgroundColor': '#007bff',
                            'color': 'white',
                            'fontWeight': 'bold',
                            'textAlign': 'center'
                        },
                        style_cell={
                            'textAlign': 'center',
                            'padding': '8px',
                            'fontFamily': 'Arial',
                            'fontSize': '16px'
                        },
                        style_data_conditional=[
                            {
                                'if': {'row_index': 'odd'},
                                'backgroundColor': '#f2f2f2'
                            },
                            {
                                'if': {'column_id': 'win_probability'},
                                'color': '#007bff',
                                'fontWeight': 'bold'
                            }
                        ],
                        style_as_list_view=True,
                        page_size=10  # Optional: adds pagination
                    )
                ], width=12)
            ])
        ]
    
    elif pathname == '/report':
         return html.Div([
            html.Iframe(srcDoc=report_html, style={"width": "100%", "height": "1000px", "border": "none"})
        ])
    elif pathname == '/datasets':
        works_cited_md = """
    # Works Cited

    **Boll, Luke.** *Gridiron Genius: Using Neural Networks to Predict College Football.* University of Michigan Honors Capstone Report, 2024. Deep Blue, University of Michigan, [https://deepblue.lib.umich.edu/bitstream/handle/2027.42/176935/Luke_Boll_Honors_Capstone_Report_-_Luke_Boll.pdf?sequence=1](https://deepblue.lib.umich.edu/bitstream/handle/2027.42/176935/Luke_Boll_Honors_Capstone_Report_-_Luke_Boll.pdf?sequence=1). Accessed 19 Sept. 2025.

    **mexwell.** *NCAA Stadiums.* Kaggle, 2020, [https://www.kaggle.com/datasets/mexwell/ncaa-stadiums](https://www.kaggle.com/datasets/mexwell/ncaa-stadiums). Accessed 19 Sept. 2025.

    **“College Football Data.”** *CollegeFootballData.com,* [https://collegefootballdata.com/](https://collegefootballdata.com/). Accessed 19 Sept. 2025.

    **“Covers.com: Sports Betting Odds, Lines, Picks & News.”** *Covers.com,* [https://www.covers.com/](https://www.covers.com/). Accessed 19 Sept. 2025.

    """
        return html.Div([
            html.H2("Datasets & Works Cited", style={"textAlign": "center", "marginBottom": "24px"}),
            dcc.Markdown(works_cited_md, style={"background": "#f8f9fa", "borderRadius": "10px", "padding": "32px 24px", "boxShadow": "0 2px 8px rgba(0,0,0,0.04)", "maxWidth": "900px", "margin": "0 auto"})
        ])
    elif pathname == '/' or pathname == '' or pathname is None:
        return dbc.Container([
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H2("College Football Dashboard", className="card-title", style={"textAlign": "center"}),
                        html.P(
                            "Explore predictions, rankings, Vegas lines, injuries, and feature importances for this week's college football matchups.",
                            className="card-text",
                            style={"textAlign": "center"}
                        ),
                        html.Hr(),
                        html.Div([
                            dbc.Button("View Matchups", href="/matchups", color="primary", className="me-2"),
                            dbc.Button("Rankings", href="/rankings", color="info", className="me-2"),
                            dbc.Button("Report", href="/report", color="secondary")
                        ], style={"textAlign": "center"})
                    ])
                ], style={"maxWidth": "500px", "margin": "40px auto", "boxShadow": "0 2px 12px rgba(0,0,0,0.08)"})
            ], width=12)
        ])
    ])

@app.callback(
    Output('prediction-card', 'children'),
    Output('line-movement-chart', 'figure'),
    Output('injury-table', 'children'),
    Output('shap-bar', 'figure'),
    Input('game-dropdown', 'value')
)
def update_all_charts(game_id):
    if not game_id:
        return dbc.Card("Select a game above."), go.Figure(), "Select a game.", go.Figure()
    
    game_row = games_df[games_df['game_id'] == game_id].iloc[0]
    home_team = game_row['home_team_name']
    away_team = game_row['away_team_name']

    #Prediction Card
    pred_row = pred_df[pred_df['game_id'] == game_id]
    if not pred_row.empty:
        winner = home_team if pred_row['predicted_home_win'].iloc[0] else away_team
        prob = pred_row['home_win_probability'].iloc[0] if pred_row['predicted_home_win'].iloc[0] else 1 - pred_row['home_win_probability'].iloc[0]
        card = dbc.Card([
            dbc.CardHeader("Prediction"),
            dbc.CardBody([
                html.H4(f"{winner}", className='card-title'),
                html.P(f"Win Probability: {prob:.2%}", className='card-text')
            ])
        ], color="primary", inverse=True)
    else:
        card = dbc.Card("No Prediction Available")
    
    #Line Chart
    line_game = line_df[(line_df['game_id'] == game_id)]
    if not line_game.empty:
        fig = px.line(
            line_game,
            x='timestamp',
            y='spread',
            color='sportsbook',
            labels={'spread': 'Spread', 'timestamp': 'Time'},
            height=450
        )
        fig.update_layout(legend_title_text='Sportsbook')
    else:
        fig = go.Figure()

    #Injuries Table
    home_inj = injuries_df[injuries_df['team_id'] == game_row['home_team_id']]
    away_inj = injuries_df[injuries_df['team_id'] == game_row['away_team_id']]
    inj_table = html.Div([
        html.H5(f'{home_team}'),
        dash_table.DataTable(
            data=home_inj.to_dict('records'),
            columns=[{"name": i, "id": i} for i in home_inj.columns],
            style_table={'overflowX': 'auto'}
        ) if not home_inj.empty else html.P("No Reported Injuries"),
        html.H5(f"{away_team}"),
        dash_table.DataTable(
            data=away_inj.to_dict('records'),
            columns=[{"name": i, "id": i} for i in away_inj.columns],
            style_table={'overflowX': 'auto'}
        ) if not away_inj.empty else html.P("No Reported Injuries")
    ])

    #SHAP features
    shap_game = shap_df[shap_df['game_id'] == game_id]
    if not shap_game.empty:
        shap_top = shap_game.nlargest(5, 'shap_value', keep='all')
        shap_top = shap_game.sort_values('shap_value', ascending=False)
        colors = ['blue' if v > 0 else 'red' for v in shap_top['shap_value']]
        fig_shap = go.Figure(go.Bar(
            x=shap_top['shap_value'],
            y=shap_top['feature'],
            marker_color=colors,
            orientation='h'
        ))
        fig_shap.update_layout(
            title="Blue: Home Strength -- Red: Away Strength",
            xaxis_title = "Feature",
            yaxis_title = "SHAP Value",
            height=350,
            width=500
        )
    else:
        fig_shap = go.Figure()
        fig_shap.update_layout(
            height=350,
            width=500
        )
    return card, fig, inj_table, fig_shap

@app.callback(
    Output('rankings-table', 'data'),
    Output('rankings-table', 'columns'),
    Input('rankings-game-dropdown', 'value')
)
def update_rankings_table(selected_game_ids):
    if not selected_game_ids:
        return [], [{"name": "No games selected", "id": "empty"}]
    # Limit to 10 games
    selected_game_ids = selected_game_ids[:10]
    # Filter predictions
    preds = pred_df[pred_df['game_id'].isin(selected_game_ids)].copy()
    preds['winner'] = preds.apply(
        lambda row: games_df.loc[games_df['game_id'] == row['game_id'], 'home_team_name'].values[0]
        if row['predicted_home_win'] else games_df.loc[games_df['game_id'] == row['game_id'], 'away_team_name'].values[0],
        axis=1
    )
    preds['win_probability'] = preds.apply(
        lambda row: row['home_win_probability'] if row['predicted_home_win'] else 1 - row['home_win_probability'],
        axis=1
    )
    # Order by probability descending
    preds = preds.sort_values('win_probability', ascending=False)
    # Prepare table data
    table_data = preds[['game_id', 'winner', 'win_probability']].to_dict('records')
    table_columns = [
        {"name": "Game", "id": "game_id"},
        {"name": "Predicted Winner", "id": "winner"},
        {"name": "Win Probability", "id": "win_probability", "type": "numeric", "format": dash_table.FormatTemplate.percentage(2)}
    ]
    return table_data, table_columns


if __name__ == "__main__":
    app.run(debug=True)

