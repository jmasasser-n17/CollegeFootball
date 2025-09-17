 # utils.py

from datetime import datetime
import pandas as pd
import requests
import time
import os
import json
from typing import Optional
from geopy.distance import geodesic


CFBD_BASE_URL = "https://api.collegefootballdata.com"

TEAMS_ENDPOINT = f"{CFBD_BASE_URL}/teams/fbs"
GAMES_ENDPOINT = f"{CFBD_BASE_URL}/games"
TEAM_STATS_ENDPOINT = f"{CFBD_BASE_URL}/team/stats"
VEGAS_LINES_ENDPOINT = f"{CFBD_BASE_URL}/lines"
SCHEDULE_ENDPOINT = f"{CFBD_BASE_URL}/schedule"
ADVANCED_STATS_ENDPOINT = f"{CFBD_BASE_URL}/stats/game/advanced"
ELO_ENDPOINT = f"{CFBD_BASE_URL}/ratings/elo"



def parse_api_date(date_str: str) -> str:
    """
    Convert API datetime (e.g. '2025-08-22T01:44:44.173Z') 
    into a YYYY-MM-DD string.
    """
    if not date_str:
        return None
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")   # returns '2025-08-22'
    except Exception as e:
        print(f"Date parsing error for {date_str}: {e}")
        return None


# ------------------------------
# Fetch Teams & Games
# ------------------------------
def fetch_teams_api(api_key: str) -> pd.DataFrame:
    """
    Fetch all FBS teams from CFBD API, with cached geocoding.
    Returns:
        pd.DataFrame: team_id, team_name, stadium_name, lat, lon, conference
    """
    headers = {"Authorization": f"Bearer {api_key}"}
    url = f"{TEAMS_ENDPOINT}"  
    print(f"Fetching teams from: {url}")
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    teams_json = response.json()
    
    records = []

    for t in teams_json:
        team_id = t.get("id")
        team_name = t.get("school")
        conference = t.get("conference")
        stadium = t.get("location", {}).get("name", None)
        lat = t.get("location", {}).get("latitude", None)
        lon = t.get("location", {}).get("longitude", None)

        records.append({
            "team_id": team_id,
            "team_name": team_name,
            "stadium_name": stadium,
            "lat": lat,
            "lon": lon,
            "conference": conference
        })

    print(f"Teams fetched: {len(records)}")
    return pd.DataFrame(records)

def fetch_games_api(api_key: str, seasons: list[int]) -> pd.DataFrame:
    """
    Fetch all games for a list of seasons.
    Returns: game_id, season, week, home_team_id, away_team_id, scores, venue, date, neutral_site
    """
    headers = {"Authorization": f"Bearer {api_key}"}
    records = []
    
    for season in seasons:
        response = requests.get(f"{GAMES_ENDPOINT}?year={season}&classification=fbs", headers=headers)
        response.raise_for_status()
        games_json = response.json()

        for g in games_json:
            game_id = g.get("id")
            season = season
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
    print(f"Games Fetched: {len(records)}")
    return pd.DataFrame(records)

def get_weeks_per_season(games_df: pd.DataFrame, team_id: int) -> dict:
    """
    Returns a dict mapping season -> sorted list of week numbers for a given team in that season.

    Args:
        games_df: DataFrame with columns [game_id, season, week, home_team_id, away_team_id, home_points, away_points, venue, date, neutral_site]
        team_id: The team ID to filter for

    Returns:
        dict: {season: [week1, week2, ...], ...}
    """
    team_games = games_df[(games_df['home_team_id'] == team_id) | (games_df['away_team_id'] == team_id)]

    return team_games.groupby('season')['week'].unique().apply(sorted).to_dict()

def fetch_game_advanced_stats_api(api_key: str, seasons: list[int], teams_df: pd.DataFrame, games_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch per-team, per-week advanced stats for given seasons using chunky requests (one per team per season).
    Only includes games where both teams are FBS (in teams_df).
    Returns:
        DataFrame matching 'game_advanced_stats' table.
    """
    headers = {"Authorization": f"Bearer {api_key}"}
    team_names = teams_df['team_name']
    records = []
    team_map = dict(zip(teams_df['team_name'], teams_df['team_id']))

    for season in seasons:
        for team in team_names:
            while True:
                try:
                    response = requests.get(
                        f"{ADVANCED_STATS_ENDPOINT}?year={season}&team={team}",
                        headers=headers
                    )
                    response.raise_for_status()
                    break
                except requests.exceptions.HTTPError as e:
                    if response.status_code == 429:
                        print("Rate limit hit. Sleeping for 60 seconds...")
                        time.sleep(60)
                    else:
                        raise

            stats_json = response.json()
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
            time.sleep(1)  # Respect rate limits between teams
    print(f"Stats Fetched: {len(records)}")
    return pd.DataFrame(records)

def fetch_elo_api(api_key: str, seasons: list[int], teams_df: pd.DataFrame, games_df: pd.DataFrame):
    """
    Fetch ELO for all teams in all weeks in a list of given seasons.
    Args:
        api_key: CFBD API key
        seasons: list of seasons (years)
        teams_df: DataFrame with columns ['team_id', 'team_name']
        games_df: DataFrame with columns [game_id, season, week, home_team_id, away_team_id, scores, venue, date, neutral_site]

    Returns:
        DataFrame matching 'elo_ratings' table
    """
    headers = {"Authorization": f"Bearer {api_key}"}
    team_names = teams_df['team_name']
    records = []
    team_map = dict(zip(teams_df['team_name'], teams_df['team_id']))

    for season in seasons:
        for team in team_names:
            team_id = team_map.get(team)
            while True:
                try:
                    response = requests.get(
                        f"{ELO_ENDPOINT}?year={season}&team={team}",
                        headers=headers,
                        timeout=30
                    )
                    response.raise_for_status()
                    break
                except requests.exceptions.Timeout:
                    print(f"Timeout fetching ELO for {team} in {season}. Retrying.")
                    time.sleep(5)
                except requests.exceptions.HTTPError as e:
                    if response.status_code == 429:
                        print("Rate limit hit. Sleeping for 60 seconds...")
                        time.sleep(60)
                    else:
                        raise

            elo_json = response.json()
            for g in elo_json:
                week = g.get("week")
                elo = g.get("elo")
                print(f"Team: {team}, Season: {season}, Week: {week}, ELO: {elo}")
                if week is not None:
                    records.append({
                        "team_id": team_id,
                        "season": season,
                        "week": week,
                        "elo": elo
                    })
            time.sleep(1)
    print(f"ELO records fetched {len(records)}")
    return pd.DataFrame(records)


def fetch_vegas_lines_api(api_key: str, seasons: list[int]) -> pd.DataFrame:
    """
    Fetch closing Vegas lines for all games in given seasons.
    Returns a DataFrame ready to populate `vegas_lines` table:
    columns: game_id, spread_close, over_under_close, moneyline_home, moneyline_away
    """
    headers = {"Authorization": f"Bearer {api_key}"}
    records = []

    for season in seasons:
        response = requests.get(f"{VEGAS_LINES_ENDPOINT}?year={season}", headers=headers)
        response.raise_for_status()
        lines_json = response.json()

        for g in lines_json:
            # pick the first (or only) provider in the "lines" array
            lines = g.get("lines", [])
            if lines:
                line = lines[0]
            else:
                line = {}

            records.append({
                "game_id": g.get("id"),
                "spread_close": line.get("spread"),
                "over_under_close": line.get("overUnder"),
                "moneyline_home": line.get("homeMoneyline"),
                "moneyline_away": line.get("awayMoneyline")
            })
    print(f"Vegas Line Records Fetched {len(records)}")
    return pd.DataFrame(records)



def fetch_team_stats_api(api_key: str, seasons: list[int], teams_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch per-team, per-week stats for given seasons.
    
    Args:
        api_key: CFBD API key
        seasons: list of seasons (years)
        teams_df: DataFrame with columns ['team_id', 'team_name']
        
    Returns:
        DataFrame matching `team_stats` table schema
    """
    headers = {"Authorization": f"Bearer {api_key}"}
    records = []
    
    # Map team names to team_id
    team_map = dict(zip(teams_df['team_name'], teams_df['team_id']))

    for season in seasons:
        response = requests.get(f"{TEAM_STATS_ENDPOINT}?year={season}", headers=headers)
        try:
            response.raise_for_status()
            if response.text.strip() == "":
                print(f"Warning Empty response for season {season}")
                continue
            stats_json = response.json()
        except Exception as e:
            print(f"Error fetching team stats for season {season}: {e}")
            print(f"Response text: {response.text}")
            continue

        for team in stats_json:
            team_name = team.get("school")
            team_id = team_map.get(team_name)
            if team_id is None:
                print(f"Warning: team '{team_name}' not found in teams_df")
                continue

            weeks_data = team.get("weeks")
            if not weeks_data:
                # fallback: treat as season-level data
                weeks_data = [{"week": 0, **team}]

            for week_data in weeks_data:
                records.append({
                    "team_id": team_id,
                    "season": season,
                    "week": week_data.get("week", 0),
                    "games_played": week_data.get("gamesPlayed"),
                    "points_per_game": week_data.get("pointsPerGame"),
                    "yards_per_play": week_data.get("yardsPerPlay"),
                    "explosiveness": week_data.get("explosiveness"),
                    "success_rate": week_data.get("successRate"),
                    "redzone_pct": week_data.get("redzonePercent"),
                    "points_allowed_per_game": week_data.get("pointsAllowedPerGame"),
                    "yards_allowed_per_play": week_data.get("yardsAllowedPerPlay"),
                    "havoc_rate": week_data.get("havocRate"),
                    "third_down_defense_pct": week_data.get("thirdDownDefensePercent"),
                    "redzone_defense_pct": week_data.get("redzoneDefensePercent"),
                    "turnover_margin": week_data.get("turnoverMargin"),
                    "fumble_rate": week_data.get("fumbleRate"),
                    "interception_rate": week_data.get("interceptionRate"),
                    "home_away_ppg_diff": week_data.get("homeAwayPpgDiff"),
                    "recent_form_ppg": week_data.get("recentFormPPG")
                })

        # Respect rate limits
        time.sleep(1)

    return pd.DataFrame(records)



# --------------------------------
# Helper Functions for Team Stats
# --------------------------------

def team_games_mask(games_df: pd.DataFrame, team_id: int, season: int, week: int):
    """
    Returns a boolean mask for games played by team_id in a given season up to (but not including) a given week.
    """
    return (
        ((games_df['home_team_id'] == team_id) | (games_df['away_team_id'] == team_id)) &
        (games_df['season'] == season) &
        (games_df['week'] < week)
    )

def team_adv_stats_mask(games_adv_stats_df: pd.DataFrame, team_id: int, season: int, week: int):
    """'
    Returns a boolean mask for games played by team_id in a given season up to (but not including) a given week.
    """
    return (
        (games_adv_stats_df['team_id'] == team_id) &
        (games_adv_stats_df['season'] == season) &
        (games_adv_stats_df['week'] < week)
    )


def calculate_games_played(games_df: pd.DataFrame, team_id: int, season: int, week: int) -> int:
    """
    Calculate the number of games played by a team in a given season up to (but not including) a given week.
    """
    mask = team_games_mask(games_df, team_id, season, week)
    return games_df[mask].shape[0]

def calculate_points_per_game(games_df: pd.DataFrame, team_id: int, season: int, week: int):
    """
    Calculates total points scored / games played (up to a given week)
    Returns None if no prior games.
    """
    mask = team_games_mask(games_df, team_id, season, week)

    filtered_games = games_df[mask]
    total_games = filtered_games.shape[0]
    if total_games == 0:
        return None
    
    home_points = filtered_games.loc[filtered_games['home_team_id'] == team_id, 'home_points'].sum()
    away_points = filtered_games.loc[filtered_games['away_team_id'] == team_id, 'away_points'].sum()
    total_points = home_points + away_points

    return (total_points / total_games)

def calculate_recent_points_per_game(games_df: pd.DataFrame, team_id: int, season: int, week: int, N: int = 3):
    """
    Calculates a team's average Points over the last N games.
    Returns None if no prior games
    """
    mask = team_games_mask(games_df, team_id, season, week)

    past_games = games_df[mask].sort_values('week', ascending=False).head(N)
    total_games = past_games.shape[0]
    if total_games == 0:
        return None
    
    home_points = past_games.loc[past_games['home_team_id'] == team_id, 'home_points'].sum()
    away_points = past_games.loc[past_games['away_team_id'] == team_id, 'away_points'].sum()
    total_points = home_points + away_points

    return (total_points / total_games)

def calculate_recent_points_allowed_per_game(games_df: pd.DataFrame, team_id: int, season: int, week: int, N: int = 3):
    """
    Calculates a team's average points allowed over the last N games.
    Returns None if no prior games
    """
    mask = team_games_mask(games_df, team_id, season, week)
    past_games = games_df[mask].sort_values('week', ascending=False).head(N)
    total_games = past_games.shape[0]
    if total_games == 0:
        return None
    
    points_allowed_at_home = past_games.loc[past_games['home_team_id'] == team_id, 'away_points'].sum()
    points_allowed_away = past_games.loc[past_games['away_team_id'] == team_id, 'home_points'].sum()
    total_points = points_allowed_at_home + points_allowed_away

    return (total_points / total_games)


def calculate_points_allowed_per_game(games_df: pd.DataFrame, team_id: int, season: int, week:int):
    """
    Calculates total points allowed / games played (up to a given week)
    Returns None if no prior games
    """
    mask = team_games_mask(games_df, team_id, season, week)

    filtered_games = games_df[mask]
    total_games = filtered_games.shape[0]
    if total_games == 0:
        return None
    home_points_allowed = filtered_games.loc[filtered_games['home_team_id'] == team_id, 'away_points'].sum()
    away_points_allowed = filtered_games.loc[filtered_games['away_team_id'] == team_id, 'home_points'].sum()
    total_points_allowed = home_points_allowed + away_points_allowed

    return (total_points_allowed / total_games)

def calculate_margin_of_victory_per_game(games_df: pd.DataFrame, team_id: int, season: int, week: int):
    """
    Calculates margin of victory or defeat up to a given week
    Returns None if no prior games
    """
    mask = team_games_mask(games_df, team_id, season, week)
    filtered_games = games_df[mask]
    total_games = filtered_games.shape[0]

    if total_games == 0:
        return None
    ppg = calculate_points_per_game(games_df, team_id, season, week)
    papg = calculate_points_allowed_per_game(games_df, team_id, season, week)

    return (ppg - papg)

def calculate_win_rate(games_df: pd.DataFrame, team_id: int, season: int, week: int):
    """
    Calculates win rate for all games played by a team before a given week in a season.
    Return None if no prior games.
    """

    mask = team_games_mask(games_df, team_id, season, week)
    past_games = games_df[mask].sort_values('week', ascending=False)
    if past_games.shape[0] == 0:
        return None
    
    home_wins = past_games.loc[(past_games['home_team_id'] == team_id) & (past_games['home_points'] > past_games['away_points'])].shape[0]
    away_wins = past_games.loc[(past_games['away_team_id'] == team_id) & (past_games['away_points'] > past_games['home_points'])].shape[0]
    total_wins = home_wins + away_wins
    return (total_wins / past_games.shape[0])

def calculate_recent_form(games_df: pd.DataFrame, team_id: int, season: int, week: int, N: int = 3):
    """
    Calculates win rate for the last N games played by a team before a given week in a season.
    Returns None if no prior games.
    """
  
    mask = team_games_mask(games_df, team_id, season, week)

    past_games = games_df[mask].sort_values('week', ascending=False).head(N)
    if past_games.shape[0] == 0:
        return None

    home_wins = past_games.loc[(past_games['home_team_id'] == team_id) & (past_games['home_points'] > past_games['away_points'])].shape[0]
    away_wins = past_games.loc[(past_games['away_team_id'] == team_id) & (past_games['away_points'] > past_games['home_points'])].shape[0]
    total_wins = home_wins + away_wins
    return (total_wins / past_games.shape[0])


def calculate_yards_per_play(games_adv_stats_df: pd.DataFrame, team_id: int, season: int, week: int):
    """
    Returns avg yards per play for all games up to a given week in a season for a given team. 
    Returns None if no prior games.
    """
    
    mask = team_adv_stats_mask(games_adv_stats_df, team_id, season, week)
    filtered_stats = games_adv_stats_df[mask]
    if filtered_stats.shape[0] == 0:
        return None
    
    total_offensive_yards = (
        filtered_stats['offense_line_yards_total'].sum() +
        filtered_stats['offense_second_level_yards_total'].sum() +
        filtered_stats['offense_open_field_yards_total'].sum()
    )

    total_plays = filtered_stats['offense_plays'].sum()
    if total_plays == 0:
        return None

    return (total_offensive_yards / total_plays)

def calculate_yards_allowed_per_play(games_adv_stats_df: pd.DataFrame, team_id: int, season: int, week: int):
    """
    Returns avg yards per play for all games up to a given week in a season for a given team. 
    Returns None if no prior games.
    """

    mask = team_adv_stats_mask(games_adv_stats_df, team_id, season, week)
    filtered_stats = games_adv_stats_df[mask]
    if filtered_stats.shape[0] == 0:
        return None
    
    total_defensive_yards = (
        filtered_stats['defense_line_yards_total'].sum() +
        filtered_stats['defense_second_level_yards_total'].sum() +
        filtered_stats['defense_open_field_yards_total'].sum()
    )

    total_plays = filtered_stats['defense_plays'].sum()
    if total_plays == 0:
        return None
    
    return (total_defensive_yards / total_plays)

def calculate_avg_explosiveness(games_adv_stats_df: pd.DataFrame, team_id: int, season: int, week: int):
    """
    Returns avg explosiveness across all games up to a (but not including) a given week in a season
    Returns None if no previous games have been played
    """
    mask = team_adv_stats_mask(games_adv_stats_df, team_id, season, week)
    filtered_stats = games_adv_stats_df[mask]
    if filtered_stats.shape[0] == 0:
        return None
    avg_explosiveness = filtered_stats['offense_explosiveness'].mean()
    return avg_explosiveness

def calculate_avg_success_rate(games_adv_stats_df: pd.DataFrame, team_id: int, season: int, week: int):
    """
    Returns avg explosiveness across all games up to a (but not including) a given week in a season
    Returns None if no previous games have been played
    """   
    mask = team_adv_stats_mask(games_adv_stats_df, team_id, season, week)
    filtered_stats = games_adv_stats_df[mask]
    if filtered_stats.shape[0] == 0:
        return None
    avg_success_rate = filtered_stats['offense_success_rate'].mean()
    return avg_success_rate

def calculate_travel_distance(teams_df: pd.DataFrame, games_df: pd.DataFrame, game_id: int):
    """
    Returns distance between home and away stadiums for a given game.
    """
    filtered_game = games_df[games_df['game_id'] == game_id]
    if filtered_game.empty:
        return None

    home_id = filtered_game.iloc[0]['home_team_id']
    away_id = filtered_game.iloc[0]['away_team_id']

    home_team = teams_df[teams_df['team_id'] == home_id]
    away_team = teams_df[teams_df['team_id'] == away_id]

    if home_team.empty or away_team.empty:
        return None

    home_coords = (home_team.iloc[0]['lat'], home_team.iloc[0]['lon'])
    away_coords = (away_team.iloc[0]['lat'], away_team.iloc[0]['lon'])

    distance = geodesic(home_coords, away_coords).miles
    return distance

def calculate_rest_days(games_df: pd.DataFrame, team_id: int, season: int, week: int):
    """
    Returns the number of days since the team's previous game before the given week in a season.
    Returns None if no prior games.
    Handles both string and datetime.date types in the 'date' column.
    """
    mask = team_games_mask(games_df, team_id, season, week)
    past_games = games_df[mask].sort_values('week', ascending=False)
    if past_games.shape[0] == 0:
        return None

    # Get the most recent previous game
    last_game_date_val = past_games.iloc[0]['date']
    if not last_game_date_val:
        return None

    # Get current game date
    current_game = games_df[
        ((games_df['home_team_id'] == team_id) | (games_df['away_team_id'] == team_id)) &
        (games_df['season'] == season) &
        (games_df['week'] == week)
    ]
    if current_game.empty or not current_game.iloc[0]['date']:
        return None

    current_game_date_val = current_game.iloc[0]['date']

    # Convert to datetime objects if needed
    if isinstance(last_game_date_val, str):
        last_game_date = datetime.strptime(last_game_date_val, "%Y-%m-%d")
    else:
        last_game_date = last_game_date_val

    if isinstance(current_game_date_val, str):
        current_game_date = datetime.strptime(current_game_date_val, "%Y-%m-%d")
    else:
        current_game_date = current_game_date_val

    rest_days = (current_game_date - last_game_date)
    return rest_days

def calculate_home_win(games_df: pd.DataFrame, game_id: int):
    """
    Returns a boolean indicating whether or not the home team won a given game.
    """
    game = games_df[games_df['game_id'] == game_id]
    if game.empty:
        return None
    return (game.iloc[0]['home_points'] > game.iloc[0]['away_points'])

def check_neutral_site(games_df: pd.DataFrame, game_id: int):
    """
    Returns a boolean indicating whether or not a game was played at a neutral site.
    """
    game = games_df[games_df['game_id'] == game_id]
    if game.empty:
        return None
    return (game.iloc[0]['neutral_site'])

def find_vegas_spread_close(lines_df: pd.DataFrame, game_id: int):
    """
    Returns the closing vegas spread from a given game
    """
    game = lines_df[lines_df['game_id'] == game_id]
    if game.empty:
        return None
    return (game.iloc[0]['spread_close'])

def find_vegas_over_under_close(lines_df: pd.DataFrame, game_id: int):
    """
    Returns the closing vegas over under from a given game
    """
    game = lines_df[lines_df['game_id'] == game_id]
    if game.empty:
        return None
    return (game.iloc[0]['over_under_close'])

def calculate_elo_diff(elo_df: pd.DataFrame, home_team_id: int, away_team_id: int, season: int, week: int):
    """
    Calculates the ELO difference between two teams in a given week and season.
    Returns None if either team is missing.
    """
    home = elo_df[(elo_df['season'] == season) & (elo_df['week'] == week) & (elo_df['team_id'] == home_team_id)]
    away = elo_df[(elo_df['season'] == season) & (elo_df['week'] == week) & (elo_df['team_id'] == away_team_id)]
    if home.empty or away.empty:
        return None
    home_elo = home.iloc[0]['elo']
    away_elo = away.iloc[0]['elo']
    return (home_elo - away_elo)

def calculate_points_per_game_diff(games_df: pd.DataFrame, home_team_id: int, away_team_id: int, season: int, week: int):
    """
    Calculates the PPG difference between two teams in a given week and season.
    Returns None if either team is missing.
    """
    home_points_per_game = calculate_points_per_game(games_df, home_team_id, season, week)
    away_points_per_game = calculate_points_per_game(games_df, away_team_id, season, week)
    if (home_points_per_game is None) or (away_points_per_game is None):
        return None
    return (home_points_per_game - away_points_per_game)

def calculate_points_allowed_per_game_diff(games_df: pd.DataFrame, home_team_id: int, away_team_id: int, season: int, week: int):
    """
    Calculates the Points Allowed Per Game difference between two teams in a given week and season.
    Returns None if either team is missing.
    """
    home_points_allowed_per_game = calculate_points_allowed_per_game(games_df, home_team_id, season, week)
    away_points_allowed_per_game = calculate_points_allowed_per_game(games_df, away_team_id, season, week)
    if (home_points_allowed_per_game is None) or (away_points_allowed_per_game is None):
        return None
    return (home_points_allowed_per_game - away_points_allowed_per_game)

def calculate_recent_points_per_game_diff(games_df: pd.DataFrame, home_team_id: int, away_team_id: int, season: int, week: int, N: int = 3):
    """"
    Calculates the difference in points per game between two teams over the last N games in a given week and season.
    Returns None if data from either team is missing.
    """
    recent_home_points_per_game = calculate_recent_points_per_game(games_df, home_team_id, season, week, N)
    recent_away_points_per_game = calculate_recent_points_per_game(games_df, away_team_id, season, week, N)
    if (recent_home_points_per_game is None) or (recent_away_points_per_game is None):
        return None
    return (recent_home_points_per_game - recent_away_points_per_game)

def calculate_recent_points_allowed_per_game_diff(games_df: pd.DataFrame, home_team_id: int, away_team_id: int, season: int, week: int, N: int = 3):
    """"
    Calculates the difference in points allowed per game between two teams over the last N games in a given week and season.
    Returns None if data from either team is missing.
    """
    recent_home_points_allowed_per_game = calculate_recent_points_allowed_per_game(games_df, home_team_id, season, week, N)
    recent_away_points_allowed_per_game = calculate_recent_points_allowed_per_game(games_df, away_team_id, season, week, N)
    if (recent_home_points_allowed_per_game is None) or (recent_away_points_allowed_per_game is None):
        return None
    return (recent_home_points_allowed_per_game - recent_away_points_allowed_per_game)

def calculate_margin_of_victory_diff(games_df: pd.DataFrame, home_team_id: int, away_team_id: int, season: int, week: int):
    """
    Calculates the difference between two team's margin of victory statistics up to a given week in a given season.
    Return None if data from either team is missing.
    """
    home_margin_of_victory = calculate_margin_of_victory_per_game(games_df, home_team_id, season, week)
    away_margin_of_victory = calculate_margin_of_victory_per_game(games_df, away_team_id, season, week)
    if (home_margin_of_victory is None) or (away_margin_of_victory is None):
        return None
    return (home_margin_of_victory - away_margin_of_victory)

def calculate_win_rate_diff(games_df: pd.DataFrame, home_team_id: int, away_team_id: int, season: int, week: int):
    """"
    Calculates the difference between two team's win rates up to a given week in a given season.
    Returns None if data from either team is missing.
    """
    home_win_rate = calculate_win_rate(games_df, home_team_id, season, week)
    away_win_rate = calculate_win_rate(games_df, away_team_id, season, week)
    if (home_win_rate is None) or (away_win_rate is None):
        return None
    return (home_win_rate - away_win_rate)

def calculate_yards_per_play_diff(games_adv_stats_df: pd.DataFrame, home_team_id: int, away_team_id: int, season: int, week: int):
    """"
    Calculates the difference between two teams yards per play averages up to a given week in a given season.
    Returns None if data from either team is missing.
    """
    home_yards_per_play = calculate_yards_per_play(games_adv_stats_df, home_team_id, season, week)
    away_yards_per_plays = calculate_yards_per_play(games_adv_stats_df, away_team_id, season, week)
    if (home_yards_per_play is None) or (away_yards_per_plays is None):
        return None
    return (home_yards_per_play - away_yards_per_plays)

def calculate_yards_allowed_per_play_diff(games_adv_stats_df: pd.DataFrame, home_team_id: int, away_team_id: int, season: int, week: int):
    """'
    Calculates the difference between two teams yards per play allowed averages up to a given week in a given season.
    Returns None if data from either team is missing.
    """
    home_yards_allowed_per_play = calculate_yards_allowed_per_play(games_adv_stats_df, home_team_id, season, week)
    away_yards_allowed_per_play = calculate_yards_allowed_per_play(games_adv_stats_df, away_team_id, season, week)
    if (home_yards_allowed_per_play is None) or (away_yards_allowed_per_play is None):
        return None
    return (home_yards_allowed_per_play - away_yards_allowed_per_play)

def calculate_explosiveness_diff(games_adv_stats_df: pd.DataFrame, home_team_id: int, away_team_id: int, season: int, week: int):
    """"
    Calculates the difference between two teams average explosiveness statistic up to a given week in a given season.
    Returns None if data from either team is missing.
    """
    home_explosiveness = calculate_avg_explosiveness(games_adv_stats_df, home_team_id, season, week)
    away_explosiveness = calculate_avg_explosiveness(games_adv_stats_df, away_team_id, season, week)
    if (home_explosiveness is None) or (away_explosiveness is None):
        return None
    return (home_explosiveness - away_explosiveness)

def calculate_success_rate_diff(games_adv_stats_df: pd.DataFrame, home_team_id: int, away_team_id: int, season: int, week: int):
    """"
    Calculates the difference between two teams averages success rate statistics up to a given week in a given season.
    Returns None if data from either team is missing.
    """
    home_success_rate = calculate_avg_success_rate(games_adv_stats_df, home_team_id, season, week)
    away_success_rate = calculate_avg_success_rate(games_adv_stats_df, away_team_id, season, week)
    if (home_success_rate is None) or (away_success_rate is None):
        return None
    return (home_success_rate - away_success_rate)

def calculate_rest_days_diff(games_df: pd.DataFrame, home_team_id: int, away_team_id: int, season: int, week: int):
    """
    Calculates the difference between two teams rest days on the date they matchup
    Returns None if data from either team is missing.
    """
    home_rest = calculate_rest_days(games_df, home_team_id, season, week)
    away_rest = calculate_rest_days(games_df, away_team_id, season, week)
    if (home_rest is None) or (away_rest is None):
        return None
    return (home_rest - away_rest)

def calculate_recent_form_diff(games_df: pd.DataFrame, home_team_id: int, away_team_id: int, season: int, week: int, N: int = 3):
    """"
    Calculates the difference in recent form between two teams over the last N games.
    Returns None if data from either team is missing.
    """
    home_recent_form = calculate_recent_form(games_df, home_team_id, season, week, N)
    away_recent_form = calculate_recent_form(games_df, away_team_id, season, week, N)
    if (home_recent_form is None) or (away_recent_form is None):
        return None
    return (home_recent_form - away_recent_form)

def calculate_matchup_features(games_df: pd.DataFrame, games_adv_stats_df: pd.DataFrame, teams_df: pd.DataFrame, elo_df: pd.DataFrame, lines_df: pd.DataFrame):
    """
    Takes games_df, games_adv_stats_df, teams_df, elo_df, and lines_df and returns a DataFrame ready to load into the matchup_features table
    """
    records = []

    for _, game in games_df.iterrows():
        try:
            game_id = game["game_id"]
            season = game["season"]
            week = game["week"]
            home_team = game["home_team_id"]
            away_team = game["away_team_id"]

            elo_diff = calculate_elo_diff(elo_df, home_team, away_team, season, week)
            points_per_game_diff = calculate_points_per_game_diff(games_df, home_team, away_team, season, week)
            points_allowed_per_game_diff = calculate_points_allowed_per_game_diff(games_df, home_team, away_team, season, week)
            recent_points_per_game_diff = calculate_recent_points_per_game_diff(games_df, home_team, away_team, season, week, 3)
            recent_points_allowed_per_game_diff = calculate_recent_points_allowed_per_game_diff(games_df, home_team, away_team, season, week, 3)
            margin_of_victory_diff = calculate_margin_of_victory_diff(games_df, home_team, away_team, season, week)
            win_rate_diff = calculate_win_rate_diff(games_df, home_team, away_team, season, week)
            yards_per_play_diff = calculate_yards_per_play_diff(games_adv_stats_df, home_team, away_team, season, week)
            yards_allowed_per_play_diff = calculate_yards_allowed_per_play_diff(games_adv_stats_df, home_team, away_team, season, week)
            explosiveness_diff = calculate_explosiveness_diff(games_adv_stats_df, home_team, away_team, season, week)
            success_rate_diff = calculate_success_rate_diff(games_adv_stats_df, home_team, away_team, season, week)
            travel_distance = calculate_travel_distance(teams_df, games_df, game_id)
            rest_days_diff = calculate_rest_days_diff(games_df, home_team, away_team, season, week)
            recent_form_diff = calculate_recent_form_diff(games_df, home_team, away_team, season, week)
            neutral_site = check_neutral_site(games_df, game_id)
            vegas_spread_close = find_vegas_spread_close(lines_df, game_id)
            vegas_over_under_close = find_vegas_over_under_close(lines_df, game_id)
            home_win = calculate_home_win(games_df, game_id)

            records.append({
                "game_id": game_id,
                "season": season,
                "week": week,
                "elo_diff": elo_diff,
                "points_per_game_diff": points_per_game_diff,
                "points_allowed_per_game_diff": points_allowed_per_game_diff,
                "recent_points_per_game_diff": recent_points_per_game_diff,
                "recent_points_allowed_per_game_diff": recent_points_allowed_per_game_diff,
                "margin_of_victory_diff": margin_of_victory_diff,
                "win_rate_diff": win_rate_diff,
                "yards_per_play_diff": yards_per_play_diff,
                "yards_allowed_per_play_diff": yards_allowed_per_play_diff,
                "explosiveness_diff": explosiveness_diff,
                "success_rate_diff": success_rate_diff,
                "travel_distance": travel_distance,
                "rest_days_diff": rest_days_diff,
                "recent_form_diff": recent_form_diff,
                "neutral_site": neutral_site,
                "vegas_spread_close": vegas_spread_close,
                "vegas_over_under_close": vegas_over_under_close,
                "home_win": home_win
            })
        except Exception as e:
            print(f"Error processing game_id {game.get('game_id', 'N/A')}: {e}")
            continue
    return pd.DataFrame(records)





# ----------------------------
# Helper function to load ELO
# ----------------------------

def calculate_all_teams_elo(games_df: pd.DataFrame, team_ids: list, initial_elo: float = 1500, k: float = 20) -> pd.DataFrame:
    """
    Returns a DataFrame with columns: team_id, season, week, elo
    """
    records = []
    for team_id in team_ids:
        team_games = games_df[(games_df['home_team_id'] == team_id) | (games_df['away_team_id'] == team_id)]
        team_games = team_games.sort_values(['season', 'week'])
        current_elo = initial_elo
        team_elos = {team_id: initial_elo}
        for _, game in team_games.iterrows():
            season = game['season']
            week = game['week']
            home_id = game['home_team_id']
            away_id = game['away_team_id']
            home_points = game['home_points']
            away_points = game['away_points']
            # Determine if this team is home or away
            if team_id == home_id:
                opponent_id = away_id
                team_score = home_points
                opp_score = away_points
            else:
                opponent_id = home_id
                team_score = away_points
                opp_score = home_points
            opp_elo = team_elos.get(opponent_id, initial_elo)
            expected = 1 / (1 + 10 ** ((opp_elo - current_elo) / 400))
            actual = 1 if team_score > opp_score else 0.5 if team_score == opp_score else 0
            new_elo = current_elo + k * (actual - expected)
            team_elos[team_id] = new_elo
            team_elos[opponent_id] = opp_elo + k * ((1 - actual) - (1 - expected))
            records.append({
                "team_id": team_id,
                "season": season,
                "week": week,
                "elo": new_elo
            })
            current_elo = new_elo
    elo_df = pd.DataFrame(records)
    elo_df = elo_df.sort_values(['team_id', 'season', 'week', 'elo'])
    elo_df = elo_df.drop_duplicates(subset=['team_id', 'season', 'week'], keep='last')


    return elo_df

    



