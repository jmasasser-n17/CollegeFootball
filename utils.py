# utils.py

from datetime import datetime
import pandas as pd
import requests
import time
import os
import json
from typing import Optional


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
        lon = t.get("longitude", {}).get("longitude", None)

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


def calculate_games_played(games_df: pd.DataFrame, team_id: int, season: int, week: int) -> int:
    """
    Calculate the number of games played by a team in a given season up to (but not including) a given week.
    """
    mask = team_games_mask(games_df, team_id, season, week)
    return games_df[mask].shape[0]

def calculate_points_per_game(games_df: pd.DataFrame, team_id: int, season: int, week: int):
    """
    Calculates total points scored / games played (up to a given week)
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


def calculate_yards_per_play(games_df: pd.DataFrame, team_id: int, season: int, week: int):
    """
    Returns total yards 
    """
    pass




    



