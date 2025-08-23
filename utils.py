# utils.py

from datetime import datetime
import pandas as pd
import requests
from geopy.geocoders import Nominatim
import time
import os
import json
from geopy.distance import geodesic


CFBD_BASE_URL = "https://api.collegefootballdata.com"
NOAA_BASE_URL = "https://api.weather.gov"
STATION_CACHE_FILE = "station_cache.json"

TEAMS_ENDPOINT = f"{CFBD_BASE_URL}/teams/fbs"
GAMES_ENDPOINT = f"{CFBD_BASE_URL}/games"
TEAM_STATS_ENDPOINT = f"{CFBD_BASE_URL}/team/stats"
VEGAS_LINES_ENDPOINT = f"{CFBD_BASE_URL}/lines"
SCHEDULE_ENDPOINT = f"{CFBD_BASE_URL}/schedule"



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
# Geocoding
# ------------------------------
GEOCODE_CACHE_FILE = "geocode_cache.json"
geolocator = Nominatim(user_agent="cfb_team_geocoder")

def load_geocode_cache():
    if os.path.exists(GEOCODE_CACHE_FILE):
        with open(GEOCODE_CACHE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_geocode_cache(cache: dict):
    with open(GEOCODE_CACHE_FILE, "w") as f:
        json.dump(cache, f)

def geocode_city_state(city: str, state: str, cache: dict, delay: float = 1.0):
    """
    Convert city + state to latitude/longitude using Nominatim.
    Uses a local cache to avoid repeated requests.
    """
    key = f"{city},{state}"
    if key in cache:
        return cache[key]

    try:
        location = geolocator.geocode(f"{city}, {state}, USA")
        if location:
            lat_lon = (location.latitude, location.longitude)
        else:
            lat_lon = (None, None)
    except Exception as e:
        print(f"Geocoding error for {city}, {state}: {e}")
        lat_lon = (None, None)
    finally:
        time.sleep(delay)

    cache[key] = lat_lon
    return lat_lon

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
    
    geocode_cache = load_geocode_cache()
    records = []

    for t in teams_json:
        team_id = t.get("id")
        team_name = t.get("school")
        conference = t.get("conference")
        stadium = t.get("stadium", {}).get("name", None)
        city = t.get("location", {}).get("city", None)
        state = t.get("location", {}).get("state", None)

        lat, lon = (None, None)
        if city and state:
            lat, lon = geocode_city_state(city, state, geocode_cache)

        records.append({
            "team_id": team_id,
            "team_name": team_name,
            "stadium_name": stadium,
            "lat": lat,
            "lon": lon,
            "conference": conference
        })

    save_geocode_cache(geocode_cache)
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
            line = g.get("lines", [{}])[0]

            records.append({
                "game_id": g.get("id"),
                "spread_close": line.get("spread"),
                "over_under_close": line.get("overUnder"),
                "moneyline_home": line.get("homeMoneyline"),
                "moneyline_away": line.get("awayMoneyline")
            })

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
        response.raise_for_status()
        stats_json = response.json()

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

def calculate_elo(games_df: pd.DataFrame, teams_df: pd.DataFrame,
                  initial_elo=1500, k_factor=20, home_field_adv=50) -> pd.DataFrame:
    """
    Calculate ELO ratings for all teams based on historical games.

    Args:
        games_df: DataFrame with columns: game_id, season, week, home_team_id, away_team_id, home_points, away_points
        teams_df: DataFrame with all team_ids
        initial_elo: starting ELO for all teams
        k_factor: ELO K-factor (higher = faster adjustment)
        home_field_adv: home field advantage in ELO points

    Returns:
        DataFrame with columns: team_id, season, week, elo
    """
    # Initialize ELOs
    elo_dict = {team_id: initial_elo for team_id in teams_df['team_id']}
    records = []

    # Sort games chronologically (season -> week -> game_id)
    games_df = games_df.sort_values(['season', 'week', 'game_id'])

    for _, game in games_df.iterrows():
        home_id = game['home_team_id']
        away_id = game['away_team_id']
        home_elo = elo_dict[home_id]
        away_elo = elo_dict[away_id]

        # Expected probability for home team
        expected_home = 1 / (1 + 10 ** ((away_elo - (home_elo + home_field_adv)) / 400))

        # Actual result
        if game['home_points'] > game['away_points']:
            actual_home = 1
        elif game['home_points'] < game['away_points']:
            actual_home = 0
        else:
            actual_home = 0.5  # tie, rare in college football

        # ELO update
        delta = k_factor * (actual_home - expected_home)
        elo_dict[home_id] += delta
        elo_dict[away_id] -= delta  # zero-sum

        # Store pre-game ELOs if desired
        records.append({
            "team_id": home_id,
            "season": game['season'],
            "week": game['week'],
            "elo": home_elo
        })
        records.append({
            "team_id": away_id,
            "season": game['season'],
            "week": game['week'],
            "elo": away_elo
        })

    return pd.DataFrame(records)


def load_station_cache():
    if os.path.exists(STATION_CACHE_FILE):
        with open(STATION_CACHE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_station_cache(cache: dict):
    with open(STATION_CACHE_FILE, "w") as f:
        json.dump(cache, f)

def get_nearest_station(lat: float, lon: float, stations: list, cache: dict):
    """
    Returns the stationId of the nearest station to the given lat/lon.
    Caches results to avoid recalculation.
    """
    key = f"{lat},{lon}"
    if key in cache:
        return cache[key]

    nearest_station = None
    min_dist = float("inf")
    for s in stations:
        s_lat = s['geometry']['coordinates'][1]
        s_lon = s['geometry']['coordinates'][0]
        dist = geodesic((lat, lon), (s_lat, s_lon)).miles
        if dist < min_dist:
            min_dist = dist
            nearest_station = s['id']

    cache[key] = nearest_station
    return nearest_station

def fetch_weather_for_game(station_id: str, game_date: str):
    """
    Fetches hourly observations from NOAA for the given station and game date.
    Aggregates to daily averages / sums.
    """
    start = f"{game_date}T00:00:00Z"
    end = f"{game_date}T23:59:59Z"
    url = f"{NOAA_BASE_URL}/stations/{station_id}/observations?start={start}&end={end}"

    response = requests.get(url)
    response.raise_for_status()
    observations = response.json().get("features", [])

    if not observations:
        return {
            "temperature": None,
            "wind_speed": None,
            "precipitation": None,
            "humidity": None,
            "conditions": None
        }

    temps = []
    winds = []
    precips = []
    hums = []
    conditions = []

    for obs in observations:
        props = obs['properties']
        if props.get('temperature') and props['temperature']['value'] is not None:
            temps.append(props['temperature']['value'])
        if props.get('windSpeed') and props['windSpeed']['value'] is not None:
            winds.append(props['windSpeed']['value'])
        if props.get('precipitationLastHour') and props['precipitationLastHour']['value'] is not None:
            precips.append(props['precipitationLastHour']['value'])
        if props.get('relativeHumidity') and props['relativeHumidity']['value'] is not None:
            hums.append(props['relativeHumidity']['value'])
        if props.get('textDescription'):
            conditions.append(props['textDescription'])

    return {
        "temperature": float(pd.Series(temps).mean()) if temps else None,
        "wind_speed": float(pd.Series(winds).mean()) if winds else None,
        "precipitation": float(pd.Series(precips).sum()) if precips else None,
        "humidity": float(pd.Series(hums).mean()) if hums else None,
        "conditions": ", ".join(set(conditions)) if conditions else None
    }

def populate_historical_weather(games_df: pd.DataFrame):
    """
    For each game in games_df (must have lat/lon of home team/stadium),
    fetch weather and return DataFrame matching `weather` table.
    """
    # 1. Load all NOAA stations
    stations_resp = requests.get(f"{NOAA_BASE_URL}/stations")
    stations_resp.raise_for_status()
    stations = stations_resp.json()['features']

    station_cache = load_station_cache()
    records = []

    for _, game in games_df.iterrows():
        lat = game['home_lat']  # make sure games_df includes lat/lon
        lon = game['home_lon']
        game_date = game['date']

        if not lat or not lon or not game_date:
            print(f"Skipping game {game['game_id']}: missing lat/lon/date")
            continue

        station_id = get_nearest_station(lat, lon, stations, station_cache)
        weather = fetch_weather_for_game(station_id, game_date)
        records.append({
            "game_id": game['game_id'],
            **weather
        })

        # avoid hitting NOAA rate limits
        time.sleep(1)

    save_station_cache(station_cache)
    return pd.DataFrame(records)

def populate_historical_weather_safe(games_df: pd.DataFrame, max_retries=3, delay=1.5, log_file="weather_progress.json"):
    """
    Fetch historical weather for all games with retry, progress logging, and rate limiting.
    
    Args:
        games_df: must include 'home_lat', 'home_lon', 'date', 'game_id'
        max_retries: number of retries on request failure
        delay: delay between requests (seconds)
        log_file: JSON file to save completed game_ids

    Returns:
        DataFrame of weather records
    """
    # Load NOAA stations
    stations_resp = requests.get(f"{NOAA_BASE_URL}/stations")
    stations_resp.raise_for_status()
    stations = stations_resp.json()['features']

    # Load caches
    station_cache = load_station_cache()
    completed = set()
    if os.path.exists(log_file):
        with open(log_file, "r") as f:
            completed = set(json.load(f))

    records = []

    for idx, game in games_df.iterrows():
        game_id = game['game_id']
        if game_id in completed:
            continue  # skip already processed

        lat = game['home_lat']
        lon = game['home_lon']
        game_date = game['date']

        if not lat or not lon or not game_date:
            print(f"Skipping game {game_id}: missing lat/lon/date")
            continue

        station_id = get_nearest_station(lat, lon, stations, station_cache)

        for attempt in range(max_retries):
            try:
                weather = fetch_weather_for_game(station_id, game_date)
                break
            except Exception as e:
                print(f"Error fetching weather for game {game_id} (attempt {attempt+1}): {e}")
                time.sleep(delay)
        else:
            print(f"Failed to fetch weather for game {game_id} after {max_retries} retries")
            weather = {"temperature": None, "wind_speed": None, "precipitation": None,
                       "humidity": None, "conditions": None}

        records.append({"game_id": game_id, **weather})
        completed.add(game_id)

        # Save progress every 10 games
        if idx % 10 == 0:
            with open(log_file, "w") as f:
                json.dump(list(completed), f)
            save_station_cache(station_cache)

        time.sleep(delay)  # respect NOAA rate limits

    # Final save
    with open(log_file, "w") as f:
        json.dump(list(completed), f)
    save_station_cache(station_cache)

    return pd.DataFrame(records)


    