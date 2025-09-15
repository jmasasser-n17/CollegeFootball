# test_utils.py

import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import utils

def test_parse_api_date_valid():
	assert utils.parse_api_date("2025-08-22T01:44:44.173Z") == "2025-08-22"

def test_parse_api_date_invalid():
	assert utils.parse_api_date("") is None
	assert utils.parse_api_date(None) is None

@patch("utils.geolocator.geocode")
def test_geocode_city_state_cache(mock_geocode):
	cache = {}
	mock_geocode.return_value = MagicMock(latitude=40.0, longitude=-75.0)
	latlon = utils.geocode_city_state("Philadelphia", "PA", cache)
	assert latlon == (40.0, -75.0)
	# Should use cache on second call
	latlon2 = utils.geocode_city_state("Philadelphia", "PA", cache)
	assert latlon2 == (40.0, -75.0)
	assert mock_geocode.call_count == 1

@patch("utils.requests.get")
def test_fetch_teams_api(mock_get):
	mock_get.return_value.json.return_value = [
		{
			"id": 1,
			"school": "Test Team",
			"conference": "Test Conf",
			"stadium": {"name": "Test Stadium"},
			"location": {"city": "Test City", "state": "TS"}
		}
	]
	mock_get.return_value.raise_for_status = lambda: None
	with patch("utils.geocode_city_state", return_value=(10.0, 20.0)):
		df = utils.fetch_teams_api("fake_api_key")
		assert "team_id" in df.columns
		assert df.iloc[0]["team_name"] == "Test Team"

@patch("utils.requests.get")
def test_fetch_games_api(mock_get):
	mock_get.return_value.json.return_value = [
		{
			"id": 1,
			"week": 1,
			"homeId": 1,
			"awayId": 2,
			"homePoints": 21,
			"awayPoints": 14,
			"venue": "Test Venue",
			"startDate": "2025-08-22T01:44:44.173Z",
			"neutralSite": False
		}
	]
	mock_get.return_value.raise_for_status = lambda: None
	df = utils.fetch_games_api("fake_api_key", [2025])
	assert "game_id" in df.columns
	assert df.iloc[0]["home_points"] == 21

@patch("utils.requests.get")
def test_fetch_vegas_lines_api(mock_get):
	mock_get.return_value.json.return_value = [
		{
			"id": 1,
			"lines": [{"spread": -7.5, "overUnder": 55.5, "homeMoneyline": -300, "awayMoneyline": 250}]
		}
	]
	mock_get.return_value.raise_for_status = lambda: None
	df = utils.fetch_vegas_lines_api("fake_api_key", [2025])
	assert "spread_close" in df.columns
	assert df.iloc[0]["spread_close"] == -7.5

def test_calculate_elo():
	games_df = pd.DataFrame([
		{"game_id": 1, "season": 2025, "week": 1, "home_team_id": 1, "away_team_id": 2, "home_points": 21, "away_points": 14},
		{"game_id": 2, "season": 2025, "week": 2, "home_team_id": 2, "away_team_id": 1, "home_points": 10, "away_points": 17}
	])
	teams_df = pd.DataFrame([{"team_id": 1}, {"team_id": 2}])
	elo_df = utils.calculate_elo(games_df, teams_df)
	assert set(["team_id", "season", "week", "elo"]).issubset(elo_df.columns)
