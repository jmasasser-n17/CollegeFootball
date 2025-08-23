# test_historical_loader.py
import pytest
from unittest.mock import patch, MagicMock
import pandas as pd

@patch("historical_loader.fetch_teams_api")
@patch("historical_loader.fetch_games_api")
@patch("historical_loader.fetch_vegas_lines_api")
@patch("historical_loader.fetch_team_stats_api")
@patch("historical_loader.populate_historical_weather_safe")
@patch("historical_loader.create_engine")
def test_historical_loader_main(
    mock_engine, mock_weather, mock_stats, mock_lines, mock_games, mock_teams
):
    import historical_loader
    # Setup mocks
    mock_teams.return_value = pd.DataFrame([
        {"team_id": 1, "team_name": "A", "lat": 0, "lon": 0}
    ])
    mock_games.return_value = pd.DataFrame([
        {"game_id": 1, "home_team_id": 1, "date": "2025-08-22", "home_lat": 0, "home_lon": 0}
    ])
    mock_lines.return_value = pd.DataFrame([{"game_id": 1}])
    mock_stats.return_value = pd.DataFrame([{"team_id": 1}])
    mock_weather.return_value = pd.DataFrame([{"game_id": 1}])
    mock_engine.return_value = MagicMock()

    # Call main() with all dependencies mocked
    historical_loader.main()
