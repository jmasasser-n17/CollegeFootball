# College Football Pick'em Picker

This project is designed to help users make informed picks for a college football pick'em league. In this league, participants predict the outcomes of 10 games each week and assign a confidence ranking (1-10) to each prediction.

## Features
- Scrapes data from multiple sources:
  - [CollegeFootballData.com](https://collegefootballdata.com/)
  - [NOAA NCEI](https://www.ncei.noaa.gov/)
  - [WeatherAPI](https://www.weatherapi.com/)
  - [NCAA Stadiums Kaggle Dataset](https://www.kaggle.com/datasets/mexwell/ncaa-stadiums)
- Uses Elastic Net Regression and Gradient Boosting models to detect model edge versus Vegas odds each week.
- Designed for flexibility—data sources and models can be updated as needed.

## How It Works
1. Scrapes and aggregates relevant data for upcoming college football games.
2. Applies machine learning models to predict game outcomes and confidence levels.
3. Compares model predictions to Vegas odds to identify potential edges.
4. Outputs weekly pick recommendations with ranked confidence.

## Data Sources (Works Cited)
- [CollegeFootballData.com](https://collegefootballdata.com/)
- [NOAA NCEI](https://www.ncei.noaa.gov/)
- [WeatherAPI](https://www.weatherapi.com/)
- [NCAA Stadiums Kaggle Dataset](https://www.kaggle.com/datasets/mexwell/ncaa-stadiums)

## Getting Started
1. Clone this repository.
2. Install dependencies (see requirements.txt or environment setup instructions).
3. Set up your `.env` file with necessary API keys and configuration.
4. Run the main script to generate weekly picks.

## Notes
- This project is for educational and entertainment purposes only.
- Data sources and models may change over time; update the works cited section as needed.

---
