This project helps users make informed picks for a college football pick'em league, where participants predict the outcomes of 10 games each week and assign a confidence ranking (1-10) to each prediction.

## Features
- Scrapes data from multiple sources:
  - [CollegeFootballData.com](https://collegefootballdata.com/)
  - [NCAA Stadiums Kaggle Dataset](https://www.kaggle.com/datasets/mexwell/ncaa-stadiums)
  - [Covers.com](https://www.covers.com/) (for injury reports)
- Uses multiple machine learning models:
  - Elastic Net Regression
  - Gradient Boosting
  - Logistic Regression
  - Random Forest (**best performer and final model used**)
- Compares model predictions to Vegas odds to identify potential edges.
- Outputs weekly pick recommendations with ranked confidence.

## How It Works
1. Aggregates relevant data for upcoming college football games.
2. Scrapes injury information from Covers.com.
3. Applies machine learning models to predict game outcomes and confidence levels.
4. Compares model predictions to Vegas odds to identify potential edges.
5. Outputs weekly pick recommendations with ranked confidence.

## Data Sources (Works Cited)
- [CollegeFootballData.com](https://collegefootballdata.com/)
- [NCAA Stadiums Kaggle Dataset](https://www.kaggle.com/datasets/mexwell/ncaa-stadiums)
- [Covers.com](https://www.covers.com/) (injury reports)

## Getting Started
1. Clone this repository.
2. Install dependencies (see requirements.txt or environment setup instructions).
3. Set up your `.env` file with necessary API keys and configuration.
4. Run the main script to generate weekly picks.

## Notes
- This project is for educational and entertainment purposes only.
---