import requests
from dynaconf import Dynaconf

# -------------------------------
# Config
# -------------------------------


settings = Dynaconf(
    envvar_prefix="MYAPP",
    load_dotenv=True
)
API_KEY = settings.CFDB_API_KEY

headers = {"Authorization": f"Bearer {API_KEY}"}
url = "https://api.collegefootballdata.com/stats/season?year=2023&team=Alabama"
response = requests.get(url, headers=headers)
print(response.json())