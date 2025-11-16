# extractors/ergast_extractor.py
# Ergast is a reliable programmatic endpoint for race results and driver info

import requests
import time

ERGAST_BASE = "https://ergast.com/api/f1"

def fetch_race_results(season: int, round: int = None):
    # example: http://ergast.com/api/f1/2023/1/results.json
    path = f"/{season}"
    if round:
        path += f"/{round}"
    path += "/results.json"
    url = ERGAST_BASE + path
    r = requests.get(url, headers={"Accept":"application/json"})
    r.raise_for_status()
    return r.json()  # raw payload - store as-is
