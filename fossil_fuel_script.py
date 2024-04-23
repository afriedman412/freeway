import requests
from src.config import GOV_BASE_URL
import os
import pandas as pd
from time import sleep
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("GOV_API_KEY")
assert api_key

companies = open('filtered_ff_companies.txt').readlines()

offset = 0

suffix = "schedules/schedule_a"
url = os.path.join(GOV_BASE_URL, suffix)
for n, c in enumerate(companies[offset:], start=offset):
    c = c.strip()
    sleep(3) if not n % 10 else sleep(1)
    print(n, c)
    params = {
        "api_key": api_key,
        "two_year_transaction_period": 2022,
        "per_page": 100,
        "contributor_name": c,
        "contributor_type": "committee"
    }

    r = requests.get(
        url,
        params=params
    )

    if r.status_code == 200:
        try:
            results = r.json().get('results')
            if results:
                donors = list(set([(c, r_['contributor_name'], r_['contributor_id']) for r_ in results]))
                print(len(donors))
                with open("fossil_fuel_ids.csv", "a+") as f:
                    for d in donors:
                        f.write(str(d) + "\n")
            else:
                print('no results')

        except requests.exceptions.JSONDecodeError:
            continue
    else:
        raise Exception(f"Bad status code: {r.status_code}, {r.content}")
