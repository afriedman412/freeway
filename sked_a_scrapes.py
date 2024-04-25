import requests
from time import sleep
import os
import pandas as pd
from collections import Counter
from dotenv import load_dotenv
from user_agents import user_agents
import random

load_dotenv()


def verify_r(r: requests.Response):
    assert r.status_code == 200, f"bad status code: {r.status_code}"
    assert r.json(), "bad response json"
    assert r.json()['results'], "no results loaded"
    return len(r.json()['results'])


# for PAC in [
#     "Senate Leadership Fund",
#     "Congressional Leadership Fund",
#     "Club for Growth",
#     "Americans for Prosperity"
# ]:

cycle = 2020
offset = 0
filings = []
output_file_name = "sked_a_{}_{}.csv"

PAC = "Club For Growth"
print(PAC)

url = "https://api.propublica.org/campaign-finance/v1/{}/filings/search.json"

while True:
    r = requests.get(
        url=url.format(cycle),
        params={'offset': offset, 'query': PAC},
        headers={'x-api-key': os.environ["PRO_PUBLICA_API_KEY"]},
    )
    try:
        print(verify_r(r), offset, r.json()['results'][0]['filing_id'])
        filings += r.json()['results']
        offset += 20
        sleep(1)
    except Exception:
        break
count = Counter([f['form_type'] for f in filings])
print(count)
for n, f in enumerate([f for f in filings if f['form_type'] == 'F3']):
    url = os.path.join(f['fec_uri'], "sa", "ALL")
    print(n, "/", count['F3'], url)
    r = requests.get(
        url=url,
        headers={
            'User-Agent':
            random.choice(user_agents)
        }
    )
    try:
        df = pd.read_html(r.content)[0]
        for k in [
            'filing_id',
            'fec_committee_id',
            'committee_name'
        ]:
            df[k] = f[k]
        df.to_csv(output_file_name.format(PAC, cycle), mode="a+")
    except ValueError:
        print('no tables found...')
    sleep(1)

