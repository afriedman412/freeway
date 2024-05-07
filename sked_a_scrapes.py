import requests
from time import sleep
import os
import pandas as pd
from collections import Counter
from dotenv import load_dotenv
from user_agents import user_agents
from src.utilities import load_data
import random

load_dotenv()
DATA = load_data()

def verify_r(r: requests.Response):
    assert r.status_code == 200, f"bad status code: {r.status_code}"
    assert r.json(), "bad response json"
    assert r.json()['results'], "no results loaded"
    return len(r.json()['results'])


cycle = 2022
offset = 0
uri_file_name = "sked_a_uris_{}_{}.txt"
output_file_name = "sked_a_{}_{}.csv"
url = "https://api.propublica.org/campaign-finance/v1/{}/filings/search.json"

# for PAC in DATA['Committees']:
#     print(PAC['name'], "getting uris")
#     filings = []
#     while True:
#         r = requests.get(
#             url=url.format(cycle),
#             params={'offset': offset, 'query': PAC},
#             headers={'x-api-key': os.environ["PRO_PUBLICA_API_KEY"]},
#         )
#         try:
#             print(verify_r(r), offset, r.json()['results'][0]['filing_id'])
#             filings += r.json()['results']
#             offset += 20
#             sleep(1)
#         except Exception:
#             break
#     count = Counter([f['form_type'] for f in filings])
#     print(count)
#     for filing in [f for f in filings if f['form_type'] == 'F3']:
#         with open(uri_file_name.format(PAC['name'], cycle), mode='a+') as file:
#             file.write(filing['fec_uri'] + "\n")

# for PAC in DATA['Committees']:
#     print(PAC['name'], 'getting sked as')
#     filings = open(uri_file_name.format(PAC['name'], cycle)).readlines()
filings = open('all_sked_as_2022.txt').readlines()
for n, f in enumerate(filings):
    url = os.path.join(f.strip(), "sa", "ALL")
    print(n, "/", len(filings), url)
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
        df.to_csv(output_file_name.format('all_of_em', cycle), mode="a+")
    except ValueError:
        print('no tables found...')
    sleep(1)

