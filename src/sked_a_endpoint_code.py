"""
Raw code to get all committee receipts for a cycle for a given committee. Adapt this into a reqular query to track donations to target committees.
"""
import requests
import os 
from time import sleep
import yaml
from .src import make_conn
import pandas as pd

def committee_sked_a_scrape(committee_id):
    url = "https://api.open.fec.gov/v1/schedules/schedule_a"
    params = {
            'committee_id': committee_id,
            'per_page': 100,
            'contributor_type': 'committee',
            "api_key": os.environ["GOV_API_KEY"]
            }
    results = []
    while True:
        r = requests.get(
            url,
            params=params
            )
        if r.status_code == 200:
            if (
                    'last_indexes' in r.json() and not r.json()['last_indexes']
                ) or (
                    'results' in r.json() and not r.json()['results']
                ):
                break
            else:
                print(r.json()['pagination']['last_indexes']['last_index'])
                for k in ['last_index', 'last_contribution_receipt_date']:
                    params[k] = r.json()['pagination']['last_indexes'][k]
                    results.append(r.json()['results'])
        else:
            raise Exception("Bad Status Code", r.status_code, r.content)
        sleep(1)
    return results


def flatten_dicto(dicto, prefix):
    new_dicto = {}
    for k,v in dicto.items():
        k = '_'.join([prefix, k])
        if isinstance(v, list):
            new_dicto[k] = ",".join([str(v_) for v_ in v])
        else:
            new_dicto[k] = v
    return new_dicto

def format_results(results, update_dict: dict=None):
    new_results = []
    for j in results:
        for r in j:
            new_r = {}
            for k, v in r.items():
                if isinstance(v, dict):
                    new_r.update(flatten_dicto(v, k))
                else:
                    new_r[k] = v
            if update_dict:
                new_r.update(update_dict)
            new_results.append(new_r)
    return new_results


def full_scrape(committee_id, update_dict: dict=None):
    results = committee_sked_a_scrape(committee_id)
    if not results:
        print("no results returned!")
        return []
    new_results = format_results(results, update_dict)
    print(len(new_results))
    return new_results


def scrape_all_committees(committee_data):
    output = []
    for c in committee_data['Committees']:
        print(c)
        results = full_scrape(c['id'], {
            'query_committee_id': c['id'],
            'query_committee_name': c['name']
        })
        if results:
            output += results
        continue
    return output


def load_sked_a():
    """
    Data pull from data.yaml should probably be broken out elsehwere!
    """
    with open("src/data.yaml") as f:
        data = yaml.safe_load(f)

    # formatting here is annoying too, can prob do better
    q = f"""
        SELECT * FROM fec_sked_a
        WHERE contributor_id IN (
        {",".join([f"'{d['id']}'" for d in data['Donors']])}
        )"""
    
    conn = make_conn()
    donor_data = pd.read_sql(q, conn)
    return donor_data