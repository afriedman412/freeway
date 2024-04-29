"""
Raw code to get all committee receipts for a cycle for a given committee. Adapt this into a reqular query to track donations to target committees.
"""
import requests
import os 
from time import sleep
from .src import make_conn, DATA
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


def get_pac_contributions_to_candidates():
    q = f"""
    SELECT fec_committee_id, fec_committee_name, fec_candidate_id, fec_candidate_name, sum(amount), count(*) 
    FROM fiu_pp
    WHERE fec_candidate_id IN (
    {",".join([f"'{d['id']}'" for d in DATA['Candidates']])}
    ) AND fec_committee_id IN (
        {",".join([f"'{d['id']}'" for d in DATA['Committees']])}
    )
    GROUP BY fec_committee_id, fec_candidate_id
    """
    conn = make_conn()
    pac_contributions  = pd.read_sql(q, conn)
    return pac_contributions


def get_topic_donors_by_pac(topic):
    q = f"""
        SELECT query_committee_id, query_committee_name, sum(contribution_receipt_amount), count(*)
        FROM donor_sked_a
        WHERE contributor_id IN (
            {",".join([f"'{d['id']}'" for d in DATA['Donors']['topic']])}
            )
        GROUP BY query_committee_id;
        """
    conn = make_conn()
    topic_donors_by_pac = pd.read_sql(q, conn)
    return topic_donors_by_pac
    

    