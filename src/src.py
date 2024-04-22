import os
from time import sleep

import pandas as pd
import requests
from flask import render_template, session

from .logger import logger

GOV_BASE_URL = "https://api.open.fec.gov/v1/"
CYCLE = "2024"
BASE_URL = f"https://api.propublica.org/campaign-finance/v1/{CYCLE}"
RECURSIVE_SLEEP_TIME = 1
RETRY_SLEEP_TIME = 3
RETRIES = 5


"""
curl -X 'GET' \
  'https://api.open.fec.gov/v1/candidates/search/?page=1&per_page=20&q=tester&sort=name&sort_hide_null=false&sort_null_only=false&sort_nulls_last=false&api_key=DEMO_KEY' \
  -H 'accept: application/json'
"""


def qry(url: str, endpoint: str = 'p', offset: int = 0) -> requests.Response:
    logger.debug(f"querying {url}")
    headers = {}
    params = {'offset': offset}
    assert endpoint in 'pg'
    if endpoint == 'p':
        headers['X-API-Key'] = os.environ['PRO_PUBLICA_API_KEY']
    if endpoint == 'g':
        params['api_key'] = os.environ['GOV_API_KEY']

    r = requests.get(
        url=url,
        timeout=30,
        headers=headers,
        params=params
    )
    logger.debug(f"status: {r.status_code}")
    return r


def recursive_query(url: str):
    bucket = []
    offset = 0
    counter = 0
    while True:
        logger.debug(f"offset: {offset}")
        r = qry(url, offset=offset)
        if r.status_code == 200:
            try:
                transactions = r.json().get('results')
                if transactions:
                    bucket += transactions
                    offset += 20
                    sleep(RECURSIVE_SLEEP_TIME)
                else:
                    break
            except requests.exceptions.JSONDecodeError:
                logger.debug(f"JSONDecodeError ... retrying in {
                             RETRY_SLEEP_TIME}")
                sleep(RETRY_SLEEP_TIME)
                counter += 1

        else:
            raise Exception(", ".join(
                [
                    str(r.status_code),
                    r.json().get(
                        "message", "error retrieving error message"
                    )]))
    logger.debug(f"total results: {len(bucket)}")
    return bucket


def load_results(url, params):
    session['current_data'] = recursive_query(url)
    try:
        return render_template(
            "index.html",
            df_html=pd.DataFrame(session['current_data']).to_html(),
            params=params
        )
    except Exception as e:
        return f"Error: {e}"


def verify_r(r: requests.Response):
    assert r.status_code == 200, f"bad status code: {r.status_code}"
    assert r.json(), "bad response json"
    assert r.json()['results'], "no results loaded"
    return len(r.json()['results'])
