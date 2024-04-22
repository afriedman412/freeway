import requests
from flask import render_template, session
import os
from time import sleep
import pandas as pd
from .logger import logger

CYCLE = "2024"
BASE_URL = f"https://api.propublica.org/campaign-finance/v1/{CYCLE}"
RECURSIVE_SLEEP_TIME = 1
RETRY_SLEEP_TIME = 3
RETRIES = 5

def qry(url: str, offset: int=0) -> requests.Response:
    logger.debug(f"querying {url}")
    r = requests.get(
            url=url,
            timeout=30,
            headers={"X-API-Key": os.environ['PRO_PUBLICA_API_KEY']},
            params={'offset': offset}
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
                logger.debug(f"JSONDecodeError ... retrying in {RETRY_SLEEP_TIME}")
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
