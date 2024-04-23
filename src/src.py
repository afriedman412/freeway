import os
from time import sleep
import gzip
import base64
import json

import pandas as pd
import requests
from flask import render_template, session

from .logger import logger
from .config import RECURSIVE_SLEEP_TIME, RETRY_SLEEP_TIME
from typing import Callable


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


def recursive_query(url: str, limit: int=None, filter: Callable=None):
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
                    if filter:
                        if not filter(transactions):
                            break
                        else:
                            transactions = filter(transactions)
                    bucket += transactions
                    if limit and len(bucket) > limit:
                        break
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


def load_results(url: str, title_params: dict={}, limit: str=None):
    data = recursive_query(url, limit)
    save_data(data)
    try:
        return render_template(
            "index.html",
            df_html=pd.DataFrame(data).to_html(),
            params=title_params
        )
    except Exception as e:
        return f"Error: {e}"


def verify_r(r: requests.Response):
    assert r.status_code == 200, f"bad status code: {r.status_code}"
    assert r.json(), "bad response json"
    assert r.json()['results'], "no results loaded"
    return len(r.json()['results'])


def save_data(data):
    """
    Too much data = can't save

    Still has a limit, but that's for later.
    """
    compressed_data = gzip.compress(json.dumps(data).encode('utf-8'))
    encoded_data = base64.b64encode(compressed_data)
    session['current_data'] = encoded_data


def decompress_data(data):
    compressed_data = base64.b64decode(data.decode("utf-8"))
    decompressed_data = gzip.decompress(compressed_data)
    return json.loads(decompressed_data.decode('utf-8'))