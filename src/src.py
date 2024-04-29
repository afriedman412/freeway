import os
from time import sleep

import pandas as pd
import requests
from flask import render_template
import pytz
from datetime import timedelta
from datetime import datetime as dt

from .logger import logger
from .config import RECURSIVE_SLEEP_TIME, RETRY_SLEEP_TIME, BASE_URL, IE_TABLE, DATA_COLUMNS, DT_FORMAT
from .utilities import load_data, query_api, get_today, query_table, make_conn, send_email
from typing import Callable, List, Dict, Any



DATA = load_data()


def recursive_query(
        url: str, 
        increment: int = 20, 
        api_type: str = 'p',
        limit: int = None, 
        filter: Callable = None
        ):
    """
    Queries `url`, offsetting by `increment` every time, until no results or `limit` results.

    INPUTS:
        url (str): endpoint for the Pro Publica or FEC API
        increment (int): number of results to return per page (passed to API)
        api_type (str): 'p' for Pro Publica, 'g' for FEC ... changes behavior and auth
        limit (int): if provided, break the loop and return results when total results passes limit
        filter (callable): function to filter query results by

    OUTPUT:
        bucket (list): accumulated query results
    """
    bucket = []
    offset = 0
    counter = 0
    while True:
        logger.debug(f"offset: {offset}")
        r = query_api(url, offset=offset, api_type=api_type, per_page=increment)
        if r.status_code == 200:
            try:
                results = r.json().get('results')
                if results:
                    if filter:
                        if not filter(results):
                            break
                        else:
                            results = filter(results)
                    bucket += results
                    if limit and len(bucket) > limit:
                        break
                    offset += increment
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
    data = recursive_query(url, limit=limit)
    save_data(pd.DataFrame(data))
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


def save_data(data: pd.DataFrame):
    conn = make_conn()
    data.to_sql("temp", conn, if_exists='replace')
    return


def get_late_contributions(date_cutoff: str = None):
    """
    Get 24/48 hour contributions from PP api.
    """
    url = os.path.join(BASE_URL, "contributions/48hour.json")
    if not date_cutoff:
        tz = pytz.timezone('America/New_York')
        today = dt.now().astimezone(tz)
        date_cutoff = (today - timedelta(4)).strftime(DT_FORMAT)
    else:
        date_cutoff = dt.strptime(date_cutoff, DT_FORMAT).strftime(DT_FORMAT)
    data = recursive_query(
        url,
        filter=lambda transactions: [
            t for t in transactions 
            if t['contribution_date'] > date_cutoff
            and t['cycle'] == 2024
            and t['entity_type'] == "PAC"
            ]
        )
    return data


def update_daily_transactions(date: str = None, send_email: bool = True) -> List[Dict[str, Any]]:
    """
    Gets independent expenditures for provided date. Default is today by EST.
    (Loaded with get_today())



    Input:
        date (str): date in DT_FORMAT format or None

    Output:
        output (list): list of transactions
    """
    url = os.path.join(BASE_URL, "independent_expenditures/{}/{}/{}.json")

    if not date:
        date = get_today()
    # if not re.search(DT_FORMAT, date):
    #     # this should error out to the 500 endpoint
    #     date = parse(date).strftime(DT_FORMAT)
    existing_ids = [
        i[0]
        for i in
        query_table(
            f"select distinct unique_id from {IE_TABLE}"
        )]
    def filter_on_ids(results):
        return [r for r in results if r['unique_id'] not in existing_ids]

    url = url.format(*date.split("-"))
    new_today_transactions = recursive_query(url, filter=filter_on_ids)
    new_today_transactions_df = pd.DataFrame(new_today_transactions)
    engine = make_conn()
    new_today_transactions_df.to_sql(IE_TABLE, con=engine, if_exists="append")
    if send_email:
            send_email(
                f"New Independent Expenditures for {os.getenv('TODAY', 'error')}!",
                new_today_transactions_df[DATA_COLUMNS].to_html()
            )
    return new_today_transactions_df
