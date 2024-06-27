"""
General functions that everyone should have access to!
"""
import os
from datetime import datetime as dt
from time import sleep
from typing import Callable, List, Tuple, Union

import pytz
import requests
import yaml
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine

from config import (DT_FORMAT, EMAIL_FROM, EMAILS_TO, RECURSIVE_SLEEP_TIME,
                    RETRY_SLEEP_TIME)

from .logger import logger


def load_data():
    """
    Gets donor, committee and candidate info from YAML file.

    Probably better ways/places to store this, but this is fine for now.
    """
    with open("src/data.yaml") as f:
        data = yaml.safe_load(f)
    return data


def get_today() -> str:
    tz = pytz.timezone('America/New_York')
    today = dt.now().astimezone(tz)
    today = today.strftime(DT_FORMAT)
    return today


def make_conn() -> Engine:
    sql_string = "mysql://{}:{}@{}/{}".format(
        "o1yiw20hxluaaf9p",
        os.getenv('MYSQL_PW'),
        "phtfaw4p6a970uc0.cbetxkdyhwsb.us-east-1.rds.amazonaws.com",
        "izeloqfyy070up9b"
    )
    engine = create_engine(sql_string)
    return engine


def query_db(q: str) -> List[Tuple]:
    """
    TODO: Implement dict cursor!
    """
    engine = make_conn()
    with engine.connect() as conn:
        t = conn.execute(text(q))
        output = t.fetchall()
    engine.dispose()
    return output


def query_api(url: str, api_type: str = 'p', offset: int = 0, per_page: int = 20, **params) -> requests.Response:
    """
    Get data from the Pro Publica or FEC APIs.

    INPUTS:
        url (str): endpoint to query
        api_type (str): 'p' for Pro Publica, 'g' for FEC ("government") ... sets auth and pagination
        (check for any large-scale pagination FEC queries because pagination is different)
        offset (int): retrives entries starting here
        per_page (int): return this many entries (FEC API only -- PP is set to 20)
    """
    logger.debug(f"querying {url}")
    headers = {}
    params['offset'] = offset
    assert api_type in 'pg'
    if api_type == 'p':
        headers['X-API-Key'] = os.environ['PRO_PUBLICA_API_KEY']
    if api_type == 'g':
        params['api_key'] = os.environ['GOV_API_KEY']
        params['per_page'] = per_page

    r = requests.get(
        url=url,
        timeout=30,
        headers=headers,
        params=params
    )
    logger.debug(f"status: {r.status_code}")
    return r


def send_email(
    subject,
    body,
    from_email: Union[str, list] = EMAIL_FROM,
    to_email: Union[str, list] = EMAILS_TO
):
    message = Mail(
        from_email=from_email,
        to_emails=to_email,
        subject=subject,
        html_content=body)
    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        response = sg.send(message)
        assert response.status_code == 202, \
            f"Bad status code: {response.status_code}"
        logger.debug("Email sent successfully!")
        return True
    except Exception as e:
        logger.debug(f"Error while sending email: {e}")
        return False


def recursive_query(
        url: str,
        increment: int = 20,
        api_type: str = 'p',
        limit: int = None,
        filter_func: Callable = None,
        params: dict = {}
):
    """
    Queries `url`, offsetting by `increment` every time, until no results or `limit` results.

    INPUTS:
        url (str): endpoint for the Pro Publica or FEC API
        increment (int): number of results to return per page (passed to API)
        api_type (str): 'p' for Pro Publica, 'g' for FEC ... changes behavior and auth
        limit (int): if provided, break the loop and return results when total results passes limit
        filter (callable): function to filter query results by
        params (dict): additional parameters to pass to query

    OUTPUT:
        bucket (list): accumulated query results
    """
    bucket = []
    offset = 0
    counter = 0
    while True:
        logger.debug(f"offset: {offset}")
        r = query_api(url, offset=offset, api_type=api_type,
                      per_page=increment, params=params)
        if r.status_code == 200:
            try:
                results = r.json().get('results')
                if results:
                    if filter_func:
                        results = filter_func(results)
                    bucket += results
                    if limit and len(bucket) > limit:
                        break
                    offset += increment
                    sleep(RECURSIVE_SLEEP_TIME)
                else:
                    break
            except requests.exceptions.JSONDecodeError:
                logger.debug(f"JSONDecodeError ... \
                             retrying in {RETRY_SLEEP_TIME}")
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
