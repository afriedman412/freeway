"""
General functions that everyone should have access to!
"""
import yaml
import pytz
from .config import DT_FORMAT, EMAIL_FROM, EMAILS_TO
from datetime import datetime as dt
from sqlalchemy.engine.base import Engine
from sqlalchemy import create_engine, text
from .logger import logger
import requests
import os
from typing import List, Tuple, Union
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail


def load_data():
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


def query_table(q: str) -> List[Tuple]:
    """
    Implement dict cursor!
    """
    engine = make_conn()
    with engine.connect() as conn:
        t = conn.execute(text(q))
        output = t.fetchall()
    engine.dispose()
    return output


def query_api(url: str, api_type: str = 'p', offset: int = 0, per_page: int = 20) -> requests.Response:
    logger.debug(f"querying {url}")
    headers = {}
    params = {'offset': offset}
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
        to_email: Union[str, list] = EMAILS_TO):
    message = Mail(
        from_email=from_email,
        to_emails=to_email,
        subject=subject,
        html_content=body)
    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        response = sg.send(message)
        assert response.status_code == 202, f"Bad status code: {response.status_code}"
        logger.debug("Email sent successfully!")
        return True
    except Exception as e:
        logger.debug(f"Error while sending email: {e}")
        return False