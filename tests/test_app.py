import os

import pytest
from flask_testing import TestCase
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
import pandas as pd

from app import app
from src.config import BASE_URL, EMAIL_FROM
from src.utilities import make_conn, query_api, send_email, query_table
from src.src import recursive_query


class TestFolio(TestCase):
    def create_app(self):
        return app

    def test_route(self):
        response = self.client.get("/basic")
        self.assert200(response)

    def test_endpoints(self):
        for endpoint in ["/", "/committee/C00864215", "/dates"]:
            endpoint_response = self.client.get(endpoint)
            self.assert200(endpoint_response)


def test_create_engine_success():
    engine = make_conn()
    try:
        with engine.connect() as conn:
            conn.execute(text("show tables"))
    except OperationalError as e:
        pytest.fail(f"Connection failed with error: {e}")


def test_create_engine_failure():
    engine = create_engine('sqlite:///this_will_fail')
    with pytest.raises(OperationalError):
        with engine.connect() as conn:
            conn.execute(text("show tables"))


def test_email():
    email_result = send_email("testing fiu email", "this is a test message for the fiu app", to_email=EMAIL_FROM)
    assert email_result


def test_pp_query():
    url = os.path.join(BASE_URL, "independent_expenditures.json")
    r = query_api(url)
    assert r.status_code == 200


def test_committee_endpoint():
    committee_id = 'C00799031'
    committee_ies = query_table(f"select * from fiu_pp where fec_committee_id='{committee_id}'")
    assert len(committee_ies) > 0
    assert pd.DataFrame(committee_ies)['fec_committee_name'][0] == 'United Democracy Project (Udp)'


def test_daily_transactions():
    url = os.path.join(BASE_URL, "independent_expenditures/{}/{}/{}.json")
    date = "2024-04-01"
    url = url.format(*date.split("-"))
    new_today_transactions = recursive_query(url)
    new_today_transactions_df = pd.DataFrame(new_today_transactions)
    assert len(new_today_transactions_df) == 69
