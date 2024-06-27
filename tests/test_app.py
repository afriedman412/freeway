import os

import pandas as pd
import pytest
from flask_testing import TestCase
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from app import app
from config import BASE_URL, EMAIL_FROM, IE_TABLE
from src.utilities import make_conn, query_api, query_db, send_email, recursive_query


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

    def test_more_endpoints(self):
        for endpoint in [
            "/",
            "/committee/C00864215",
            "/filings/date/2024-3-25"
        ]:
            endpoint_response = self.client.get(endpoint)
            self.assert200(endpoint_response)

    def test_update_endpoint(self):
        for form_type in ['ie', 'late']:
            endpoint_response = self.client.post(
                f"/update/{form_type}",
                data={
                    'trigger_email': False,
                    'password': "d00d00"
                }
            )
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
    email_result = send_email(
        "testing fiu email", "this is a test message for the fiu app", to_email=EMAIL_FROM)
    assert email_result


def test_pp_query():
    url = os.path.join(BASE_URL, "independent_expenditures.json")
    r = query_api(url)
    assert r.status_code == 200


def test_committee_endpoint():
    committee_id = 'C00799031'
    committee_ies = query_db(
        f"select * from fiu_pp where fec_committee_id='{committee_id}'")
    assert len(committee_ies) > 0
    assert pd.DataFrame(committee_ies)[
        'fec_committee_name'][0] == 'United Democracy Project (Udp)'


def test_daily_transaction_filtering():
    existing_ids = [
        i[0]
        for i in
        query_db(
            f"select distinct unique_id from {IE_TABLE}"
        )]

    def filter_on_ids(results):
        return [r for r in results if r['unique_id'] not in existing_ids]

    # this date isn't special, it's just data that should already be in the db
    date = "2023-01-05"
    url = os.path.join(BASE_URL, "independent_expenditures/{}/{}/{}.json")

    url = url.format(*date.split("-"))
    new_today_transactions = recursive_query(url)
    assert len(new_today_transactions) == 12
    new_today_transactions = recursive_query(url, filter_func=filter_on_ids)
    assert len(new_today_transactions) == 0
