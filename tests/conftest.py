import json
import os

import pandas as pd
import pytest
from sqlalchemy import text

from src.src import get_existing_late_contributions_db_data
from src.utilities import make_conn, query_db

TABLES = ['candidate_info', 'late_contributions', 'pac_names']
TEST_FORMAT = "{}_testo"

os.environ["FLASK_ENV"] = "test"


@pytest.fixture(scope="session", autouse=True)
def testing_setup():
    assert os.getenv("FLASK_ENV") == 'test'
    make_test_tables()

    global ie_df, pac_names_df, candidate_info_df, late_contributions_df
    ie_df, pac_names_df, candidate_info_df, late_contributions_df = get_existing_late_contributions_db_data(return_data=True)

    yield
    teardown_test_tables()
    existing_tables = [t[0] for t in query_db("SHOW TABLES")]
    for t in TABLES:
        assert TEST_FORMAT.format(t) not in existing_tables


@pytest.fixture(scope="session", autouse=True)
def test_contributions():
    test_contributions = json.load(open("tests/late_contributions_test_data/test_contributions_to_add.json"))
    return test_contributions


def make_test_tables():
    print("*** MAKING TEST TABLES")
    conn = make_conn()
    for t in TABLES:
        df = pd.read_json(f"tests/late_contributions_test_data/test_{t}.json")
        df.to_sql(TEST_FORMAT.format(t), conn, if_exists="replace")
    conn.dispose()


def teardown_test_tables():
    print("*** TEARING DOWN TEST TABLES")
    conn = make_conn()
    for t in TABLES:
        with conn.connect() as ccc:
            for t in TABLES:
                ccc.execute(text(
                    f"DROP TABLE IF EXISTS {TEST_FORMAT.format(t)};"
                ))

    conn.dispose()
