import os

import pandas as pd
import pytest
from sqlalchemy import text

from src.utilities import make_conn

TABLES = ['candidate_info', 'late_contributions', 'pac_names']
TEST_FORMAT = "{}_testo"

os.environ["FLASK_ENV"] == "test"


@pytest.fixture(scope="session", autouse=True)
def testing_setup():
    assert os.getenv("FLASK_ENV") == 'test'
    make_test_tables()
    yield
    teardown_test_tables()


def make_test_tables():
    print("*** MAKING TEST TABLES")
    conn = make_conn()
    for t in TABLES:
        df = pd.read_sql(f"select * from {t} limit 5", conn)
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
