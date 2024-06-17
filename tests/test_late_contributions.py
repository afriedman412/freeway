import json
import os
import pandas as pd
from config import CYCLE, PAC_NAMES_TABLE, BASE_URL
from src.src import filter_late_contributions, bulk_format_contributions, get_late_contributions


def test_config():
    assert CYCLE == 2024

def test_late_contributions_query():
    date = "2024-04-01"
    today_late_transactions_url = get_late_contributions(date=date, return_url=True)
    assert today_late_transactions_url == "https://api.propublica.org/campaign-finance/v1/2024/contributions/48hour/2024/04/01.json"

    today_late_transactions = get_late_contributions(date=date)
    today_late_transactions_df = pd.DataFrame(today_late_transactions)
    assert len(today_late_transactions_df) == 3

def test_add_candidate_info(test_contributions):
    filtered_contributions = filter_late_contributions(test_contributions)
    assert len(filtered_contributions) == 8


def test_format_late_contributions(test_contributions):
    assert PAC_NAMES_TABLE == "pac_names_testo"

    filtered_contributions = filter_late_contributions(test_contributions)
    assert len(filtered_contributions) == 8

    formatted_contributions, pac_names_to_add, candidate_info_to_add = bulk_format_contributions(filtered_contributions)

    assert len(formatted_contributions) == 8
    assert sorted([c['committee_name'] for c in formatted_contributions])[0] == "BICE FOR CONGRESS"

    assert len(pac_names_to_add) == 7
    assert sorted([c['committee_name'] for c in pac_names_to_add])[0] == 'BICE FOR CONGRESS'

    assert len(candidate_info_to_add) == 5
    assert sorted([c['candidate_name'] for c in candidate_info_to_add])[0] == 'DUSTY JOHNSON'