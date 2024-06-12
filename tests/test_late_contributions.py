import json
import os

from config import CYCLE, PAC_NAMES_TABLE
from src.src import filter_and_format_late_contributions


def test_config():
    assert CYCLE == 2024


def test_late_contributions():
    assert PAC_NAMES_TABLE == "pac_names_testo"
    test_contributions = json.load(open("tests/test_contributions.json"))[-10:]

    r = filter_and_format_late_contributions(test_contributions)
    formatted_contributions, pac_names_to_add, candidate_info_to_add = r
    print("LENGTHS:", len(formatted_contributions), len(pac_names_to_add), len(candidate_info_to_add))

    assert formatted_contributions
    assert formatted_contributions[0]['contributor_organization_name'] == 'NATIONAL ASSOCIATION OF MUTUAL INSURANCE COS PAC'

    assert pac_names_to_add
    assert pac_names_to_add[0]['committee_name'] == 'BICE FOR CONGRESS'

    assert candidate_info_to_add
    assert candidate_info_to_add[0]['candidate_name'] == 'Stephanie Bice'
