"""
Convenience endpoints for querying Pro Publica and FEC APIs.
"""

import os

import pandas as pd
from flask import Blueprint, render_template

from config import BASE_URL, GOV_BASE_URL

from .logger import logger
from .src import get_late_contributions, load_results, query_api, save_data

api_routes = Blueprint("api_routes", __name__)


@api_routes.route("/schedule_a/<comittee_id>")
def get_schedule_a(comittee_id: str):
    """
    schedule a: money to PACs)
    """
    url = os.path.join(
        GOV_BASE_URL,
        "schedules",
        f"schedule_a?committee_id={comittee_id}")
    print(url)
    logger.debug(url)
    r = query_api(url, endpoint='g')
    return r.json()


@api_routes.route("/late")
@api_routes.route("/late/<date>")
def get_late_contributions_endpoint(date: str = None):
    """
    Get "late contribution" forms from the Pro Publica API (24/48 hour forms)

    IEs show up here often before they show up as IE data, so this is better for reporting.

    These are filtered -- only PAC contributions from CYCLE show up!

    # TODO: this only gets the specific day, not UP TO THAT DAY ... ie its not a date cutoff
    """
    if date is None:
        data = get_late_contributions()
    else:
        data = get_late_contributions(date=date)
    save_data(pd.DataFrame(data))
    return render_template(
        "index.html",
        df_html=pd.DataFrame(data).to_html(),
        params={"title": f"Late Contributions up to {date}"}
    )


@api_routes.route('/committee/<committee_id>')
def load_committee_ie(committee_id: str):
    """
    committee independent expenditures
    """
    url = os.path.join(
        BASE_URL,
        'committees',
        committee_id,
        'independent_expenditures.json'
    )
    return load_results(url, {'committee_id': committee_id})


@api_routes.route('/committee/filings/<committee_id>')
def load_committee_filings(committee_id: str):
    """
    committee filings
    """
    url = os.path.join(
        BASE_URL,
        'committees',
        committee_id,
        'filings.json'
    )
    return load_results(url, {'committee_id': committee_id})


@api_routes.route("/committee/search/<query>")
def search_committees(query: str):
    url = os.path.join(
        BASE_URL,
        'committees',
        f'search.json?query={query}'
    )
    return load_results(url, {'query': query})


@api_routes.route('/ie/date/<date>')
def load_date_ie(date: str):
    """
    independent expenditures by date
    """
    year, month, day = date.split('-')
    url = os.path.join(
        BASE_URL,
        'independent_expenditures',
        year, month, f"{day}.json"
    )
    return load_results(url, {'date': date})


@api_routes.route("/filings/date/<date>")
def load_date_filings(date: str):
    """
    filings by date
    """
    year, month, day = date.split('-')
    url = os.path.join(
        BASE_URL,
        'filings',
        year, month, f"{day}.json"
    )
    return load_results(url, {'date': date})
