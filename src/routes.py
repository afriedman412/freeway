import pandas as pd
from flask import Blueprint, Response, session
import os
import requests

from .src import BASE_URL, GOV_BASE_URL, load_results, qry
from .logger import logger

api_routes = Blueprint("api_routes", __name__)


@api_routes.route("/gov/<etc>")
def gov_api_shortcut(etc: str):
    """
    for testing fec.gov endpoints
    """
    url = os.path.join(GOV_BASE_URL, etc)
    print(url)
    logger.debug(url)
    r = qry(url, endpoint='g')
    return r.json()


@api_routes.route('/committee/<committee_id>')
def load_committee_ie(committee_id: str):
    """
    committee independent expenditures
    """
    url = "/".join([BASE_URL, 'committees', committee_id,
                   'independent_expenditures.json'])
    return load_results(url, {'committee_id': committee_id})


@api_routes.route('/committee/filings/<committee_id>')
def load_committee_filings(committee_id: str):
    """
    committee filings
    """
    url = "/".join([BASE_URL, 'committees', committee_id, 'filings.json'])
    return load_results(url, {'committee_id': committee_id})


@api_routes.route('/ie/date/<date>')
def load_date_ie(date: str):
    """
    independent expenditures by date
    """
    year, month, day = date.split('-')
    url = "/".join([BASE_URL, 'independent_expenditures',
                   year, month, f"{day}.json"])
    return load_results(url, {'date': date})


@api_routes.route("/filings/date/<date>")
def load_date_filings(date: str):
    """
    filings by date
    """
    year, month, day = date.split('-')
    url = "/".join([BASE_URL, 'filings', year, month, f"{day}.json"])
    return load_results(url, {'date': date})


@api_routes.route("/download", methods=['POST'])
def download_current_data():
    """
    download most recently loaded data
    """
    current_data = session['current_data']
    return Response(
        pd.DataFrame(current_data).to_csv(index=False),
        mimetype="text/csv",
        headers={
            "Content-disposition":
            "attachment; filename={}".format("download.csv")
        })
