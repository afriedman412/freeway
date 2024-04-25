import pandas as pd
from flask import Blueprint, Response, session, render_template
import os
import requests
import pytz
from datetime import datetime as dt
from datetime import timedelta

from .src import load_results, qry, recursive_query, save_data, decompress_data
from .config import BASE_URL, GOV_BASE_URL, DT_FORMAT
from .logger import logger

api_routes = Blueprint("api_routes", __name__)


@api_routes.route("/schedule_a/<comittee_id>")
def get_schedule_a(comittee_id: str):
    """
    schedule a: money to PACs)
    """
    # https://api.open.fec.gov/v1/schedules/schedule_a/
    url = os.path.join(
        GOV_BASE_URL,
        "schedules",
        f"schedule_a?committee_id={comittee_id}")
    print(url)
    logger.debug(url)
    r = qry(url, endpoint='g')
    return r.json()


@api_routes.route("/late")
@api_routes.route("/late/<date_cutoff>")
def get_late_contributions(date_cutoff: str = None):
    url = os.path.join(BASE_URL, "contributions/48hour.json")
    if not date_cutoff:
        tz = pytz.timezone('America/New_York')
        today = dt.now().astimezone(tz)
        date_cutoff = (today - timedelta(4)).strftime(DT_FORMAT)
    else:
        date_cutoff = dt.strptime(date_cutoff, DT_FORMAT).strftime(DT_FORMAT)
    data = recursive_query(
        url,
        filter=lambda transactions: [
            t for t in transactions 
            if t['contribution_date'] > date_cutoff
            and t['cycle'] == 2024
            and t['entity_type'] == "PAC"
            ]
        )
    save_data(data)
    return render_template(
            "index.html",
            df_html=pd.DataFrame(data).to_html(),
            params={"title": f"Late Contributions up to {date_cutoff}"}
        )


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


@api_routes.route("/committee/search/<query>")
def search_committees(query: str):
    url = "/".join([BASE_URL, 'committees', f'search.json?query={query}'])
    return load_results(url, {'query': query})


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
    current_data = decompress_data(session['current_data'])
    return Response(
        pd.DataFrame(current_data).to_csv(index=False),
        mimetype="text/csv",
        headers={
            "Content-disposition":
            "attachment; filename={}".format("download.csv")
        })


@api_routes.route("/data")
def show_currrent_data():
    current_data = decompress_data(session['current_data'])
    return render_template(
            "index.html",
            df_html=pd.DataFrame(current_data).to_html(),
            params={"title": "Current Data"}
        )