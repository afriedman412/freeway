from flask import Blueprint, session, Response
from .src import BASE_URL, load_results
import pandas as pd

api_routes =Blueprint("api_routes", __name__)

# committee independent expenditures
@api_routes.route('/committee/<committee_id>')
def load_committee_ie(committee_id: str):
    url = "/".join([BASE_URL, 'committees', committee_id, 'independent_expenditures.json'])
    return load_results(url, {'committee_id': committee_id})

# committee filings
@api_routes.route('/committee/filings/<committee_id>')
def load_committee_filings(committee_id: str):
    url = "/".join([BASE_URL, 'committees', committee_id, 'filings.json'])
    return load_results(url, {'committee_id': committee_id})

# independent expenditures by date
@api_routes.route('/ie/date/<date>')
def load_date_ie(date: str):
    year, month, day = date.split('-')
    url = "/".join([BASE_URL, 'independent_expenditures', year, month, f"{day}.json"])
    return load_results(url, {'date': date})

# filings by date
@api_routes.route("/filings/date/<date>")
def load_date_filings(date: str):
    year, month, day = date.split('-')
    url = "/".join([BASE_URL, 'filings', year, month, f"{day}.json"])
    return load_results(url, {'date': date})

# download latest data
@api_routes.route("/download", methods=['POST'])
def download_current_data():
    current_data = session['current_data']
    return Response(
        pd.DataFrame(current_data).to_csv(index=False),
        mimetype="text/csv",
        headers={
            "Content-disposition":
            "attachment; filename={}".format("download.csv")
        })