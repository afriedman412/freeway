import pandas as pd
from flask import (Blueprint, Response, current_app, jsonify, render_template,
                   request, url_for)

from .config import IE_TABLE
from .logger import logger
from .src import save_data, update_daily_transactions, update_late_contributions
from .utilities import get_today, query_table

main_routes = Blueprint("main_routes", __name__)


@main_routes.route("/")
@main_routes.route("/<date>")
def get_ies(date: str = None):
    """
    Gets Independent Expenditures.

    Returns IEs for today or `date`, or returns 10 most recent.

    Canonical date column is `dissemination_date`.
    """
    custom_date = True
    returned_transactions = True
    if not date:
        logger.debug('IEs for today...')
        date = get_today()
        custom_date = False
    date_ies = query_table(
        f"select * from {IE_TABLE} where dissemination_date='{date}'")
    if not date_ies:
        ie_message = f"No Independent Expenditures for {
            date}." if custom_date else f"No Independent Expenditures for today ({date})."
        logger.debug("No IEs for selected date, getting 10 most recent")
        date_ies = query_table(
            f"select * from {IE_TABLE} order by dissemination_date desc limit 10")
        returned_transactions = False
    ie_message = f"Independent Expenditures for {
        date}." if custom_date else f"New Independent Expenditures for today ({date})!!"
    df = pd.DataFrame(date_ies)
    save_data(df)
    logger.debug(df.shape)
    return render_template(
        'index.html',
        today=date,
        custom_date=custom_date,
        returned_transactions=returned_transactions,
        ie_message=ie_message,
        df_html=df.to_html()
    )


@main_routes.route("/committee/<committee_id>")
def get_committee_ies(committee_id: str):
    logger.debug(f"Getting IEs for {committee_id}...")
    df = pd.DataFrame()
    committee_name = None
    committee_ies = query_table(
        f"select * from fiu_pp where fec_committee_id='{committee_id}'")
    if committee_ies:
        df = pd.DataFrame(committee_ies)
        committee_name
        save_data(df)
        logger.debug(df.shape)
    return render_template(
        'committee_ie.html',
        df_html=df.to_html(),
        committee_id=committee_id,
        committee_name=df['fec_committee_name'][0]
    )


@main_routes.route("/update/<form_type>", methods=['POST'])
def update_forms(form_type: str):
    """
    Updates Independent Expenditure data or 24/48 Hour Data.

    form_type (str): either 'ie' or 'late', returns "bad form type" if not.
    """
    if form_type not in ['ie', 'late']:
        return "bad form type"
    if request.form.get("password") == 'd00d00':
        email_trigger = request.form.get('send_email', False)
        new_transactions_df = update_daily_transactions(
            send_email=email_trigger)
        return new_transactions_df.to_json()
    else:
        "bad password!"


@main_routes.route("/download", methods=['POST'])
def download_current_data():
    """
    Downloads most recently loaded data.
    """
    current_data = query_table("select * from temp")
    return Response(
        pd.DataFrame(current_data).to_csv(index=False),
        mimetype="text/csv",
        headers={
            "Content-disposition":
            "attachment; filename={}".format("download.csv")
        })


@main_routes.route("/saved_data")
def show_currrent_data():
    """
    Shows saved data.
    """
    current_data = query_table("select * from temp")
    return render_template(
        "index.html",
        df_html=pd.DataFrame(current_data).to_html(),
        params={"title": "Current Data"}
    )


@main_routes.route('/routes', methods=['GET'])
def get_routes() -> str:
    """
    Shows all available routes.
    """
    routes = []
    for rule in current_app.url_map.iter_rules():
        routes.append({
            "endpoint": rule.endpoint,
            "methods": list(rule.methods),
            "url": str(rule)
        })
    return render_template('routes.html', routes=routes)


@main_routes.route("/favicon.ico")
def favicon():
    return url_for('static', filename='data:,')


@main_routes.route("/sludge_data/<table>")
def get_sludge_data(table: str):
    q = f"select * from {table}"
    data = query_table(q)
    df = pd.DataFrame(data)
    return render_template(
        'old_index.html',
        params={"table": table},
        df_html=df.to_html()
    )


@main_routes.route("/late_contributions/update")
def update_late_contributions_endpoint() -> str:
    # get kwargs from GET
    update_late_contributions(
        date=request.args.get('date'),
        trigger_email=request.args.get('trigger_email')
    )
    return "Late contributions updated!"


@main_routes.route('/late_contributions/')
@main_routes.route('/late_contributions/<date>')
def show_late_contributions(date: str = None) -> str:
    """
    Gets late contributions.

    Returns Late Contributions for today or `date`, or returns 10 most recent.
    """
    custom_date = True
    returned_transactions = True
    if not date:
        logger.debug('Late Contributions for today...')
        date = get_today()
        custom_date = False
    date_ies = query_table(
        f"select * from late_contributions where contribution_date='{date}'")
    if not date_ies:
        message = f"No Late Contributions for {
            date}." if custom_date else f"No Late Contributions for today ({date})."
        logger.debug("No IEs for selected date, getting 10 most recent")
        date_ies = query_table(
            "select * from late_contributions order by contribution_date desc limit 10")
        returned_transactions = False
    message = f"Late Contributions for {
        date}." if custom_date else f"Late Contributions for today ({date})!!"
    df = pd.DataFrame(date_ies)
    save_data(df)
    logger.debug(df.shape)
    return render_template(
        'index.html',
        today=date,
        custom_date=custom_date,
        returned_transactions=returned_transactions,
        ie_message=message,
        df_html=df.to_html()
    )
