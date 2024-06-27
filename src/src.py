import os
from typing import Any, Dict, List, Tuple

import pandas as pd
import requests
from flask import render_template

from config import (BASE_URL, CANDIDATE_INFO_TABLE, DATA_COLUMNS,
                    GOV_URL_TEMPLATE, IE_TABLE, LATE_CONTRIBUTIONS_TABLE,
                    PAC_NAMES_TABLE)

from .logger import logger
from .utilities import (get_today, load_data, make_conn, query_api, query_db,
                        recursive_query, send_email)

DATA = load_data()
TODAY = get_today()


def load_results(url: str, title_params: dict = {}, limit: str = None):
    """
    Generic function to query the propublica database.
    """
    data = recursive_query(url, limit=limit)
    save_data(pd.DataFrame(data))
    try:
        return render_template(
            "index.html",
            df_html=pd.DataFrame(data).to_html(),
            params=title_params
        )
    except Exception as e:
        return f"Error: {e}"


def verify_r(r: requests.Response) -> int:
    """
    Convenience function to verify the queries worked.
    """
    assert r.status_code == 200, f"bad status code: {r.status_code}"
    assert r.json(), "bad response json"
    assert r.json()['results'], "no results loaded"
    return len(r.json()['results'])


def save_data(data: pd.DataFrame):
    """
    Writes "data" to "temp" table.
    """
    conn = make_conn()
    data.to_sql("temp", conn, if_exists='replace')
    return


def update_daily_transactions(date: str = None, trigger_email: bool = True) -> List[Dict[str, Any]]:
    """
    Gets independent expenditures for provided date. Default is today by EST.
    (Loaded with get_today())

    Input:
        date (str): date in DT_FORMAT format or None

    Output:
        output (list): list of transactions
    """
    url = os.path.join(BASE_URL, "independent_expenditures/{}/{}/{}.json")

    if not date:
        date = get_today()
    existing_ids = [
        i[0]
        for i in
        query_db(
            f"select distinct unique_id from {IE_TABLE}"
        )]

    def filter_on_ids(results):
        return [r for r in results if r['unique_id'] not in existing_ids]

    url = url.format(*date.split("-"))
    new_today_transactions = recursive_query(url, filter_func=filter_on_ids)
    new_today_transactions_df = pd.DataFrame(new_today_transactions)
    if len(new_today_transactions_df) > 0:
        engine = make_conn()
        new_today_transactions_df.to_sql(
            IE_TABLE, con=engine, if_exists="append")
        if trigger_email:
            send_email(
                f"New Independent Expenditures for {TODAY}!",
                new_today_transactions_df[DATA_COLUMNS].to_html()
            )
    return new_today_transactions_df


def update_late_contributions(**kwargs) -> pd.DataFrame:
    """
    Gets late contributions from today
    Filters out non-PAC and old contributions
    Adds committee name and candidate info
    Writes contributions and any new committee and candidate info to db
    Sends latest contributions

    Takes some kwargs for backfilling.
    """
    get_existing_late_contributions_db_data()

    contributions = get_late_contributions(**kwargs)
    if isinstance(contributions, requests.models.Response):
        contributions = contributions.json().get('results', [])

    filtered_contributions = filter_late_contributions(contributions)
    formatted_etc = bulk_format_contributions(filtered_contributions)
    formatted_contributions, pac_names_to_add, candidate_info_to_add = formatted_etc
    contributions_df = upload_and_send_late_contributions(
        formatted_contributions=formatted_contributions,
        pac_names_to_add=pac_names_to_add,
        candidate_info_to_add=candidate_info_to_add,
        trigger_email=kwargs.get('trigger_email', False)
    )
    return contributions_df


def get_late_contributions(**kwargs):
    """
    Queries ProPublica "late contributions" endpoints, depending on what param you pass.
    """
    if kwargs.get('candidate_id'):
        candidate_id = kwargs['candidate_id']
        logger.debug(f"getting late contributions for candidate {candidate_id}")
        url = os.path.join(BASE_URL, "candidates",
                           kwargs['candidate_id'], "48hour.json")
    elif kwargs.get("committee_id"):
        committee_id = kwargs['committee_id']
        logger.debug(f"getting late contributions for \
                        committee {committee_id}"
                     )
        url = os.path.join(BASE_URL, "committees",
                           kwargs['committe_id'], "48hour.json")
    elif kwargs.get("date"):
        date = kwargs['date']
        logger.debug(f"getting late contributions for date {date}")
        year, month, day = kwargs['date'].split("-")
        url = os.path.join(BASE_URL, "contributions",
                           "48hour", year, month, f"{day}.json")
    else:
        logger.debug("getting late contributions for today!")
        year, month, day = get_today().split("-")
        url = os.path.join(BASE_URL, "contributions",
                           "48hour", year, month, f"{day}.json")
    if "return_url" in kwargs and kwargs['return_url']:
        return url
    r = recursive_query(url)
    return r


def get_existing_late_contributions_db_data(return_data: bool = False):
    """
    Gets late contributions data from db.

    Used to get committee names and candidate info to fill out late contribution results

    Also avoid sending duplicate results.
    """
    global ie_df, pac_names_df, candidate_info_df, late_contributions_df
    conn = make_conn()
    ie_df = pd.read_sql(
        "select fec_candidate_id, candidate_name, office, state, district, fec_committee_id, fec_committee_name from fiu_pp",
        conn)
    ie_df.drop_duplicates(inplace=True)
    pac_names_df = pd.read_sql(f"select * from {PAC_NAMES_TABLE}", conn)
    candidate_info_df = pd.read_sql(
        f"select * from {CANDIDATE_INFO_TABLE}", conn)
    late_contributions_df = pd.read_sql(
        f"""select fec_filing_id, transaction_id
        from {LATE_CONTRIBUTIONS_TABLE}""", conn)
    if return_data:
        return ie_df, pac_names_df, candidate_info_df, late_contributions_df
    return


def get_committee_name(committee_id: str) -> Tuple[str, bool]:
    """
    Gets the name of a committee from a committee_id.

    Looks in the "pac_names" table, then the "ie_df" table, then queries PP if needed.

    Input:
        committee_id (str): FEC committee id

    Output:
        name (str): name of the committee
        update (bool): whether or not to write to "pac_names_df"
    """
    global pac_names_df
    global ie_df
    update = False
    try:
        name = pac_names_df.query(
            'fec_committee_id==@committee_id').iloc[0]['committee_name']
    except IndexError:
        update = True
        try:
            name = ie_df.query(
                'fec_committee_id==@committee_id').iloc[0]['fec_committee_name']
        except IndexError:
            url = os.path.join(BASE_URL, 'committees', committee_id) + ".json"
            r = query_api(url)
            try:
                name = r.json()['results'][0].get('name', 'NAME MISSING')
            except ValueError:
                name = "NAME QUERY ERROR"
    return name, update


def get_candidate_info(candidate_id: str) -> Tuple[Dict[Any, Any], bool]:
    """
    Gets candidate name, state, office and district from candidate ID.

    Looks in "candidate_info_df" table, then "ie_df" table, then queries PP if needed.

    Input:
        candidate_id (str): fec candidate id

    Output:
        candidate_info (dict): candidate info (see above)
        update (bool): whether or not to write to "candidate_info_df"
    """
    global candidate_info_df
    global ie_df
    update = False
    try:
        candidate_info = candidate_info_df.query(
            'fec_candidate_id==@candidate_id').to_dict('records')[0]
    except IndexError:
        update = True
        try:
            candidate_info = ie_df.query('fec_candidate_id==@candidate_id')[
                ['candidate_name', 'office', 'state', 'district']].to_dict('records')[0]
        except IndexError:
            url = os.path.join(BASE_URL, 'candidates', candidate_id) + ".json"
            r = query_api(url)
            try:  # TODO: work around split error
                _, _, state, office, district = r.json(
                )['results'][0]['district'].split("/")
                district = district.replace(".json", "")
                name = r.json()['results'][0]['display_name']
            except (KeyError, ValueError):
                name, state, office, district = ("QE", None, None, None)
            candidate_info = dict(zip(
                ['fec_candidate_id', 'candidate_name',
                    'office', 'state', 'district'],
                [candidate_id, name, office, state, district]
            ))
    if isinstance(candidate_info, list):
        candidate_info = candidate_info[0]
    print(candidate_info)
    return candidate_info, update


def format_late_contributions(contribution):
    """
    Adds committee name and candidate info to contribution (for clarity)

    Input:
        contribution: results from PP late contributions endpoint

    Output:
        contribution
        name_to_update
        candidate_info
    """
    contribution['html_url'] = GOV_URL_TEMPLATE.format(
        contribution['fec_committee_id'], contribution['fec_filing_id'])
    committee_name, update_name = get_committee_name(
        contribution['fec_committee_id']
    )
    contribution['committee_name'] = committee_name
    name_to_update = {
        "fec_committee_id": contribution['fec_committee_id'],
        "committee_name": committee_name
    } if update_name else None

    candidate_info, update_info = get_candidate_info(contribution['fec_candidate_id'])
    contribution.update(candidate_info)
    if update_info:
        candidate_info['fec_candidate_id'] = contribution['fec_candidate_id']

    candidate_info_to_update = candidate_info if update_info else None
    return contribution, name_to_update, candidate_info_to_update


def filter_late_contributions(contributions):
    """
    Filters out non-PAC contributions and contributions already in the "late_contributions" table.
    """
    global late_contributions_df
    logger.debug("*** filtering new contributions")
    filtered_contributions = [
        c for c in contributions
        if c['entity_type'] == "PAC"
        and [
            c['fec_filing_id'], c['transaction_id']
        ] not in late_contributions_df.values
    ]
    return filtered_contributions


def bulk_format_contributions(filtered_contributions):
    """
    Runs `format_late_contributions()` on a list of contributions.

    I broke this out for easier testing.
    """
    formatted_contributions = []
    pac_names_to_add = []
    candidate_info_to_add = []
    for c in filtered_contributions:
        contribution, name_to_add, new_candidate_info = format_late_contributions(c)
        if name_to_add:
            pac_names_to_add.append(name_to_add)
        if new_candidate_info:
            candidate_info_to_add.append(new_candidate_info)
        formatted_contributions.append(contribution)
    return formatted_contributions, pac_names_to_add, candidate_info_to_add


def upload_and_send_late_contributions(formatted_contributions,
                                       pac_names_to_add: list = [], candidate_info_to_add: list = [],
                                       trigger_email: bool = True) -> pd.DataFrame:
    """
    Adds "pac_names_to_add" and "candidate_info_to_add" and "formatted contributions" to db.

    Sends email.
    """
    logger.debug(
        f"""
        {len(formatted_contributions)} late contributions
        {len(pac_names_to_add)} committee names to add
        {len(candidate_info_to_add)} candidates to add
        """
    )
    conn = make_conn()
    for data, table in zip(
        [formatted_contributions, pac_names_to_add, candidate_info_to_add],
        [LATE_CONTRIBUTIONS_TABLE, PAC_NAMES_TABLE, CANDIDATE_INFO_TABLE]
    ):
        df = pd.DataFrame(data).drop_duplicates()
        df.to_sql(table, conn, if_exists="append", index=False)
    contributions_df = pd.DataFrame(formatted_contributions)
    if trigger_email is True and len(format_late_contributions) > 0:
        # existing transactions are filtered in "filter_late_contributions"
        logger.debug("*** sending new late contributions email")
        send_email(
            f"New Late Contributions for {TODAY}!",
            contributions_df.to_html()
        )
    return contributions_df
