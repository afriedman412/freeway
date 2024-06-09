import os
from time import sleep
from typing import Any, Callable, Dict, List, Tuple

import pandas as pd
import requests
from flask import render_template

from .config import (BASE_URL, DATA_COLUMNS, IE_TABLE, RECURSIVE_SLEEP_TIME,
                     RETRY_SLEEP_TIME)
from .logger import logger
from .utilities import (get_today, load_data, make_conn, query_api,
                        query_table, send_email)

DATA = load_data()


def recursive_query(
        url: str,
        increment: int = 20,
        api_type: str = 'p',
        limit: int = None,
        filter: Callable = None,
        params: dict = {}
):
    """
    Queries `url`, offsetting by `increment` every time, until no results or `limit` results.

    INPUTS:
        url (str): endpoint for the Pro Publica or FEC API
        increment (int): number of results to return per page (passed to API)
        api_type (str): 'p' for Pro Publica, 'g' for FEC ... changes behavior and auth
        limit (int): if provided, break the loop and return results when total results passes limit
        filter (callable): function to filter query results by
        params (dict): additional parameters to pass to query

    OUTPUT:
        bucket (list): accumulated query results
    """
    bucket = []
    offset = 0
    counter = 0
    while True:
        logger.debug(f"offset: {offset}")
        r = query_api(url, offset=offset, api_type=api_type,
                      per_page=increment, params=params)
        if r.status_code == 200:
            try:
                results = r.json().get('results')
                if results:
                    if filter and filter(results):
                        results = filter(results)
                    bucket += results
                    if limit and len(bucket) > limit:
                        break
                    offset += increment
                    sleep(RECURSIVE_SLEEP_TIME)
                else:
                    break
            except requests.exceptions.JSONDecodeError:
                logger.debug(f"JSONDecodeError ... retrying in {
                             RETRY_SLEEP_TIME}")
                sleep(RETRY_SLEEP_TIME)
                counter += 1

        else:
            raise Exception(", ".join(
                [
                    str(r.status_code),
                    r.json().get(
                        "message", "error retrieving error message"
                    )]))
    logger.debug(f"total results: {len(bucket)}")
    return bucket


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
    # if not re.search(DT_FORMAT, date):
    #     # this should error out to the 500 endpoint
    #     date = parse(date).strftime(DT_FORMAT)
    existing_ids = [
        i[0]
        for i in
        query_table(
            f"select distinct unique_id from {IE_TABLE}"
        )]

    def filter_on_ids(results):
        return [r for r in results if r['unique_id'] not in existing_ids]

    url = url.format(*date.split("-"))
    new_today_transactions = recursive_query(url, filter=filter_on_ids)
    new_today_transactions_df = pd.DataFrame(new_today_transactions)
    if len(new_today_transactions_df) > 0:
        engine = make_conn()
        new_today_transactions_df.to_sql(
            IE_TABLE, con=engine, if_exists="append")
        if trigger_email:
            send_email(
                f"New Independent Expenditures for {
                    os.getenv('TODAY', 'error')}!",
                new_today_transactions_df[DATA_COLUMNS].to_html()
            )
    return new_today_transactions_df


def update_late_contributions(**kwargs):
    """
    Gets late contributions from today
    Filters out non-PAC and old contributions
    Adds committee name and candidate info
    Writes contributions and any new committee and candidate info to db
    Sends latest contributions

    Takes some kwargs for backfilling.
    """
    contributions = get_late_contributions(**kwargs)
    f_contributions_etc = filter_and_format_late_contributions(contributions)
    upload_and_send_late_contributions(*f_contributions_etc, trigger_email=kwargs.get('trigger_email', False))
    return


def get_late_contributions(**kwargs):
    """
    Queries ProPublica "late contributions" endpoints, depending on what param you pass.
    """
    if kwargs.get("candidate_id"):
        logger.debug(
            f"getting late contributions for candidate {kwargs['candidate_id']}")
        url = os.path.join(BASE_URL, "candidates",
                           kwargs['candidate_id'], "48hour.json")
    elif kwargs.get("committe_id"):
        logger.debug(
            f"getting late contributions for committee {kwargs['committe_id']}"
            )
        url = os.path.join(BASE_URL, "committees",
                           kwargs['committe_id'], "48hour.json")
    elif kwargs.get("date"):
        logger.debug(
            f"getting late contributions for date {kwargs['date']}"
            )
        year, month, day = kwargs['date'].split("-")
        url = os.path.join(BASE_URL, "contributions",
                           "48hour", year, month, f"{day}.json")
    else:
        logger.debug(
            "getting late contributions for today!"
            )
        year, month, day = get_today().split("-")
        url = os.path.join(BASE_URL, "contributions",
                           "48hour", year, month, f"{day}.json")
    if "return_url" in kwargs and kwargs['return_url']:
        return url
    r = recursive_query(url)
    return r


def get_existing_late_contributions_db_data():
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
    pac_names_df = pd.read_sql("select * from pac_names", conn)
    candidate_info_df = pd.read_sql("select * from candidate_info", conn)
    late_contributions_df = pd.read_sql(
        "select fec_filing_id, transaction_id from late_contributions", conn)
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
            try:  # work around split error
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


def filter_and_format_late_contributions(contributions: List) -> Tuple[List, List, List]:
    """
    Filters out non-PAC contributions and contributions already in the "late_contributions" table.

    Adds committee name and candidate info to all remaining contributions (for clarity)

    Collects candidate info and committees that aren't already in db.

    Input:
        contributions (list): list of results from PP late contributions endpoint.

    Output:
        formatted_contributions (list)
        pac_names_to_add (list)
        candidate_info_to_add (list)
    """
    get_existing_late_contributions_db_data()
    global late_contributions_df

    if isinstance(contributions, requests.models.Response):
        contributions = contributions.json().get('results', [])
    formatted_contributions = []
    pac_names_to_add = []
    candidate_info_to_add = []
    url_template = "https://docquery.fec.gov/cgi-bin/forms/{}/{}/"
    logger.debug("*** filtering new contributions")
    contributions = [
        c for c in contributions
        if c['entity_type'] == "PAC"
        and [
            c['fec_filing_id'], c['transaction_id']
        ] not in late_contributions_df.values
    ]
    for c in contributions:
        c['html_url'] = url_template.format(
            c['fec_committee_id'], c['fec_filing_id'])
        committee_name, update_name = get_committee_name(c['fec_committee_id'])
        c['committee_name'] = committee_name
        if update_name:
            pac_names_to_add.append({
                "fec_committee_id": c['fec_committee_id'],
                "committee_name": committee_name
                })

        candidate_info, update_info = get_candidate_info(c['fec_candidate_id'])
        c.update(candidate_info)
        if update_info:
            candidate_info['fec_candidate_id'] = c['fec_candidate_id']
            candidate_info_to_add.append(candidate_info)

        formatted_contributions.append(c)
    logger.debug("*** new contributions formatted")
    return formatted_contributions, pac_names_to_add, candidate_info_to_add


def upload_and_send_late_contributions(formatted_contributions,
                                       pac_names_to_add: list = [], candidate_info_to_add: list = [],
                                       trigger_email: bool = True):
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
        ["late_contributions", "pac_names", "candidate_info"]
    ):
        df = pd.DataFrame(data)
        df.to_sql(table, conn, if_exists="append", index=False)
    if trigger_email is True:
        logger.debug("*** sending new late contributions email")
        send_email(
            f"New Late Contributions for {os.getenv('TODAY', 'error')}!",
            pd.DataFrame(formatted_contributions).to_html(),
            to_email="afriedman412@gmail.com"
        )
    return
