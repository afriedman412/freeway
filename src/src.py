import os
from time import sleep

import pandas as pd
import requests
from flask import render_template


from .logger import logger
from .config import RECURSIVE_SLEEP_TIME, RETRY_SLEEP_TIME, BASE_URL, IE_TABLE, DATA_COLUMNS, DT_FORMAT
from .utilities import load_data, query_api, get_today, query_table, make_conn, send_email
from typing import Callable, List, Dict, Any


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
        r = query_api(url, offset=offset, api_type=api_type, per_page=increment, params=params)
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
                logger.debug(f"JSONDecodeError ... retrying in {RETRY_SLEEP_TIME}")
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


def load_results(url: str, title_params: dict={}, limit: str=None):
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


def verify_r(r: requests.Response):
    assert r.status_code == 200, f"bad status code: {r.status_code}"
    assert r.json(), "bad response json"
    assert r.json()['results'], "no results loaded"
    return len(r.json()['results'])


def save_data(data: pd.DataFrame):
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
        new_today_transactions_df.to_sql(IE_TABLE, con=engine, if_exists="append")
        if trigger_email:
                send_email(
                    f"New Independent Expenditures for {os.getenv('TODAY', 'error')}!",
                    new_today_transactions_df[DATA_COLUMNS].to_html()
                )
    return new_today_transactions_df


def get_late_contributions(**kwargs):
    if kwargs.get("candidate_id"):
        url = os.path.join(BASE_URL, "candidates", kwargs['candidate_id'], "48hour.json")
    elif kwargs.get("committe_id"):
        url = os.path.join(BASE_URL, "committees", kwargs['committe_id'], "48hour.json")
    elif kwargs.get("date"):
        year, month, day = kwargs['date'].split("-")
        url = os.path.join(BASE_URL, "contributions", "48hour", year, month, f"{day}.json")
    else:
        year, month, day = get_today().split("-")
        url = os.path.join(BASE_URL, "contributions", "48hour", year, month, f"{day}.json")
    if "return_url" in kwargs and kwargs['return_url']:
        return url
    r = recursive_query(url)
    return r


def get_existing_late_contributions_db_data():
    global ie_df, pac_names_df, candidate_info_df
    conn = make_conn()
    ie_df = pd.read_sql("select fec_candidate_id, candidate_name, office, state, district, fec_committee_id, fec_committee_name from fiu_pp", conn)
    ie_df.drop_duplicates(inplace=True)
    pac_names_df = pd.read_sql("select * from pac_names", conn)
    candidate_info_df = pd.read_sql("select * from candidate_info", conn)
    late_transactions_df = pd.read_sql("select fec_filing_id, transaction_id from late_transactions", conn)
    return


def get_committee_name(committee_id):
    global pac_names_df
    global ie_df
    update = False
    try:
        name = pac_names_df.query('fec_committee_id==@committee_id').iloc[0]['committee_name']
    except IndexError:
        update = True
        try:
            name = ie_df.query('fec_committee_id==@committee_id').iloc[0]['fec_committee_name']
        except IndexError:
            url = os.path.join(BASE_URL, 'committees', committee_id) + ".json"
            r = query_api(url)
            try:
                name = r.json()['results'][0].get('name', 'NAME MISSING')
            except ValueError:
                name = "NAME QUERY ERROR"
    return name, update


def get_candidate_info(candidate_id):
    global candidate_info_df
    global ie_df
    assert len(ie_df) == 1842
    update = False
    try:
        candidate_info = candidate_info_df.query('fec_candidate_id==@candidate_id').to_dict('records')[0]
    except IndexError:
        update = True
        try:
            candidate_info = ie_df.query('fec_candidate_id==@candidate_id')[['candidate_name', 'office', 'state', 'district']].to_dict('records')[0]
        except IndexError:
            url = os.path.join(BASE_URL, 'candidates', candidate_id) + ".json"
            r = query_api(url)
            try:
                _, _, state, office, district = r.json()['results'][0]['district'].split("/")
                district = district.replace(".json", "")
                name = r.json()['results'][0]['display_name']
            except (KeyError, ValueError):
                name, state, office, district = ("QE", None, None, None)
            candidate_info = dict(zip(
                ['fec_candidate_id', 'candidate_name', 'office', 'state', 'district'],
                [candidate_id, name, office, state, district]
            ))
    if isinstance(candidate_info, list):
        candidate_info = candidate_info[0]
    print(candidate_info)
    return candidate_info, update


def filter_and_format_late_contributions(contributions):
    get_existing_late_contributions_db_data()
    global late_transactions_df
    if isinstance(contributions, requests.models.Response):
        contributions = contributions.json().get('results', [])
    formatted_contributions = []
    pac_names_to_add = []
    candidate_info_to_add = []
    url_template = "https://docquery.fec.gov/cgi-bin/forms/{}/{}/"
    contributions = [
        c for c in contributions 
        if c['entity_type'] == "PAC"
        and [
            c['fec_filing_id'], c['transaction_id']
            ] not in late_transactions_df.values
    ]
    for c in contributions:
        c['html_url'] = url_template.format(c['fec_committee_id'], c['fec_filing_id'])
        committee_name, update_name = get_committee_name(c['fec_committee_id'])
        c['committee_name'] = committee_name
        if update_name:
            pac_names_to_add.append((c['fec_committee_id'], committee_name))

        candidate_info, update_info = get_candidate_info(c['fec_candidate_id'])
        c.update(candidate_info)
        if update_info:
            candidate_info['fec_candidate_id'] = c['fec_candidate_id']
            candidate_info_to_add.append(candidate_info)

        formatted_contributions.append(c)

    return formatted_contributions, pac_names_to_add, candidate_info_to_add


def upload_and_send_late_contributions(formatted_contributions, pac_names_to_add, candidate_info_to_add, trigger_email=True):
    conn = make_conn()
    pd.DataFrame(formatted_contributions).to_sql('late_transactions', conn, if_exists='append', index=False)
    pd.DataFrame(pac_names_to_add, columns=['fec_committee_id', 'committee_name']).drop_duplicates().to_sql('pac_names', conn, if_exists="append", index=False)
    pd.DataFrame(candidate_info_to_add).drop_duplicates().to_sql('candidate_info', conn, if_exists="append", index=False)
    if trigger_email:
        send_email(
                    f"New Late Contributions for {os.getenv('TODAY', 'error')}!",
                    formatted_contributions.to_html(),
                    to_email="afriedman412@gmail.com"
                )
    return