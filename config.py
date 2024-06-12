import os

DEBUG = False
TESTING = False
GOV_BASE_URL = "https://api.open.fec.gov/v1/"
DT_FORMAT = "%Y-%m-%d"
CYCLE = 2024
BASE_URL = f"https://api.propublica.org/campaign-finance/v1/{CYCLE}"
RECURSIVE_SLEEP_TIME = 1
RETRY_SLEEP_TIME = 3
RETRIES = 5
DT_FORMAT = "%Y-%m-%d"
EMAIL_FROM = "afriedman412@gmail.com"
EMAILS_TO = ["david@readsludge.com", "donny@readsludge.com"] + [EMAIL_FROM]

DATA_COLUMNS = [
    'fec_committee_name',
    'fec_committee_id',
    'candidate_name',
    'office',
    'state',
    'district',
    'amount',
    'date',
    'date_received',
    'dissemination_date',
    'purpose',
    'payee',
    'support_or_oppose',
    'transaction_id'
]

IE_TABLE = "fiu_pp"
LATE_CONTRIBUTIONS_TABLE = "late_contributions"
PAC_NAMES_TABLE = "pac_names"
CANDIDATE_INFO_TABLE = "candidate_info"

if os.getenv("FLASK_ENV") == 'dev':
    DEBUG = True

if os.getenv("FLASK_ENV") == 'test':
    TESTING = True
    LATE_CONTRIBUTIONS_TABLE = "late_contributions_testo"
    PAC_NAMES_TABLE = "pac_names_testo"
    CANDIDATE_INFO_TABLE = "candidate_info_testo"
