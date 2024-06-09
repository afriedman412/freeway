GOV_BASE_URL = "https://api.open.fec.gov/v1/"
DT_FORMAT = "%Y-%m-%d"
CYCLE = "2024"
BASE_URL = f"https://api.propublica.org/campaign-finance/v1/{CYCLE}"
RECURSIVE_SLEEP_TIME = 1
RETRY_SLEEP_TIME = 3
RETRIES = 5
DT_FORMAT = "%Y-%m-%d"
IE_TABLE = "fiu_pp"
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
