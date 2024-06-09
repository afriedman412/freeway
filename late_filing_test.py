from src.src import format_contributions
from src.config import BASE_URL
from src.utilities import make_conn
import os
import pandas as pd
import requests
from tqdm import tqdm
import json

zzz = json.load(open("test_contributions.json"))

a, b, c = format_contributions(zzz)
print("LENGHTS:", len(a), len(b), len(c))

if a[0]['committee_name'] != 'WILLIAM TIMMONS FOR CONGRESS':
    print("...", a[0]['committee_name'])
if b[0] != ('C00496760', 'AMODEI FOR NEVADA'):
    print("...", b[0])
if c[0] != {'candidate_name': 'Sewell, Briana', 'office': 'house', 'state': 'VA', 'district': '07', 'fec_candidate_id': 'H4VA07259'}:
    print("...", c[0])

print("*** TEST OVER ***")