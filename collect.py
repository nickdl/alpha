"""Data collection & validation functions.

Every API call returns data that span up to two weeks back,
so this script is intended to run in a shorter interval (eg week)
so that the partials that are collected can be concatenated into a continuous dataset.

A folder data/partials_{X} is expected for the execution of the script for WEEK = X
Also an API key for Alpha Vantage has to be exported as an environment variable.
"""

import requests
import pandas as pd
import json
import os


WEEK = 20
DATA_DIR = 'data/'
PARTIALS_DIR = DATA_DIR + 'partials_' + str(WEEK) + '/'
API_KEY = os.environ['API_KEY']
URL = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY' + \
      '&symbol=%s&apikey=%s&datatype=csv&outputsize=full&interval=1min'


with open('symbols.json', 'r') as f:
    SYMBOLS = json.load(f)


def collect():
    for symbol in SYMBOLS:
        r = requests.get(URL % (symbol, API_KEY))
        print(symbol, r.status_code)
        with open(PARTIALS_DIR + symbol, 'w') as f:
            f.write(r.text)


def validate(retry=False):
    incomplete = []
    error = []
    for symbol in SYMBOLS:
        try:
            frame = pd.read_csv(PARTIALS_DIR + symbol)
            if frame.shape[0] < 10:
                error.append(symbol)
            elif frame.shape[0] < 2500:
                incomplete.append(symbol)
        except Exception:
            error.append(symbol)
    print('incomplete:', len(incomplete), incomplete)
    print('error:', len(error), error)
    if retry:
        for symbol in error + incomplete:
            r = requests.get(URL % (symbol, API_KEY))
            print(symbol, r.status_code)
            with open(PARTIALS_DIR + symbol, 'w') as f:
                f.write(r.text)


# collect()
# validate(retry=True)
# validate(retry=False)
