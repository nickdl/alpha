"""Fetches & stores the current S&P 500 symbols."""

import requests
from bs4 import BeautifulSoup
import json


URL = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'

r = requests.get(URL)
soup = BeautifulSoup(r.text, 'html.parser')

table = soup.find_all('table')[0]
rows = table.find_all('tr')

snp_list = []
for row in rows[1:]:
    snp_list.append(row.find('a').getText())

print(snp_list)

with open('symbols.json', 'w') as f:
    json.dump(sorted(snp_list), f)
