#/usr/bin/python
from bs4 import BeautifulSoup

import argparse
import urllib3

parser = argparse.ArgumentParser(description="Send an e-mail alart when an item is found on Amazon.com")
parser.add_argument('--name', metavar="SEARCH_TERM", required=True)
parser.add_argument('--email', metavar="ADDRESS", required=True)

args = vars(parser.parse_args())
search_term = args['name']
recipient = args['email']

search_url = "http://www.amazon.com/s/ref=nb_sb_noss?url=search-alias%3Daps&field-keywords=" + search_term + "+"

http = urllib3.PoolManager()
html = http.request('GET', search_url).data

soup = BeautifulSoup(html, 'lxml')


