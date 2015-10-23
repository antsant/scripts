#/usr/bin/python
import argparse
import boto
import boto.ses
import os
import re
import sys
import urllib3

from boto.dynamodb2.exceptions import ItemNotFound
from boto.dynamodb2.fields import HashKey, RangeKey
from boto.dynamodb2.table import Table
from boto.ses.connection import SESConnection
from bs4 import BeautifulSoup

parser = argparse.ArgumentParser(description="Send an e-mail alart when an item is found on Amazon.com")
parser.add_argument('--name', metavar="SEARCH_TERM", required=True)
parser.add_argument('--email', metavar="ADDRESS", required=True)

args = vars(parser.parse_args())
search_term = args['name']
recipient = args['email']

searches_notified_table = Table('SearchesNotified', schema=[
                HashKey('Recipient'),
                RangeKey('SearchTerm')
        ])
try:
    searches_notified_table.get_item(Recipient=recipient, SearchTerm=search_term)
    print("Already notified " + recipient + " for the search term " + search_term + ". Quitting.")
    sys.exit()
except ItemNotFound:
    pass

search_url = "http://www.amazon.com/s/ref=nb_sb_noss?url=search-alias%3Daps&field-keywords=" + search_term + "+"

http = urllib3.PoolManager()
html = http.request('GET', search_url).data

soup = BeautifulSoup(html, 'lxml')
results = soup.find(id="s-results-list-atf")
email_body = None
for result in results.find_all('h2', string=re.compile(search_term)):
    link = result.parent['href']
    email_body = "Item link: " + link + "\n\nSearch page: " + search_url
    continue    
 
if email_body is not None:
    smtp_conn = boto.ses.connect_to_region('us-west-2')
    smtp_conn.send_email(
	'anthony.a.santos@gmail.com',
	'Found ' + search_term + ' on Amazon!',
	email_body,
	[recipient])
    searches_notified_table.put_item(data={
		'Recipient': recipient,
		'SearchTerm': search_term
	})
