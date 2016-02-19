#!/usr/bin/python

import argparse
import datetime
import hashlib
import json
import os
import random
import re
import select
import subprocess
import sys
import urllib
import urllib2
import urlparse
import web

#SOLR_URL='http://localhost:9000/solr/citeseerx/select'
SOLR_URL='http://csxindex03.ist.psu.edu:8080/solr/citeseerx/select'
OPENSEARCH_URL='http://localhost:5000'

urls = (
  '/select', 'select'
)


def cleanup(self, query):
    s = urllib.unquote_plus(query)
    s = ' '.join(s.split(','))
    s = ' '.join(s.split())
    s = s.lower()
    return s


def generate_site_query_id(self, query):
    return hashlib.sha1(query).hexdigest()


def process_line(line):
    m = re.search('GET (.*) HTTP', line)
    if not m:
        return None
    url = m.group(1)
    pr = urlparse.urlparse(url)
    qs = pr.query
    if not qs:
        return None
    query = urlparse.parse_qs(qs)
    if 'osm' not in query:
        return None
    return query


def process_log_file(date, log_file_path):
    print log_file_path

    f = subprocess.Popen(['tail', '-F', '-n', '+0', log_file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p = select.poll()
    p.register(f.stdout)
    timeout = 1 * 1000

    while True:
        if not p.poll(timeout) and date != datetime.date.today():
            break
        line = f.stdout.readline()
        query = process_line(line)
        if not query:
            continue
        print query
    f.kill()


def process(logdir, date):
    print 'Processing', date.isoformat()

    log_file_path = logdir + 'localhost_access_log.' + date.isoformat() + '.txt'
    if not os.path.exists(log_file_path):
        print log_file_path, 'does not exist'
        return

    process_log_file(date, log_file_path)


def main():
    parser = argparse.ArgumentParser(description='"Parse CiteSeerX access log and report user clicks to TREC OpenSearch')
    parser.add_argument('-s', '--date', type=str, help='Start from date in format YYYY-MM-DD, default to today')
    parser.add_argument('-k', '--key', type=str, help='Provide a user key')
    parser.add_argument('-p', '--port', help='Port number of OpenSearch API server')
    parser.add_argument('-d', '--logdir', required=True, help='Tomcat logs directory')

    args = parser.parse_args()
    if args.date:
        date = datetime.datetime.strptime(args.date, '%Y-%m-%d').date()
    else:
        date = datetime.date.today()
    KEY = args.key
    #port = int(args.port)

    if not os.path.exists(args.logdir):
        print args.logdir, 'does not exist'
        return

    while date <= datetime.date.today():
        process(args.logdir, date)
        date += datetime.timedelta(1)

if __name__ == "__main__":
    main()
