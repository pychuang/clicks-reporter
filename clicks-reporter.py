#!/usr/bin/python

import argparse
import datetime
import hashlib
import json
import random
import re
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


def parse_log_file(log_file_path):
    print log_file_path

    with open(log_file_path) as f:
        for line in f:
            m = re.search('GET (.*) HTTP', line)
            if not m:
                continue
            url = m.group(1)
            pr = urlparse.urlparse(url)
            qs = pr.query
            if not qs:
                continue
            query = urlparse.parse_qs(qs)
            if 'osm' not in query:
                continue
            print query


def main():
    logs_dir = '/usr/local/tomcat-solr/logs/'
    date_str = datetime.date.today().isoformat()
    log_file_path = logs_dir + 'localhost_access_log.' + date_str + '.txt'
    parse_log_file(log_file_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='"Parse CiteSeerX access log and report user clicks to TREC OpenSearch')
    parser.add_argument('-k', '--key', type=str, help='Provide a user key.')
    parser.add_argument('-p', '--port', help='Port number of OpenSearch API server')
#    parser.add_argument('-d', '--logdir', required=True, help='Tomcat logs directory')

    args = parser.parse_args()
    KEY = args.key
    #port = int(args.port)
    main()
