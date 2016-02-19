#!/usr/bin/python

import argparse
import collections
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


def cleanup(query):
    s = urllib.unquote_plus(query)
    s = ' '.join(s.split(','))
    s = ' '.join(s.split())
    s = s.lower()
    return s


def generate_site_query_id(query):
    return hashlib.sha1(query).hexdigest()

def convert_feedback_format(feedback):
    os_feedback = {}
    os_feedback['sid'] = feedback['sid']
    os_feedback['site_qid'] = feedback['site_qid']
    os_feedback['doclist'] = []
    doclist = os_feedback['doclist']
    docs = feedback['docs']
    for ranking, doc in docs.iteritems():
        d = {
            'site_docid': doc['doi'],
            'team': doc['team'],
        }
        if 'clicked' in doc and doc['clicked'] == True:
            d['clicked'] = True
        doclist.append(d)
    return os_feedback

def report_feedback(feedback):
    os_feedback = convert_feedback_format(feedback)
    print os_feedback

def process_line(feedbacks, line):
    m = re.search('GET (.*) HTTP', line)
    if not m:
        return
    url = m.group(1)
    pr = urlparse.urlparse(url)
    qs = pr.query
    if not qs:
        return
    query = urlparse.parse_qs(qs)
    if 'osm' not in query:
        return
    if 'q' not in query:
        return
    if 'ossid' not in query:
        return
    if 'rank' not in query:
        return
#    print 'QUERY', query
    ossid = query['ossid'][0]
    if ossid not in feedbacks:
        q = query['q'][0]
        q = cleanup(q)
        qid = generate_site_query_id(q)

        feedbacks[ossid] = {
            'sid': ossid,
            'site_qid': qid,
            'docs': collections.OrderedDict(),
        }

    feedback = feedbacks[ossid]
    docs = feedback['docs']

    markers = query['osm'][0].split(',')
    for marker in markers:
        if not marker:
            continue
        (r, doi, t) = marker.split(':')
        if t == 'p':
            team = 'participant'
        else:
            team = 'site'
        ranking  = int(r)
        if ranking not in docs:
            docs[ranking] = {
                'doi': doi,
                'team': team,
            }

    rank = query['rank'][0]
    rank = int(rank)
    if rank not in docs:
        print 'ERROR: rank', rank, 'is not in', docs
        return
    docs[rank]['clicked'] = True

    report_feedback(feedback)


def process_log_file(date, log_file_path):
    print log_file_path

    f = subprocess.Popen(['tail', '-F', '-n', '+0', log_file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p = select.poll()
    p.register(f.stdout)
    timeout = 1 * 1000

    feedbacks = {}
    while True:
        if not p.poll(timeout) and date != datetime.date.today():
            break
        line = f.stdout.readline()
        process_line(feedbacks, line)

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
