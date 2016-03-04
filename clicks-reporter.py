#!/usr/bin/python

import argparse
import collections
import ConfigParser
import datetime
import hashlib
import json
import os
import random
import re
import requests
import select
import subprocess
import time
import sys
import urllib
import urllib2
import urlparse
import web


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
    os_feedback = {
        'type': 'tdi',
    }
    os_feedback['sid'] = feedback['sid']
    os_feedback['site_qid'] = feedback['site_qid']
    os_feedback['doclist'] = []
    doclist = os_feedback['doclist']
    docs = feedback['docs']
    for ranking, doc in docs.iteritems():
        d = {
            'site_docid': doc['doi'],
        }
        if 'team' in doc:
            d['team'] = doc['team']
        if 'clicked' in doc and doc['clicked'] == True:
            d['clicked'] = True
        doclist.append(d)
    return os_feedback

def report_feedback(opensearch_url, key, feedback):
    sid = feedback['sid']
    data = convert_feedback_format(feedback)
    # PUT /api/site/feedback/(key)/(sid)
    url = '/'.join([opensearch_url, 'api/site/feedback', key, sid])
    print "URL: %s" % url
    data_json = json.dumps(data)
    #data_json = json.dumps(data, indent=4, separators=(',', ': '))
    print data_json
    retry_sleep_time = 0
    while True:
        try:
            r = requests.put(url, data=data_json)
            print r
        except requests.exceptions.ConnectionError as e:
            if retry_sleep_time == 0:
                retry_sleep_time =  1
                print e
                sys.stdout.write('Retry...')
                sys.stdout.flush()
            elif retry_sleep_time <= 32:
                retry_sleep_time *= 2
                sys.stdout.write('.')
                sys.stdout.flush()

            time.sleep(retry_sleep_time * 60)
            continue
        break

def process_line(opensearch_url, key, feedbacks, line):
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
        ranking  = int(r)
        if ranking not in docs:
            doc = {
                'doi': doi,
            }
            if t == 'p':
                doc['team'] = 'participant'
            elif t == 's':
                doc['team'] = 'site'

            docs[ranking] = doc

    rank = query['rank'][0]
    rank = int(rank)
    if rank not in docs:
        print 'ERROR: rank', rank, 'is not in', docs
        return
    docs[rank]['clicked'] = True

    report_feedback(opensearch_url, key, feedback)


def process_log_file(opensearch_url, key, date, log_file_path):
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
        process_line(opensearch_url, key, feedbacks, line)

    f.kill()


def process(opensearch_url, key, logdir, date):
    print 'Processing', date.isoformat()

    log_file_path = logdir + 'localhost_access_log.' + date.isoformat() + '.txt'
    if not os.path.exists(log_file_path):
        print log_file_path, 'does not exist'
        return False

    process_log_file(opensearch_url, key, date, log_file_path)
    return True


def main():
    config = ConfigParser.ConfigParser()
    config.read('config.ini')
    opensearch_url = config.get('opensearch', 'url')
    opensearch_key = config.get('opensearch', 'key')
    logdir = config.get('tomcat', 'log-dir')

    parser = argparse.ArgumentParser(description='"Parse CiteSeerX access log and report user clicks to TREC OpenSearch')
    parser.add_argument('-s', '--date', type=str, help='Start from date in format YYYY-MM-DD, default to today')

    args = parser.parse_args()
    if args.date:
        date = datetime.datetime.strptime(args.date, '%Y-%m-%d').date()
    else:
        date = datetime.date.today()

    if not os.path.exists(logdir):
        print logdir, 'does not exist'
        return

    while date <= datetime.date.today():
        if not process(opensearch_url, opensearch_key, logdir, date) and date == datetime.date.today():
            time.sleep(60)
            continue
        date += datetime.timedelta(1)

if __name__ == "__main__":
    main()
