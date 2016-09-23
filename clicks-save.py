#!/usr/bin/python

import argparse
import collections
import ConfigParser
import datetime
import hashlib
import json
import os
import re
import requests
import select
import subprocess
import time
import sys
import urllib
import urlparse


logdir = ''


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
        ranking  = int(r)
        if not doi:
            return
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

    return convert_feedback_format(feedback)


def process_log_file(date, log_file_path):
    print log_file_path

    f = subprocess.Popen(['tail', '-F', '-n', '+0', log_file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p = select.poll()
    p.register(f.stdout)
    timeout = 1 * 1000

    feedbacks = {}
    converted_feedbacks = []
    while True:
        if p.poll(timeout):
            line = f.stdout.readline()
            data = process_line(feedbacks, line)
            if data:
                converted_feedbacks.append(data)
        elif date != datetime.date.today():
            break

    f.kill()
    return converted_feedbacks


def process(date):
    print 'Processing', date.isoformat()

    log_file_path = logdir + 'localhost_access_log.' + date.isoformat() + '.txt'
    if not os.path.exists(log_file_path):
        print log_file_path, 'does not exist'
        return False

    feedbacks = process_log_file(date, log_file_path)
    if feedbacks:
        json_file_path = 'citeseerx.clicks' + date.isoformat() + '.json'
        print "Write %d feedbacks to %s ..." % (len(feedbacks), json_file_path)
        data = json.dumps(feedbacks)
        f = open(json_file_path, 'w')
        f.write(data)
        f.close()

    return True


def main():
    global logdir

    config = ConfigParser.ConfigParser()
    config.read('config.ini')
    logdir = config.get('tomcat', 'log-dir')

    parser = argparse.ArgumentParser(description='"Parse CiteSeerX access log and save user clicks as JSON files')
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
        if not process(date) and date == datetime.date.today():
            time.sleep(60)
            continue
        date += datetime.timedelta(1)

if __name__ == "__main__":
    main()
