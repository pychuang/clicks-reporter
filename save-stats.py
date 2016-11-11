#!/usr/bin/python

import argparse
import collections
import ConfigParser
import csv
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
    s = s.lower()
    s = ' '.join(s.split(','))
    s = ' '.join(s.split('"'))
    s = ' '.join(s.split(';'))
    s  = re.sub(r"'([^s]|$)", r'\1', s)
    s  = re.sub(r"(^|\D)\.+", r"\1 ", s)
    s = ' '.join(s.split())
    return s


def process_line(line, top_queries, top200_queries):
    m = re.search('GET (.*) HTTP', line)
    if m:
        url = m.group(1)
        pr = urlparse.urlparse(url)
        qs = pr.query
        if qs:
            query = urlparse.parse_qs(qs)
            if 'search' in pr.path:
                if 'q' in query:
                    q = query['q'][0]
                    q = cleanup(q)
                    if q in top200_queries:
                        return 'TOP200'
                    elif q in top_queries:
                        return 'TOP'
                return 'QUERY'
            else:
                if 'osm' in query:
                    return 'FEEDBACK'

    return None

def process_log_file(date, log_file_path, writer, top_queries, top200_queries):
    print log_file_path

    f = subprocess.Popen(['tail', '-F', '-n', '+0', log_file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p = select.poll()
    p.register(f.stdout)
    timeout = 1 * 1000

    queries = 0
    matched_top200 = 0
    matched_top = 0
    feedbacks = 0
    while True:
        if p.poll(timeout):
            line = f.stdout.readline()
            result = process_line(line, top_queries, top200_queries)
            if result == 'TOP200':
                queries += 1
                matched_top200 += 1
                matched_top += 1
            elif result == 'TOP':
                queries += 1
                matched_top += 1
            elif result == 'QUERY':
                queries += 1
            elif result == 'FEEDBACK':
                feedbacks += 1

        elif date != datetime.date.today():
            break

    f.kill()
    writer.writerow([date.isoformat(), queries, matched_top, matched_top200, feedbacks])


def process(date, writer, top_queries, top200_queries):
    print 'Processing', date.isoformat()

    log_file_path = logdir + 'localhost_access_log.' + date.isoformat() + '.txt'
    if not os.path.exists(log_file_path):
        print log_file_path, 'does not exist'
        return False

    process_log_file(date, log_file_path, writer, top_queries, top200_queries)
    return True


def load_query_list(infile):
    top_queries = set()
    top200_queries = set()
    with open(infile) as f:
        for line in f:
            q = line.strip()
            top_queries.add(q)
            if len(top200_queries) < 200:
                top200_queries.add(q)
    return top_queries, top200_queries


def main():
    global logdir

    config = ConfigParser.ConfigParser()
    config.read('config.ini')
    logdir = config.get('tomcat', 'log-dir')

    parser = argparse.ArgumentParser(description='"Parse CiteSeerX access log and save statistics')
    parser.add_argument('-i', '--infile', required=True, help='query list text file')
    parser.add_argument('-s', '--date', type=str, help='Start from date in format YYYY-MM-DD, default to today')

    args = parser.parse_args()
    if args.date:
        date = datetime.datetime.strptime(args.date, '%Y-%m-%d').date()
    else:
        date = datetime.date.today()

    if not os.path.exists(logdir):
        print logdir, 'does not exist'
        return

    top_queries, top200_queries = load_query_list(args.infile)

    f = open('stats.csv', 'wb')
    writer = csv.writer(f)
    writer.writerow(['Date', 'Queries', 'Matched Top 1000 Queries' , 'Matched Top 200 Queries','User Clicks'])

    while date < datetime.date.today():
        if not process(date, writer, top_queries, top200_queries) and date == datetime.date.today():
            time.sleep(60)
            continue
        date += datetime.timedelta(1)


if __name__ == "__main__":
    main()
