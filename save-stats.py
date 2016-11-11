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


def process_line(line):
    m = re.search('GET (.*) HTTP', line)
    if m:
        url = m.group(1)
        pr = urlparse.urlparse(url)
        qs = pr.query
        if qs:
            if 'search' in pr.path:
                return 'SEARCH'
            else:
                query = urlparse.parse_qs(qs)
                if 'osm' in query:
                    return 'FEEDBACK'

    return None

def process_log_file(date, log_file_path, writer):
    print log_file_path

    f = subprocess.Popen(['tail', '-F', '-n', '+0', log_file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p = select.poll()
    p.register(f.stdout)
    timeout = 1 * 1000

    searches = 0
    feedbacks = 0
    while True:
        if p.poll(timeout):
            line = f.stdout.readline()
            result = process_line(line)
            if result == 'SEARCH':
                searches += 1
            elif result == 'FEEDBACK':
                feedbacks += 1

        elif date != datetime.date.today():
            break

    f.kill()
    writer.writerow([date.isoformat(), searches, feedbacks])


def process(date, writer):
    print 'Processing', date.isoformat()

    log_file_path = logdir + 'localhost_access_log.' + date.isoformat() + '.txt'
    if not os.path.exists(log_file_path):
        print log_file_path, 'does not exist'
        return False

    process_log_file(date, log_file_path, writer)
    return True


def main():
    global logdir

    config = ConfigParser.ConfigParser()
    config.read('config.ini')
    logdir = config.get('tomcat', 'log-dir')

    parser = argparse.ArgumentParser(description='"Parse CiteSeerX access log and save statistics')
    parser.add_argument('-s', '--date', type=str, help='Start from date in format YYYY-MM-DD, default to today')

    args = parser.parse_args()
    if args.date:
        date = datetime.datetime.strptime(args.date, '%Y-%m-%d').date()
    else:
        date = datetime.date.today()

    if not os.path.exists(logdir):
        print logdir, 'does not exist'
        return

    f = open('stats.csv', 'wb')
    writer = csv.writer(f)
    writer.writerow(['date', 'searches', 'feedbacks'])

    while date < datetime.date.today():
        if not process(date, writer) and date == datetime.date.today():
            time.sleep(60)
            continue
        date += datetime.timedelta(1)


if __name__ == "__main__":
    main()
