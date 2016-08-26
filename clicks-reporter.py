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


opensearch_url = ''
opensearch_key = ''
repo_url = ''
solr_url = ''
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


def report_feedback(feedback):
    sid = feedback['sid']
    data = convert_feedback_format(feedback)
    # PUT /api/site/feedback/(key)/(sid)
    url = '/'.join([opensearch_url, 'api/site/feedback', opensearch_key, sid])
    print "URL: %s" % url
    data_json = json.dumps(data)
    #data_json = json.dumps(data, indent=4, separators=(',', ': '))
    print data_json
    retry_sleep_time = 0
    while True:
        try:
            r = requests.put(url, data=data_json)
            print r
            if r.status_code != 200:
                print r.text
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


def get_doc_text_from_repo(doi):
    global repo_url

    params = {
        'repid': 'rep1',
        'type': 'txt',
        'doi': doi,
    }

    r = requests.get(repo_url, params=params)
    if r.status_code != 200:
        print "Filaed to get text of %s from repository" % doi
        return None
    return r.text


def get_doc_from_solr(doi):
    global solr_url

    params = {
        'q': '*:*',
        'wt': 'json',
        'fq': 'doi:' + doi,
    }
    r = requests.get(solr_url, params=params)
    if r.status_code != 200:
        return None
    result = r.json()
    response = result['response']
    docs = response['docs']
    if not docs:
        print "Cannot find %s in Solr" % doi
        return None
    return docs[0]


def put_doc_to_opensearch(title, text, doi):
    doc = {
        'site_docid': doi,
        'title': title,
        'content': {
            'text': text,
        }
    }
    #print doc

    # PUT /api/site/doc/(key)/(site_docid)
    url = '/'.join([opensearch_url, 'api/site/doc', opensearch_key, doi])
    print "PUT %s" % url
    data_json = json.dumps(doc)
    try:
        r = requests.put(url, data=data_json)
        print r
    except Exception as e:
        print e
        print "Failed put %s to OpenSearch" % doi
        return False
    return True


def upload_doc(doi):
    doc = get_doc_from_solr(doi)
    if not doc:
        return False

    if 'title' in doc:
        title = doc['title']
    else:
        title = ''

    if 'abstract' in doc:
        abstract = doc['abstract']
    else:
        abstract = ''

    text = get_doc_text_from_repo(doi)
    if not text:
        text = abstract

    # Due to the 16MB limitation of BSON documents in MongoDB,
    # we need to trim down large documents in data set.
    encoded_text = text.encode('utf-8')[:16 * 1024 * 1024]
    text = encoded_text.decode('utf-8', 'ignore')

    return put_doc_to_opensearch(title, text, doi)


doc_set = set()

def upload_doc_if_necessary(doi):
    global doc_set

    if doi in doc_set:
        print "%s already in OpenSearch" % doi
        return

    print "%s not in OpenSearch database, upload it" % doi
    if upload_doc(doi):
        doc_set.add(doi)
        with open('docs.txt', 'a') as f:
            f.write("%s\n" % doi)


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
        upload_doc_if_necessary(doi)
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

    report_feedback(feedback)


def process_log_file(date, log_file_path):
    print log_file_path

    f = subprocess.Popen(['tail', '-F', '-n', '+0', log_file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p = select.poll()
    p.register(f.stdout)
    timeout = 1 * 1000

    feedbacks = {}
    while True:
        if p.poll(timeout):
            line = f.stdout.readline()
            process_line(feedbacks, line)
        elif date != datetime.date.today():
            break

    f.kill()


def process(date):
    print 'Processing', date.isoformat()

    log_file_path = logdir + 'localhost_access_log.' + date.isoformat() + '.txt'
    if not os.path.exists(log_file_path):
        print log_file_path, 'does not exist'
        return False

    process_log_file(date, log_file_path)
    return True


def main():
    global opensearch_url
    global opensearch_key
    global repo_url
    global solr_url
    global logdir
    global doc_set

    with open('docs.txt') as f:
        for line in f:
            doi = line.strip()
            doc_set.add(doi)

    config = ConfigParser.ConfigParser()
    config.read('config.ini')
    opensearch_url = config.get('opensearch', 'url')
    opensearch_key = config.get('opensearch', 'key')
    repo_url = config.get('repo', 'url')
    solr_url = config.get('solr', 'url')
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
        if not process(date) and date == datetime.date.today():
            time.sleep(60)
            continue
        date += datetime.timedelta(1)

if __name__ == "__main__":
    main()
