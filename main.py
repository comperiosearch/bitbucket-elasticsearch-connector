#!/usr/bin/env python
# -*- coding: utf-8 -*-

from index import index_repos
from update import update_repos
from pindexer import index_repos_parallel
from update import test
import logging
import os
import time
import requests
import json
import getpass
import sys, signal
import atexit

try:
    from argparse import ArgumentParser
except ImportError:
    logging.error("argparse is required to run this script")
    exit(1)
try:
    from elasticsearch import Elasticsearch
except ImportError:
    logging.error("Elasticsearch is required to run this script")
    exit(1)
try:
    from requests_oauthlib import OAuth1
except ImportError:
    logging.error("requests-oauthlib is required to run this script")
    exit(1)

lastrun = None

def check_es_configs(config):
    if 'host' not in config.keys():
        raise KeyError("Elasticsearch host is missing in elasticsearch.conf")
        exit(1)
    if 'index' not in config.keys():
        raise KeyError("Elasticsearch index is missing in elasticsearch.conf")
        exit(1)

def check_bitbucket_configs(config):
    if 'key' not in config.keys():
        raise KeyError("Key is missing in bitbucket.conf")
        exit(1)
    if 'secret' not in config.keys():
        raise KeyError("secret is missing in bitbucket.conf")
        exit(1)
    if 'v2_endpoint' not in config.keys():
        raise KeyError("v2_endpoint is missing in bitbucket.conf")
        exit(1)

def last_run():
    '''
    reads from .bitbucketHistory when bitbucket content was last indexed
    '''
    if os.path.isfile(".bitbucketHistory"):
        sincestr = open(".bitbucketHistory").read()
        since = time.strptime(sincestr, '%Y-%m-%dT%H:%M:%S')
    else:
        since = 0
    return since


def write_history(lastrun):
    '''
    writes the timestamp when bitbucket content was last indexed or updated
    uses a file named '.bitbucketHistory' to save the timestamp for next run
    '''
    if lastrun:
        history_file = open(".bitbucketHistory", 'w')
        history_file.write(lastrun)
        history_file.close()

def init_elasticsearch():
    config = {}
    execfile("elasticsearch.conf", config)
    check_es_configs(config)
    try:
        es_conn = Elasticsearch(config['host'], max_retries=8)
    except:
        logging.error("elasticsearch is not running")
        exit(1)
    if not es_conn.indices.exists(index=config['index']):
        index_settings = json.loads(open("index-settings.json", "r").read())
        es_conn.indices.create(index=config['index'], body=index_settings)

        ## read mapping(s)
        file_mapping = json.loads(open("file_mapping.json", "r").read())
        es_conn.indices.put_mapping(index=config['index'], doc_type='file', body=file_mapping)
    return es_conn


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    bitbucket_config = {}
    execfile("bitbucket.conf", bitbucket_config)
    check_bitbucket_configs(bitbucket_config)
    auth = OAuth1(bitbucket_config['key'], bitbucket_config['secret'])

    ## Bitbucket connection:
    s = requests.Session()
    s.auth = auth

    ## elasticsearch connection:
    es_conn = init_elasticsearch()

    argparser = ArgumentParser(description=__doc__)
    argparser.add_argument('index', default='index',
                           help='index, update or pindex')
    args = argparser.parse_args()
    if args.index == "index":
        lastrun = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime())
        index_repos(s, es_conn)
        write_history(lastrun)
    elif args.index == "update":
        lastrun = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime())
        since = last_run()
        update_repos(s, es_conn, since)
        write_history(lastrun)
    elif args.index == "pindex":
        lastrun = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime())
        index_repos_parallel(s, es_conn)
        write_history(lastrun)
    elif args.index == "test":
        test(s)
    else:
        raise ValueError("Unknown mode. Please use one of the following:\n index\n update\n pindex")

atexit.register(write_history, lastrun)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        #lastrun = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime())
        print lastrun
        write_history(lastrun)    
        sys.exit()
