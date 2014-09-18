#!/usr/bin/env python
# -*- coding: utf-8 -*-

from index import index_dir
from index import enhance_repo
import logging
import os
import time
import requests
import json
from multiprocessing import Pool, Process
import multiprocessing
from requests_oauthlib import OAuth1
import requests
try:
    from elasticsearch import Elasticsearch
    from elasticsearch.exceptions import ConnectionError
    from elasticsearch import helpers
except ImportError:
    logging.error("Elasticsearch is required to run this script")
    exit(1)

es_config = {}
execfile("elasticsearch.conf", es_config)
bb_config = {}
execfile("bitbucket.conf", bb_config)

def index_repos_parallel(session, es):
    logging.info("Indexing repositories")
    page_num = 1
    repos = repos_list = []
    while True:
        page_params = {"page": str(page_num)}
        try:
            response = session.get(bb_config['repos_endpoint'], params=page_params)
        except ConnectionError:
            logging.error("Connection error! at page " + str(page_num))
        if response.status_code == requests.codes.ok:
            repos = response.json()['values']
            print len(repos)
            if len(repos) == 0:
                break
            repos_bulk = []
            for repo in repos:
                repo = enhance_repo(session, repo)
                action = {}
                action.update({"_source": repo})
                action.update({"_index" : es_config['index']})
                action.update({"_type" : 'repo'})
                repos_bulk.append(action)
            helpers.bulk(es, repos_bulk)
            repos_list.append(repos)
            logging.info(str(len(repos)) + " repos were just indexed")
            page_num += 1
        elif response.status_code == 400:
            break
        else:
            logging.info("Indexing repos stopped with response code " + str(response.status_code))
            break
    for num in range(len(repos_list)):
        Process(target=parallel_index_files, args=(repos_list[num], num)).start()
        logging.info("Started process num: " + str(num))


def parallel_index_files(repos, num):
    bitbucket_config = {}
    execfile("bitbucket.conf", bitbucket_config)
    auth = OAuth1(bitbucket_config['key'], bitbucket_config['secret'])
    ## Bitbucket connection:
    s = requests.Session()
    s.auth = auth
    es_conn = Elasticsearch(es_config['host'])
    for repo in repos:
        index_files(s, es_conn, repo, num)
        logging.info(repo['full_name'] + " was indexed by " + multiprocessing.current_process().name)

def index_files(session, es, repo, num):
    filename = "logs/process" + str(num)  + ".log"
    logging.basicConfig(filename=filename, level=logging.INFO, format='%(asctime)s %(message)s')
    repo_branches = bb_config['v1_endpoint'] + repo['full_name'] + "/branches"
    response = session.get(repo_branches)
    if response.status_code == requests.codes.ok:
        branches = response.json()
        if 'branches' in repo:
            for branch in repo['branches']:
                logging.debug("traversing branch: " + branch + " in" + repo['full_name'])
                src_endpoint = bb_config['v1_endpoint'] + repo['full_name'] + "/src/" + branch + "/"
                #index_dir(session, es, repo, branch, src_endpoint, src_endpoint)
                bulk_to_index = []
                index_dir(session, es, repo, branch, src_endpoint, src_endpoint, bulk_to_index)
                if len(bulk_to_index) > 0:
                    logging.info(str(len(bulk_to_index)) + " will be indexed, repo: " + repo['full_name'])
                    helpers.bulk(es, bulk_to_index)
                else:
                    logging.info("Zero files were returned for this repo, " + repo['full_name'])
