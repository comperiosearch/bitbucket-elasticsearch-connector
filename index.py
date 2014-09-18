#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import time
import requests
import json
from requests.exceptions import ConnectionError

try:
    from elasticsearch import Elasticsearch
    from elasticsearch import helpers
except ImportError:
    logging.error("Elasticsearch is required to run this script")
    exit(1)

es_config = {}
bb_config = {}
execfile("elasticsearch.conf", es_config)
execfile("bitbucket.conf", bb_config)

bulk_to_index = []

def get_watchers(session, repo):
    '''
    gets a list of the repo's watchers //unused
    '''
    page_len = 30
    page_params = {"pagelen": str(page_len)}
    try:
        response = session.get(repo['links']['watchers']['href'], params=page_params)
    except ConnectionError:
        logging.error("Connection error, will skip " + repo['links']['watchers']['href'])
    if response.status_code == requests.codes.ok and 'values' in response.json():
        watchers = response.json()['values']
        repo.update({unicode("watchers") : watchers})
    else:
        logging.info("No one is watching " + repo['full_name'])
    return repo

def enhance_repo(session, repo):
    '''
    The repositories endpoint in bitbucket API doesn't return who is watching
    the repo and what branches it has, so this function will get such
    information and add it to the repo JSON object
    '''
    # get repo's branches, note that in order to get the branches we had to use
    # the api version 1
    repo_branches = bb_config['v1_endpoint'] + repo['full_name'] + "/branches"
    branches_response = session.get(repo_branches)
    if branches_response.status_code == requests.codes.ok:
        branches = branches_response.json()
        repo.update({unicode("branches") : branches})
    return repo

def index_repos(session, es):
    '''
    This function requests 10 repos at a time, indexes them and their files then
    asks the bitbucket API for another 10 repos
    '''
    logging.info("Indexing repositories")
    page_num = 1
    while True:
        page_params = {"page": str(page_num)}
        try:
            response = session.get(bb_config['repos_endpoint'], params=page_params)
        except ConnectionError:
            logging.error("Connection error! at page " + str(page_num))
        if response.status_code == requests.codes.ok:
            repos = response.json()['values']
            if len(repos) == 0:
                break
            for repo in repos:
                # Add watchers and branches to repos
                repo = enhance_repo(session, repo)
                es.index(index=es_config['index'], doc_type="repo", body=repo)
                index_files(session, es, repo)
            logging.info(str(len(repos)) + " repos were just indexed")
            page_num += 1
        elif response.status_code == 400:
            break
        else:
            logging.info("Indexing repos stopped with response code " + str(response.status_code))
            break

def index_files(session, es, repo):
    '''
    Given a repo, index the files in each branch
    '''
    repo_branches = bb_config['v1_endpoint'] + repo['full_name'] + "/branches"
    # Get all branches in this repo
    response = session.get(repo_branches)
    if response.status_code == requests.codes.ok:
        branches = response.json()
        if 'branches' in repo:
            for branch in repo['branches']:
                logging.info("Started indexing: " + repo['full_name'] + "/" + branch)
                src_endpoint = bb_config['v1_endpoint'] + repo['full_name'] + "/src/" + branch + "/"
                # index_dir will recursively index the repos directories and files
                bulk_to_index = []
                index_dir(session, es, repo, branch, src_endpoint, src_endpoint, bulk_to_index)
                if len(bulk_to_index) > 0:
                    logging.info(str(len(bulk_to_index)) + " will be indexed, repo: " + repo['full_name'])
                    helpers.bulk(es, bulk_to_index)
                else:
                    logging.info("Zero files were returned for this repo, " + repo['full_name'])
                

def index_dir(session, es, repo, branch, src_endpoint, endpoint, bulk_index):
    '''
    recursively indexes a given repo's files in a given branch
    '''
    files_to_index = []
    response = session.get(endpoint)
    if response.status_code == requests.codes.ok:
        src = response.json()
        if 'files' in src:
            for f in src['files']:
                if 'size' in f and f['size'] < es_config['size_limit']:
                    filedata_response = session.get(src_endpoint + f['path'])
                    if filedata_response.status_code == requests.codes.ok:
                        filedata = filedata_response.json()
                        f.update({unicode("filedata") : filedata})
                else:
                    logging.info(repo['full_name'] + branch + f['path'] + " is too large, only its metadata will be indexed")
                f.update({unicode("branch") : branch})
                f.update({unicode("repo") : repo['full_name']})
                f.update({unicode("repolanguage") : repo['language']})
                f.update({unicode("link") : repo['links']['html']['href'] + "/src/" + branch + "/" + f['path']})
                f.update({unicode("extension") : f['path'].split('/')[-1].split('.')[-1]})
                f.update({"collapse_id" : repo['full_name'] + "/" + f['path']})
                #es.index(index=es_config['index'], doc_type="file", body=f)
                action = {}
                action.update({"_source": f})
                action.update({"_index" : es_config['index']})
                action.update({"_type" : 'file'})
                bulk_index.append(action)
        if 'directories' in src:
            for directory in src['directories']:
                index_dir(session, es, repo, branch, src_endpoint, endpoint + directory + "/", bulk_index)
    else:
        logging.error(endpoint + str(response.status_code) + response.text)
