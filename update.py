#!/usr/bin/env python
# -*- coding: utf-8 -*-

from index import index_files
from index import enhance_repo
import logging
import os
import time
import requests
import json

try:
    from elasticsearch import Elasticsearch
except ImportError:
    logging.error("Elasticsearch is required to run this script")
    exit(1)

es_config = {}
bb_config = {}
execfile("elasticsearch.conf", es_config)
execfile("bitbucket.conf", bb_config)

def update_repos(session, es, since):
    page_num = 1
    updated_repos = []
    size = 0
    while True:
        page_params = {"page": str(page_num)}
        repos = session.get(bb_config['repos_endpoint'], params=page_params).json()
        if 'values' not in repos:
            if (size == 0) or (page_num * 10 < size):
                logging.error("Error in calling " + bb_config['repos_endpoint'])
                logging.error("Please check your bitbucket.conf file")
                exit(1)
            logging.info("Checked all repos")
            break
        else:
            size = repos['size']
            repos = repos['values']

        for repo in repos:
            repo_updated_on = time.strptime(repo["updated_on"].split(".")[0], '%Y-%m-%dT%H:%M:%S')
            if (since < repo_updated_on):
                repo = enhance_repo(session, repo)
                old_repo = es.search(index=es_config['index'], body={"query":{ "match_phrase":{"full_name": repo['full_name'] }}})
                # if the repo already exists, update it
                if len(old_repo['hits']['hits']) > 0:
                    logging.info(repo["full_name"] + " - Repo already exists, updating it")
                    repo_id = old_repo['hits']['hits'][0]['_id']
                    es.index(index=es_config['index'], doc_type="repo", id=repo_id, body=repo)
                    updated_repos.append(repo)

                # if not, index it and index its files
                else:
                    es.index(index=es_config['index'], doc_type="repo", body=repo)
                    index_files(session, es, repo)
        page_num += 1
    logging.info(str(len(updated_repos)) + " updated repos were found")
    update_files(session, es, updated_repos, since)


def update_files(session, es, repos, since):
    '''
    Note: this function assumes that bitbucket API returns
    commits ordered by date
    '''
    for repo in repos:
        commits_endpoint = bb_config['v2_endpoint'] + repo['full_name'] + "/commits"
        page_num = 1
        size = 0
        new = True

        while new:
            page_params = {"page": str(page_num)}
            commits = session.get(commits_endpoint, params=page_params).json()
            if 'values' not in commits:
                if (size == 0) or (page_num * 10 < size):
                    logging.error("Error in calling " + bb_config['repos_endpoint'])
                    logging.error("Please check your bitbucket.conf file")
                    exit(1)
                logging.info("Checked all repos")
                new = False
                break
            else:
                size = commits['pagelen']
                commits = commits['values']
            files = []
            for commit in commits:
                logging.debug(commit['date'].split("+")[0])
                commitdate = time.strptime(commit['date'].split("+")[0], '%Y-%m-%dT%H:%M:%S')
                if (since < commitdate):
                    diff = session.get(commit['links']['diff']['href']).text
                    files = files + parse_diff(diff)
                else:
                    new = False

            repo_branches = bb_config['v1_endpoint'] + repo['full_name'] + "/branches"
            branches = session.get(repo_branches).json()
            logging.debug(branches)
            for f in files:
                for branchname in branches:
                    logging.debug(branchname)
                    branch_updated = time.strptime(branches[branchname]['timestamp'], '%Y-%m-%d %H:%M:%S')
                    if (since < branch_updated):
                        if f['mode'] == 'new':
                            index_file(session, es, repo, branches[branchname], f)
                        elif f['mode'] == 'deleted':
                            delete_file(session, es, repo, branches[branchname], f)
                        elif f['mode'] == 'index':
                            update_file(session, es, repo, branches[branchname], f)
            page_num += 1


def index_file(session, es, repo, branch, diff_file, doc_id=None):
    f = read_file_from_repo_dir(session, repo, branch, diff_file)
    if not f:
        f = {}
    filepath = bb_config['v1_endpoint'] + repo['full_name'] + "/src/" +\
               branch['branch'] + diff_file['filepath']
    logging.info("Indexing new file: " + filepath)
    if session.get(filepath).ok:
        filedata = session.get(filepath).json()
    else:
        logging.error("File not found " + filepath)
        return
    if len(filedata['data']) < es_config['size_limit']:
        f.update({unicode("filedata") : filedata})
    else:
        logging.info(repo['full_name'] + branch['branch'] + filepath + " is too large, only its metadata will be indexed")
    f.update({unicode("branch") : branch['branch']})
    f.update({unicode("repo") : repo['full_name']})
    f.update({unicode("repolanguage") : repo['language']})
    logging.debug(f)
    f.update({unicode("link") : repo['links']['html']['href'] + "/src/" \
              + branch['branch'] + "/" + filedata['path']})
    f.update({unicode("extension") : filedata['path'].split('/')[-1].split('.')[-1]})
    f.update({"collapse_id" : repo['full_name'] + "/" + filedata['path']})
    logging.debug(f)
    if doc_id == None:
        es.index(index=es_config['index'], doc_type="file", body=f)
    else:
        es.index(index=es_config['index'], doc_type="file", id=doc_id, body=f)

def read_file_from_repo_dir(session, repo, branch, diff_file):
    files_endpoint = bb_config['v1_endpoint'] + repo['full_name'] + "/src/" \
                     + branch['branch'] + diff_file['path']
    try:
        files = session.get(files_endpoint).json()
        if 'files' in files:
            for f in files['files']:
                if f['path'] == diff_file['name']:
                    return f
    except ValueError:
        logging.warn("No files found in " + files_endpoint)
    return None

def update_file(session, es, repo, branch, diff_file):
    logging.info("Updating file: " + diff_file['filepath'])
    doc_id = get_file_id(session, es, repo, branch, diff_file)
    #delete_file(session, es, repo, branch, diff_file)
    index_file(session, es, repo, branch, diff_file, doc_id)
    return


def delete_file(session, es, repo, branch, diff_file):
    logging.info("Deleting file: " + diff_file['filepath'])
    query = { 'query': { 'bool': { 'must': [ \
            { 'match_phrase': { 'branch': branch['branch'] }}, \
            { 'match_phrase': { 'path': diff_file['filepath'][1:]}}, \
            { 'match_phrase': { 'repo': repo['full_name']}}]}}}
    es.delete_by_query(index=es_config['index'], doc_type="file", body=query)
    return

def get_file_id(session, es, repo, branch, diff_file):
    query = { 'query': { 'bool': { 'must': [ \
            { 'match_phrase': { 'branch': branch['branch']}}, \
            { 'match_phrase': { 'path': diff_file['filepath'][1:]}}, \
            { 'match_phrase': { 'repo': repo['full_name']}}]}}}
    hits = es.search(index=es_config['index'], doc_type="file", body=query)
    if len(hits['hits']['hits']) > 0:
        return hits['hits']['hits'][0]['_id']
    else:
        return None

def parse_diff(diff):
    '''
    parses a diff file which looks as follows:
    diff --git a/test2.txt b/test2.txt
    deleted file mode 100644
    index a5bce3f..0000000
    --- a/test2.txt
    +++ /dev/null
    @@ -1 +0,0 @@
    -test1
    diff --git a/test3.txt b/test3.txt
    new file mode 100644
    index 0000000..df6b0d2
    --- /dev/null
    +++ b/test3.txt
    @@ -0,0 +1 @@
    +test3
    '''
    files = []
    lines = diff.split('\n')
    for i in range(len(lines)):
        if lines[i].startswith('diff'):
            f = {}
            f['name'] = lines[i].split(' ')[2].split('/')[-1]
            f['filepath'] = lines[i].split(' ')[2][1:]
            f['path'] = f['filepath'].split(f['name'])[0]
            f['mode'] = lines[i+1].split(' ')[0]
            files.append(f)
    logging.info(files)
    return files


def test(session):
    test1 = "https://bitbucket.org/!api/2.0/repositories/comperio/front-java-framework/diff/03101f22b1017108675601c3dc9a13a8e78ab3af"
    test2 = "https://bitbucket.org/!api/2.0/repositories/comperio/customer-posten-tilbudssok-uploadgui-saml/diff/8ab932b80e0fd6ab5c2e63e8f9cc0e1d06e9f7f2"
    parse_diff(session.get(test1).text)
    parse_diff(session.get(test2).text)
