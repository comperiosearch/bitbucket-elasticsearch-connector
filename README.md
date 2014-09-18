Bitbucket to elasticsearch connector written in Python. It supports indexing files and repos, updates and deletions.

Prerequisites
--------------
* python 2.7
* requests (pip install requests)
* requests_oauthlib
* elasticsearch (pip install elasticsearch)


Authentication
---------------
**Create an OAuth key and secret**

OAuth needs a key and secret together these are known as an OAuth consumer. You can create a consumer on any existing individual or team account. To create a consumer, do the following:

1. Log into your Bitbucket account.
2. Click accountname > Manage account from the menu.
3. Click OAuth under ACCESS MANAGEMENT.
4. Click the Add consumer button (Give it a name, description, etc.).

**Add the Key and Secret to [bitbucket.conf](bitbucket.conf)**

### Configuration files

    bitbucket.conf
    elasticsearch.conf

In [bitbucket.conf](bitbucket.conf) you can set the keys for the API (as explained above) and you also need to change the [USERNAME] in "repos\_endpoint" to be your username or the team's username.

In [elasticsearch.conf](elasticsearch.conf) you can set the elasticsearch host address, index name and maximum size for files to be indexed (this max size will only affect whether or not to index the content of the file, all other fields will be indexed).


How to run
--------------
```
    python main.py index
```

### Running modes
    index
    update
    pindex

To fully index your bitbucket content use the *index* mode, you can also use the experimental *pindex* mode which runs different python processes in parallel each indexing 10 repos. 

To update your elasticsearch index (for newly added, updated or deleted files/repos) use the *update* mode, this will use a history file named .bitbucketHistory to keep track of when the script was last ran.
