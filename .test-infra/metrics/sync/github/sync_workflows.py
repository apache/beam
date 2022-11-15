#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
'''
This module queries GitHub to collect Beam-related workflows metrics and put them in
PostgreSQL.
This Script it's running every 2  minutes in a cloud function in apache-beam-testing project.
This cloud function is triggered by a pubsub topic.
You can found the cloud function in the next link 
https://console.cloud.google.com/functions/details/us-central1/github_actions_workflows_dashboard_sync?env=gen1&project=apache-beam-testing
Pub sub topic : https://console.cloud.google.com/cloudpubsub/topic/detail/github_actions_workflows_sync?project=apache-beam-testing
Cron Job : https://console.cloud.google.com/cloudscheduler/jobs/edit/us-central1/github_actions_workflows_dashboard_sync?project=apache-beam-testing
Writing the latest 10 jobs of every postcommit workflow in master branch in a beammetrics database

'''
import os
import sys
import time
import re
import requests
import psycopg2

from datetime import datetime
from github import GithubIntegration 

DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ['DB_PORT']
DB_NAME = os.environ['DB_NAME']
DB_USER_NAME = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASS']
GH_WORKFLOWS_TABLE_NAME = "github_workflows"
# Number of workflows that fetch github API
GH_NUMBER_OF_WORKFLOWS = 100  
GH_WORKFLOWS_NUMBER_EXECUTIONS = 10
GH_WORKFLOWS_YML_FILENAME = []

# The table will save the latest ten run of every workflow
GH_WORKFLOWS_CREATE_TABLE_QUERY = f"""
CREATE TABLE IF NOT EXISTS {GH_WORKFLOWS_TABLE_NAME} (
    job_name text PRIMARY KEY,
    job_yml_filename text,
    job1 text,
    job2 text,
    job3 text,
    job4 text,
    job5 text,
    job6 text,
    job7 text,
    job8 text,
    job9 text,
    job10 text
)
"""
def githubWorkflowsGrafanaSync(data,context):
    print('Started')

    print('Updating table with recent workflow runs')
    databaseOperations(initDbConnection(),fetchWorkflowData())
    print('Done')
    return "Completed"

def initDbConnection():
    '''Init connection with the Database'''
    connection = None
    maxRetries = 3
    i = 0 
    while connection == None and i < maxRetries:
        try:
            connection = psycopg2.connect(
                f"dbname='{DB_NAME}' user='{DB_USER_NAME}' host='{DB_HOST}'"
                f" port='{DB_PORT}' password='{DB_PASSWORD}'")
        except:
            print('Failed to connect to DB; retrying in 1 minute')
            sys.stdout.flush()
            time.sleep(60)
            i = i + 1
    return connection

def getToken():
    git_integration = GithubIntegration(
    os.environ["GH_APP_ID"],
    os.environ["GH_PEM_KEY"])

    token=git_integration.get_access_token(
            os.environ["GH_APP_INSTALLATION_ID"]
        ).token
    return token

def fetchWorkflowData():
    '''Return a json with all the workflows and the latests
    ten executions'''
    listOfWorkflows = {}
    workflowsStatus = {}
    try:
        url = "https://api.github.com/repos/apache/beam/actions/workflows"
        queryOptions = { 'branch' : 'master', 'per_page' : GH_NUMBER_OF_WORKFLOWS }
        response = requests.get(url = url, params = queryOptions)
        jsonResponse = response.json()
        workflows = jsonResponse['workflows']
        for item in workflows:
            path =(item['path'])
            isPostCommit = re.search('(.*)postcommit(.*)',path)
            if isPostCommit:
                result = re.search('/(.*).yml', path)
                path =(result.group(1)) + ".yml"
                GH_WORKFLOWS_YML_FILENAME.append(path)
                listOfWorkflows[(item['id'])] = item['name']
        url = "https://api.github.com/repos/apache/beam/actions/workflows/"
        queryOptions = { 'branch' : 'master', 'per_page' : GH_WORKFLOWS_NUMBER_EXECUTIONS,
                    'page' :'1', 'exclude_pull_request':True }
        #headers = {'Authorization': 'Bearer {}'.format(getToken())}
        for key in listOfWorkflows:
            response = requests.get(url = "{}{}/runs".format(url,key),
                                params=queryOptions)
            responseJson = response.json()
            workflowsRuns = responseJson['workflow_runs']
            workflowsStatus[listOfWorkflows[key]] = []
            for  item in workflowsRuns:
                if item['status'] == 'completed':
                    workflowsStatus[listOfWorkflows[key]].append(item['conclusion'])
                else:
                    workflowsStatus[listOfWorkflows[key]].append(item['status'])
            for i in range(0,GH_WORKFLOWS_NUMBER_EXECUTIONS):   
                if i >= len(workflowsStatus[listOfWorkflows[key]]):
                    workflowsStatus[listOfWorkflows[key]].append('None')
    except:
        print('Failed to get GHA workflows')
    return workflowsStatus

def databaseOperations(connection, workflowStatus):
    '''Create the table if not exist and update the table with the latest runs
    of the workflows '''
    queryInsert = "INSERT INTO {} VALUES ".format(GH_WORKFLOWS_TABLE_NAME)
    cursor = connection.cursor()
    cursor.execute(GH_WORKFLOWS_CREATE_TABLE_QUERY)
    cursor.execute("DELETE FROM {};".format(GH_WORKFLOWS_TABLE_NAME))
    query = ""
    for item in workflowStatus:
        rowInsert = """ (\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',
            \'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\'),""".format(
            item, GH_WORKFLOWS_YML_FILENAME[0],
            workflowStatus[item][0],workflowStatus[item][1],
            workflowStatus[item][2], workflowStatus[item][3],
            workflowStatus[item][4], workflowStatus[item][5],
            workflowStatus[item][6], workflowStatus[item][7],
            workflowStatus[item][8], workflowStatus[item][9]
            )
        query = query + rowInsert
        GH_WORKFLOWS_YML_FILENAME.pop(0)
    query = query[:-1] + ";"  
    query = queryInsert + query
    cursor.execute(query)
    cursor.close()
    connection.commit()
    connection.close()