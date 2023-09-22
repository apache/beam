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
This Script is running every 3 hours in a cloud function in apache-beam-testing project.
This cloud function is triggered by a pubsub topic.
You can find the cloud function in the next link 
https://console.cloud.google.com/functions/details/us-central1/github_actions_workflows_dashboard_sync?env=gen1&project=apache-beam-testing
Pub sub topic : https://console.cloud.google.com/cloudpubsub/topic/detail/github_actions_workflows_sync?project=apache-beam-testing
Cron Job : https://console.cloud.google.com/cloudscheduler/jobs/edit/us-central1/github_actions_workflows_dashboard_sync?project=apache-beam-testing
Writing the latest 10 runs of every workflow in master branch in a beammetrics database
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
DB_NAME = os.environ['DB_DBNAME']
DB_USER_NAME = os.environ['DB_DBUSERNAME']
DB_PASSWORD = os.environ['DB_DBPWD']
GH_WORKFLOWS_TABLE_NAME = "github_workflows"
# Number of workflows that fetch github API
GH_NUMBER_OF_WORKFLOWS = 100  
GH_WORKFLOWS_NUMBER_EXECUTIONS = 100
WORKFLOWS_OBJECT_LIST = []


class Workflow:
    def __init__(self,id,name,filename):
        self.id = id
        self.name = name
        self.filename = filename
        self.listOfRuns = []
        self.runUrl = []

# The table will save the latest ten run of every workflow
GH_WORKFLOWS_CREATE_TABLE_QUERY = f"""
CREATE TABLE IF NOT EXISTS {GH_WORKFLOWS_TABLE_NAME} (
    job_name text PRIMARY KEY,
    job_yml_filename text"""
for i in range(0,GH_WORKFLOWS_NUMBER_EXECUTIONS):
    i = i + 1
    GH_WORKFLOWS_CREATE_TABLE_QUERY += """,\n    run{} text,
    run{}Id text""".format(str(i),str(i))
GH_WORKFLOWS_CREATE_TABLE_QUERY += ")\n"

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
        except Exception as e:
            print('Failed to connect to DB; retrying in 1 minute')
            print(e)
            time.sleep(60)
            i = i + 1
            if i >= maxRetries:
                print("Number of retries exceded ")
                sys.exit(1)
    return connection

def getToken():
    git_integration = GithubIntegration(
    os.environ["GH_APP_ID"],
    os.environ["GH_PEM_KEY"])
    token=git_integration.get_access_token(
            os.environ["GH_APP_INSTALLATION_ID"]
        ).token
    return token

def retriesRequest(request):
    requestSucceeded = False
    retryFactor = 1
    while not requestSucceeded:
        retryTime = 60 * retryFactor
        if request.status_code != 200:
            print('Failed to get the request with code {}'.format(request.status_code))
            time.sleep(retryTime)
            retryFactor = retryFactor + retryFactor
            if retryFactor * 60 >= 3600:
                print("Error: The request take more than an hour")
                sys.exit(1)
        else:
            requestSucceeded = True
def fetchWorkflowData():
    '''Return a json with all the workflows and the latests
    ten executions'''
    completed = False
    page = 1 
    workflows = []
    try:
        while not completed:
            url = "https://api.github.com/repos/apache/beam/actions/workflows"
            queryOptions = { 'branch' : 'master', 'page': page, 'per_page' : GH_NUMBER_OF_WORKFLOWS }
            response = requests.get(url = url, params = queryOptions)
            retriesRequest(response)
            jsonResponse = response.json()
            if jsonResponse['total_count'] >= GH_NUMBER_OF_WORKFLOWS:
                page = page + 1
                workflowsPage = jsonResponse['workflows']
                workflows.append(workflowsPage)
            else:
                completed = True
                workflowsPage = jsonResponse['workflows']
                workflows.append(workflowsPage)
        for pageItem in workflows:
            for item in pageItem:
                path = item['path']
                result = re.search('/(.*).yml', path)
                path = (result.group(1)) + ".yml"
                workflowObject = Workflow(item['id'],item['name'],path)
                WORKFLOWS_OBJECT_LIST.append(workflowObject)
            url = "https://api.github.com/repos/apache/beam/actions/workflows/"
            queryOptions = { 'branch' : 'master', 'per_page' : GH_WORKFLOWS_NUMBER_EXECUTIONS,
                        'page' :'1', 'exclude_pull_request':True }
        for workflow in WORKFLOWS_OBJECT_LIST:        
            response = requests.get(url = "{}{}/runs".format(url,workflow.id),
                        params=queryOptions)
            retriesRequest(response)
            responseJson = response.json()
            workflowsRuns = responseJson['workflow_runs']
            for  item in workflowsRuns:
                if item['status'] == 'completed':
                    workflow.runUrl.append(item['html_url'])
                    workflow.listOfRuns.append(item['conclusion'])
                elif item['status'] != 'cancelled':
                    workflow.listOfRuns.append(item['status'])
                    workflow.runUrl.append(item['html_url'])
            for i in range(0,GH_WORKFLOWS_NUMBER_EXECUTIONS):   
                if i >= len(workflow.listOfRuns):
                    workflow.listOfRuns.append('None')
                    workflow.runUrl.append('None')
    except Exception as e:
        print('Failed to get GHA workflows')
        print(e)

def databaseOperations(connection,fetchWorkflows):
    '''Create the table if not exist and update the table with the latest runs
    of the workflows '''
    queryInsert = "INSERT INTO {} VALUES ".format(GH_WORKFLOWS_TABLE_NAME)
    cursor = connection.cursor()
    cursor.execute(GH_WORKFLOWS_CREATE_TABLE_QUERY)
    cursor.execute("DELETE FROM {};".format(GH_WORKFLOWS_TABLE_NAME))
    query = ""
    for workflow in WORKFLOWS_OBJECT_LIST:
        rowInsert = "(\'{}\',\'{}\'".format(workflow.name,workflow.filename)
        for run, runUrl  in zip(workflow.listOfRuns,workflow.runUrl):
            rowInsert += ",\'{}\',\'{}\'".format(run,runUrl)
        query = query + rowInsert
        query += "),"
    query = query[:-1] + ";"  
    query = queryInsert + query
    cursor.execute(query)
    cursor.close()
    connection.commit()
    connection.close()
