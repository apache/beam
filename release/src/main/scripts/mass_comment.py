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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Script for mass-commenting Jenkins test triggers on a Beam PR."""

import itertools
import os
import socket
import sys
import time
import traceback
import re
import requests
from datetime import datetime

COMMENTS_TO_ADD = [
    "Run Release Gradle Build",
    "Run Go PostCommit",
    "Run Java PostCommit",
    "Run Java Flink PortableValidatesRunner Batch",
    "Run Java Flink PortableValidatesRunner Streaming",
    "Run Dataflow ValidatesRunner",
    "Run Flink ValidatesRunner",
    "Run Samza ValidatesRunner",
    "Run Spark ValidatesRunner",
    "Run Java Spark PortableValidatesRunner Batch",
    "Run Python Spark ValidatesRunner",
    "Run Python Dataflow ValidatesContainer",
    "Run Python Dataflow ValidatesRunner",
    "Run Python 3.5 Flink ValidatesRunner",
    "Run Twister2 ValidatesRunner",
    "Run Python 2 PostCommit",
    "Run Python 3.5 PostCommit",
    "Run Python 3.6 PostCommit",
    "Run Python 3.7 PostCommit",
    "Run SQL PostCommit",
    "Run Go PreCommit",
    "Run Java PreCommit",
    "Run Java_Examples_Dataflow PreCommit",
    "Run JavaPortabilityApi PreCommit",
    "Run Portable_Python PreCommit",
    "Run PythonLint PreCommit",
    "Run Python PreCommit",
    "Run Python DockerBuild PreCommit"
]


def executeGHGraphqlQuery(accessToken, query):
  '''Runs graphql query on GitHub.'''
  url = 'https://api.github.com/graphql'
  headers = {'Authorization': 'Bearer %s' % accessToken}
  r = requests.post(url=url, json={'query': query}, headers=headers)
  return r.json()


def getSubjectId(accessToken, prNumber):
  query = '''
query FindPullRequestID {
  repository(owner:"apache", name:"beam") {
    pullRequest(number:%s) {
      id
    }
  }
}
''' % prNumber
  response = executeGHGraphqlQuery(accessToken, query)
  return response['data']['repository']['pullRequest']['id']


def fetchGHData(accessToken, subjectId, commentBody):
  '''Fetches GitHub data required for reporting Beam metrics'''
  query = '''
mutation AddPullRequestComment {
  addComment(input:{subjectId:"%s",body: "%s"}) {
    commentEdge {
        node {
        createdAt
        body
      }
    }
    subject {
      id
    }
  }
}
''' % (subjectId, commentBody)
  return executeGHGraphqlQuery(accessToken, query)


def postComments(accessToken, subjectId):
  '''
  Main workhorse method. Fetches data from GitHub and puts it in metrics table.
  '''

  for commentBody in COMMENTS_TO_ADD:
    jsonData = fetchGHData(accessToken, subjectId, commentBody)
  print(jsonData)


def probeGitHubIsUp():
  '''
  Returns True if GitHub responds to simple queries. Else returns False.
  '''
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  result = sock.connect_ex(('github.com', 443))
  return True if result == 0 else False


################################################################################
if __name__ == '__main__':
  '''
  This script is supposed to be invoked directly.
  However for testing purposes and to allow importing,
  wrap work code in module check.
  '''
  print("Started.")

  if not probeGitHubIsUp():
    print("GitHub is unavailable, skipping fetching data.")
    exit()

  print("GitHub is available start fetching data.")

  accessToken = input("Enter your Github access token: ")

  pr = input("Enter the Beam PR number to test (e.g. 11403): ")
  subjectId = getSubjectId(accessToken, pr)

  postComments(accessToken, subjectId)
  print("Fetched data.")

  print('Done.')
