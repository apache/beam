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

import os
import requests
import socket
import time


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


def addPrComment(accessToken, subjectId, commentBody):
  '''Adds a pr comment to the PR defined by subjectId'''
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

def getPrStatuses(accessToken, prNumber):
  query = '''
query GetPRChecks {
  repository(name: "beam", owner: "apache") {
    pullRequest(number: %s) {
      commits(last: 1) {
        nodes {
          commit {
            status {
              contexts {
                targetUrl
                context
              }
            }
          }
        }
      }
    }
  }
}
''' % (prNumber)
  return executeGHGraphqlQuery(accessToken, query)


def postComments(accessToken, subjectId, commentsToAdd):
  '''
  Main workhorse method. Posts comments to GH.
  '''

  for comment in commentsToAdd:
    jsonData = addPrComment(accessToken, subjectId, comment[0])
    print(jsonData)
    # Space out comments 30 seconds apart to avoid overwhelming Jenkins
    time.sleep(30)


def probeGitHubIsUp():
  '''
  Returns True if GitHub responds to simple queries. Else returns False.
  '''
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  result = sock.connect_ex(('github.com', 443))
  return True if result == 0 else False

def getRemainingComments(accessToken, pr, initialComments):
  '''
  Filters out the comments that already have statuses associated with them from initial comments
  '''
  queryResult = getPrStatuses(accessToken, pr)
  pull = queryResult["data"]["repository"]["pullRequest"]
  commit = pull["commits"]["nodes"][0]["commit"]
  check_urls = str(list(map(lambda c : c["targetUrl"], commit["status"]["contexts"])))
  remainingComments = []
  for comment in initialComments:
    if f'/{comment[1]}_Phrase/' not in check_urls and f'/{comment[1]}_PR/' not in check_urls \
        and f'/{comment[1]}_Commit/' not in check_urls and f'/{comment[1]}/' not in check_urls \
        and 'Sickbay' not in comment[1]:
      print(comment)
      remainingComments.append(comment)
  return remainingComments

################################################################################
if __name__ == '__main__':
  '''
  This script is supposed to be invoked directly.
  However for testing purposes and to allow importing,
  wrap work code in module check.
  '''
  print("Started.")
  comments = []
  dirname = os.path.dirname(__file__)
  with open(os.path.join(dirname, 'jenkins_jobs.txt')) as file:
    comments = [line.strip() for line in file if len(line.strip()) > 0]
  
  for i in range(len(comments)):
    parts = comments[i].split(',')
    comments[i] = (parts[0], parts[1])

  if not probeGitHubIsUp():
    print("GitHub is unavailable, skipping fetching data.")
    exit()

  print("GitHub is available start fetching data.")

  accessToken = input("Enter your Github access token: ")

  pr = input("Enter the Beam PR number to test (e.g. 11403): ")
  subjectId = getSubjectId(accessToken, pr)

  # TODO(yathu): also auto rerun failed GitHub Action workflow
  remainingComments = getRemainingComments(accessToken, pr, comments)
  if len(remainingComments) == 0:
    print('Jobs have been started for all comments. If you would like to retry all jobs, create a new commit before running this script.')
  while len(remainingComments) > 0:
    postComments(accessToken, subjectId, remainingComments)
    # Sleep 60 seconds to allow checks to start to status
    time.sleep(60)
    remainingComments = getRemainingComments(accessToken, pr, remainingComments)
    if len(remainingComments) > 0:
      print(f'{len(remainingComments)} comments must be reposted because no check has been created for them: {str(remainingComments)}')
      print('Sleeping for 1 hour to allow Jenkins to recover and to give it time to status.')
      for i in range(60):
        time.sleep(60)
        print(f'{i} minutes elapsed, {60-i} minutes remaining')
    remainingComments = getRemainingComments(accessToken, pr, remainingComments)
    if len(remainingComments) == 0:
      print(f'{len(remainingComments)} comments still must be reposted: {str(remainingComments)}')
      print('Trying to repost comments.')

  print('Done.')
