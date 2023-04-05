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
This query is used to fetch PR data from github via Github API v4 (GraphQL).
Returns PRs on apache/beam repo that are older than provided timestamp.
Time format "2017-10-26T20:00:00Z
'''
MAIN_PR_QUERY = '''
query {
  search(query: "type:pr repo:apache/beam updated:><TemstampSubstitueLocation> sort:updated-asc", type: ISSUE, first: 100) {
    issueCount
    pageInfo {
      endCursor
      startCursor
      hasNextPage
      hasPreviousPage
    }
    edges {
      cursor
      node {
        ... on PullRequest {
          number
          createdAt
          updatedAt
          closedAt
          comments(first: 100) {
            pageInfo {
              endCursor
              startCursor
              hasNextPage
              hasPreviousPage
            }
            edges {
              node {
                author {
                  login
                }
                body
                createdAt
              }
            }
          }
          reviewRequests(first: 50) {
            pageInfo {
              startCursor
              endCursor
              hasNextPage
              hasPreviousPage
            }
            edges {
              node {
                requestedReviewer {
                  ... on User {
                    login
                  }
                }
              }
            }
          }
          assignees(first: 50) {
            pageInfo {
              startCursor
              endCursor
              hasNextPage
              hasPreviousPage
            }
            edges {
              node {
                login
              }
            }
          }
          reviews (first:50) {
            pageInfo {
              startCursor
              endCursor
              hasNextPage
              hasPreviousPage
            }
            edges {
              node {
                author {
                  login
                }
                body
                createdAt
                state
              }
            }
          }
          author {
            login
          }
          url
          body
          merged
          mergedAt
          mergedBy {
            login
          }
        }
      }
    }
  }
}
'''

'''
This query is used to fetch issue data from github via Github API v4 (GraphQL).
Returns issues on apache/beam repo that are older than provided timestamp.
Time format "2017-10-26T20:00:00Z
'''
MAIN_ISSUES_QUERY = '''
query {
  search(
    query: "type:issue repo:apache/beam updated:><TemstampSubstitueLocation> sort:updated-asc"
    type: ISSUE
    first: 100
  ) {
    issueCount
    pageInfo {
      endCursor
      startCursor
      hasNextPage
      hasPreviousPage
    }
    edges {
      cursor
      node {
        ... on Issue {
          number
          author {
            login
          }
          createdAt
          closedAt
          updatedAt
          assignees(first: 50) {
            pageInfo {
              startCursor
              endCursor
              hasNextPage
              hasPreviousPage
            }
            edges {
              node {
                login
              }
            }
          }
          title
          labels(first: 10) {
            edges {
              node {
                name
              }
            }
          }
        }
      }
    }
  }
}
'''