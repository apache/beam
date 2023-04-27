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

import requests
import socket
import time

# This list can be found by querying the Jenkins API, see BEAM-13951
COMMENTS_TO_ADD = [
    ("Run CommunityMetrics PreCommit", "beam_PreCommit_CommunityMetrics"),
    ("Run Dataflow Runner Nexmark Tests", "beam_PostCommit_Java_Nexmark_Dataflow"),
    ("Run Dataflow Runner Tpcds Tests", "beam_PostCommit_Java_Tpcds_Dataflow"),
    ("Run Dataflow Runner V2 Java 11 Nexmark Tests", "beam_PostCommit_Java_Nexmark_DataflowV2_Java11"),
    ("Run Dataflow Runner V2 Java 17 Nexmark Tests", "beam_PostCommit_Java_Nexmark_DataflowV2_Java17"),
    ("Run Dataflow Runner V2 Nexmark Tests", "beam_PostCommit_Java_Nexmark_Dataflow_V2"),
    ("Run Dataflow Streaming ValidatesRunner", "beam_PostCommit_Java_ValidatesRunner_Dataflow_Streaming"),
    ("Run Dataflow ValidatesRunner Java 11", "beam_PostCommit_Java_ValidatesRunner_Dataflow_Java11"),
    ("Run Dataflow ValidatesRunner Java 17", "beam_PostCommit_Java_ValidatesRunner_Dataflow_Java17"),
    ("Run Dataflow ValidatesRunner", "beam_PostCommit_Java_ValidatesRunner_Dataflow"),
    ("Run Direct Runner Nexmark Tests", "beam_PostCommit_Java_Nexmark_Direct"),
    ("Run Direct ValidatesRunner Java 11", "beam_PostCommit_Java_ValidatesRunner_Direct_Java11"),
    ("Run Direct ValidatesRunner Java 17", "beam_PostCommit_Java_ValidatesRunner_Direct_Java17"),
    ("Run Direct ValidatesRunner Java 11", "beam_PostCommit_Java_ValidatesRunner_Direct_Java11"),
    ("Run Direct ValidatesRunner", "beam_PostCommit_Java_ValidatesRunner_Direct"),
    ("Run Flink Runner Nexmark Tests", "beam_PostCommit_Java_Nexmark_Flink"),
    ("Run Flink ValidatesRunner Java 11", "beam_PostCommit_Java_ValidatesRunner_Flink_Java11"),
    ("Run Flink Runner Tpcds Tests", "beam_PostCommit_Java_Tpcds_Flink"),
    ("Run Flink ValidatesRunner", "beam_PostCommit_Java_ValidatesRunner_Flink"),
    ("Run Go Flink ValidatesRunner", "beam_PostCommit_Go_VR_Flink"),
    ("Run Go PostCommit", "beam_PostCommit_Go"),
    ("Run Go PreCommit", "beam_PreCommit_Go"),
    ("Run Go Samza ValidatesRunner", "beam_PostCommit_Go_VR_Samza"),
    ("Run Go Spark ValidatesRunner", "beam_PostCommit_Go_VR_Spark"),
    ("Run GoPortable PreCommit", "beam_PreCommit_GoPortable"),
    ("Run Java 11 Examples on Dataflow Runner V2", "beam_PostCommit_Java_Examples_Dataflow_V2_Java11"),
    ("Run Java 17 Examples on Dataflow Runner V2", "beam_PostCommit_Java_Examples_Dataflow_V2_Java17"),
    ("Run Java Dataflow V2 ValidatesRunner Streaming", "beam_PostCommit_Java_VR_Dataflow_V2_Streaming"),
    ("Run Java Dataflow V2 ValidatesRunner", "beam_PostCommit_Java_VR_Dataflow_V2"),
    ("Run Java Examples on Dataflow Runner V2", "beam_PostCommit_Java_Examples_Dataflow_V2"),
    ("Run Java Examples_Direct", "beam_PostCommit_Java_Examples_Direct"),
    ("Run Java Examples_Flink", "beam_PostCommit_Java_Examples_Flink"),
    ("Run Java Examples_Spark", "beam_PostCommit_Java_Examples_Spark"),
    ("Run Java Flink PortableValidatesRunner Streaming", "beam_PostCommit_Java_PVR_Flink_Streaming"),
    ("Run Java PostCommit", "beam_PostCommit_Java"),
    ("Run Java PreCommit", "beam_PreCommit_Java"),
    ("Run Java Samza PortableValidatesRunner", "beam_PostCommit_Java_PVR_Samza"),
    ("Run Java Spark PortableValidatesRunner Batch", "beam_PostCommit_Java_PVR_Spark_Batch"),
    ("Run Java Spark v2 PortableValidatesRunner Streaming", "beam_PostCommit_Java_PVR_Spark2_Streaming"),
    ("Run Java Spark v3 PortableValidatesRunner Streaming", "beam_PostCommit_Java_PVR_Spark3_Streaming"),
    ("Run Java examples on Dataflow Java 11", "beam_PostCommit_Java_Dataflow_Examples_Java11"),
    ("Run Java examples on Dataflow Java 17", "beam_PostCommit_Java_Dataflow_Examples_Java17"),
    ("Run Java_Amazon-Web-Services2_IO_Direct PreCommit", "beam_PreCommit_Java_Amazon-Web-Services2_IO_Direct"),
    ("Run Java_Amazon-Web-Services_IO_Direct PreCommit", "beam_PreCommit_Java_Amazon-Web-Services_IO_Direct"),
    ("Run Java_Amqp_IO_Direct PreCommit", "beam_PreCommit_Java_Amqp_IO_Direct"),
    ("Run Java_Azure_IO_Direct PreCommit", "beam_PreCommit_Java_Azure_IO_Direct"),
    ("Run Java_Cassandra_IO_Direct PreCommit", "beam_PreCommit_Java_Cassandra_IO_Direct"),
    ("Run Java_Cdap_IO_Direct PreCommit", "beam_PreCommit_Java_Cdap_IO_Direct"),
    ("Run Java_Clickhouse_IO_Direct PreCommit", "beam_PreCommit_Java_Clickhouse_IO_Direct"),
    ("Run Java_Debezium_IO_Direct PreCommit", "beam_PreCommit_Java_Debezium_IO_Direct"),
    ("Run Java_ElasticSearch_IO_Direct PreCommit", "beam_PreCommit_Java_ElasticSearch_IO_Direct"),
    ("Run Java_Examples_Dataflow PreCommit", "beam_PreCommit_Java_Examples_Dataflow"),
    ("Run Java_Examples_Dataflow_Java11 PreCommit", "beam_PreCommit_Java_Examples_Dataflow_Java11"),
    ("Run Java_Examples_Dataflow_Java17 PreCommit", "beam_PreCommit_Java_Examples_Dataflow_Java17"),
    ("Run Java_GCP_IO_Direct PreCommit", "beam_PreCommit_Java_GCP_IO_Direct"),
    ("Run Java_HBase_IO_Direct PreCommit", "beam_PreCommit_Java_HBase_IO_Direct"),
    ("Run Java_HCatalog_IO_Direct PreCommit", "beam_PreCommit_Java_HCatalog_IO_Direct"),
    ("Run Java_Hadoop_IO_Direct PreCommit", "beam_PreCommit_Java_Hadoop_IO_Direct"),
    ("Run Java_InfluxDb_IO_Direct PreCommit", "beam_PreCommit_Java_InfluxDb_IO_Direct"),
    ("Run Java_JDBC_IO_Direct PreCommit", "beam_PreCommit_Java_JDBC_IO_Direct"),
    ("Run Java_Jms_IO_Direct PreCommit", "beam_PreCommit_Java_Jms_IO_Direct"),
    ("Run Java_Kafka_IO_Direct PreCommit", "beam_PreCommit_Java_Kafka_IO_Direct"),
    ("Run Java_Kinesis_IO_Direct PreCommit", "beam_PreCommit_Java_Kinesis_IO_Direct"),
    ("Run Java_Kudu_IO_Direct PreCommit", "beam_PreCommit_Java_Kudu_IO_Direct"),
    ("Run Java_MongoDb_IO_Direct PreCommit", "beam_PreCommit_Java_MongoDb_IO_Direct"),
    ("Run Java_Mqtt_IO_Direct PreCommit", "beam_PreCommit_Java_Mqtt_IO_Direct"),
    ("Run Java_Neo4j_IO_Direct PreCommit", "beam_PreCommit_Java_Neo4j_IO_Direct	"),
    ("Run Java_PVR_Flink_Batch PreCommit", "beam_PreCommit_Java_PVR_Flink_Batch"),
    ("Run Java_PVR_Flink_Docker PreCommit", "beam_PreCommit_Java_PVR_Flink_Docker"),
    ("Run Java_Parquet_IO_Direct PreCommit", "beam_PreCommit_Java_Parquet_IO_Direct"),
    ("Run Java_Pulsar_IO_Direct PreCommit", "beam_PreCommit_Java_Pulsar_IO_Direct"),
    ("Run Java_RabbitMq_IO_Direct PreCommit", "beam_PreCommit_Java_RabbitMq_IO_Direct"),
    ("Run Java_Redis_IO_Direct PreCommit", "beam_PreCommit_Java_Redis_IO_Direct"),
    ("Run Java_SingleStore_IO_Direct PreCommit", "beam_PreCommit_Java_SingleStore_IO_Direct"),
    ("Run Java_Snowflake_IO_Direct PreCommit", "beam_PreCommit_Java_Snowflake_IO_Direct"),
    ("Run Java_Solr_IO_Direct PreCommit", "beam_PreCommit_Java_Solr_IO_Direct"),
    ("Run Java_Spark3_Versions PreCommit", "beam_PreCommit_Java_Spark3_Versions"),
    ("Run Java_Splunk_IO_Direct PreCommit", "beam_PreCommit_Java_Splunk_IO_Direct"),
    ("Run Java_Thrift_IO_Direct PreCommit", "beam_PreCommit_Java_Thrift_IO_Direct"),
    ("Run Java_Tika_IO_Direct PreCommit", "beam_PreCommit_Java_Tika_IO_Direct"),
    ("Run Javadoc PostCommit", "beam_PostCommit_Javadoc"),
    ("Run Jpms Dataflow Java 11 PostCommit", "beam_PostCommit_Java_Jpms_Dataflow_Java11"),
    ("Run Jpms Dataflow Java 17 PostCommit", "beam_PostCommit_Java_Jpms_Dataflow_Java17"),
    ("Run Jpms Direct Java 11 PostCommit", "beam_PostCommit_Java_Jpms_Direct_Java11"),
    ("Run Jpms Direct Java 17 PostCommit", "beam_PostCommit_Java_Jpms_Direct_Java17"),
    ("Run Jpms Flink Java 11 PostCommit", "beam_PostCommit_Java_Jpms_Flink_Java11"),
    ("Run Jpms Spark Java 11 PostCommit", "beam_PostCommit_Java_Jpms_Spark_Java11"),
    ("Run Kotlin_Examples PreCommit", "beam_PreCommit_Kotlin_Examples"),
    ("Run PortableJar_Flink PostCommit", "beam_PostCommit_PortableJar_Flink"),
    ("Run PortableJar_Spark PostCommit", "beam_PostCommit_PortableJar_Spark"),
    ("Run Portable_Python PreCommit", "beam_PreCommit_Portable_Python"),
    ("Run PostCommit_Java_Dataflow", "beam_PostCommit_Java_DataflowV1"),
    ("Run PostCommit_Java_DataflowV2", "beam_PostCommit_Java_DataflowV2"),
    ("Run PostCommit_Java_Hadoop_Versions", "beam_PostCommit_Java_Hadoop_Versions"),
    ("Run Python 3.7 PostCommit", "beam_PostCommit_Python37"),
    ("Run Python 3.8 PostCommit", "beam_PostCommit_Python38"),
    ("Run Python 3.9 PostCommit", "beam_PostCommit_Python39"),
    ("Run Python 3.10 PostCommit", "beam_PostCommit_Python310"),
    ("Run Python 3.11 PostCommit", "beam_PostCommit_Python311"),
    ("Run Python Dataflow V2 ValidatesRunner", "beam_PostCommit_Py_VR_Dataflow_V2"),
    ("Run Python Dataflow ValidatesContainer", "beam_PostCommit_Py_ValCont"),
    ("Run Python Dataflow ValidatesRunner", "beam_PostCommit_Py_VR_Dataflow"),
    ("Run Python Direct Runner Nexmark Tests", "beam_PostCommit_Python_Nexmark_Direct"),
    ("Run Python Examples_Dataflow", "beam_PostCommit_Python_Examples_Dataflow"),
    ("Run Python Examples_Direct", "beam_PostCommit_Python_Examples_Direct"),
    ("Run Python Examples_Flink", "beam_PostCommit_Python_Examples_Flink"),
    ("Run Python Examples_Spark", "beam_PostCommit_Python_Examples_Spark"),
    ("Run Python Flink ValidatesRunner", "beam_PostCommit_Python_VR_Flink"),
    ("Run Python PreCommit", "beam_PreCommit_Python"),
    ("Run Python Samza ValidatesRunner", "beam_PostCommit_Python_VR_Samza"),
    ("Run Python Spark ValidatesRunner", "beam_PostCommit_Python_VR_Spark"),
    ("Run PythonDocker PreCommit", "beam_PreCommit_PythonDocker"),
    ("Run PythonDocs PreCommit", "beam_PreCommit_PythonDocs"),
    ("Run PythonFormatter PreCommit", "beam_PreCommit_PythonFormatter"),
    ("Run PythonLint PreCommit", "beam_PreCommit_PythonLint"),
    ("Run Python_Coverage PreCommit", "beam_PreCommit_Python_Coverage"),
    ("Run Python_Dataframes PreCommit", "beam_PreCommit_Python_Dataframes"),
    ("Run Python_Examples PreCommit", "beam_PreCommit_Python_Examples"),
    ("Run Python_Integration PreCommit", "beam_PreCommit_Python_Integration"),
    ("Run Python_PVR_Flink PreCommit", "beam_PreCommit_Python_PVR_Flink"),
    ("Run Python_Runners PreCommit", "beam_PreCommit_Python_Runners"),
    ("Run Python_Transforms PreCommit", "beam_PreCommit_Python_Transforms"),
    ("Run RAT PreCommit", "beam_PreCommit_RAT"),
    ("Run Release Gradle Build", "beam_Release_Gradle_Build"),
    ("Run SQL PostCommit", "beam_PostCommit_SQL"),
    ("Run SQL PreCommit", "beam_PreCommit_SQL"),
    ("Run SQL_Java11 PreCommit", "beam_PreCommit_SQL_Java11"),
    ("Run SQL_Java17 PreCommit", "beam_PreCommit_SQL_Java17"),
    ("Run Samza ValidatesRunner", "beam_PostCommit_Java_ValidatesRunner_Samza"),
    ("Run Spark Runner Nexmark Tests", "beam_PostCommit_Java_Nexmark_Spark"),
    ("Run Spark Runner Tpcds Tests", "beam_PostCommit_Java_Tpcds_Spark"),
    ("Run Spark StructuredStreaming ValidatesRunner", "beam_PostCommit_Java_ValidatesRunner_SparkStructuredStreaming"),
    ("Run Spark ValidatesRunner Java 11", "beam_PostCommit_Java_ValidatesRunner_Spark_Java11"),
    ("Run Spark ValidatesRunner", "beam_PostCommit_Java_ValidatesRunner_Spark"),
    ("Run Spotless PreCommit", "beam_PreCommit_Spotless"),
    ("Run Twister2 ValidatesRunner", "beam_PostCommit_Java_ValidatesRunner_Twister2"),
    ("Run Typescript PreCommit", "beam_PreCommit_Typescript"),
    ("Run ULR Loopback ValidatesRunner", "beam_PostCommit_Java_ValidatesRunner_ULR"),
    ("Run Whitespace PreCommit", "beam_PreCommit_Whitespace"),
    ("Run XVR_Direct PostCommit", "beam_PostCommit_XVR_Direct"),
    ("Run XVR_Flink PostCommit", "beam_PostCommit_XVR_Flink"),
    ("Run XVR_GoUsingJava_Dataflow PostCommit", "beam_PostCommit_XVR_GoUsingJava_Dataflow"),
    ("Run XVR_JavaUsingPython_Dataflow PostCommit", "beam_PostCommit_XVR_JavaUsingPython_Dataflow"),
    ("Run XVR_PythonUsingJavaSQL_Dataflow PostCommit", "beam_PostCommit_XVR_PythonUsingJavaSQL_Dataflow"),
    ("Run XVR_PythonUsingJava_Dataflow PostCommit", "beam_PostCommit_XVR_PythonUsingJava_Dataflow"),
    ("Run XVR_Samza PostCommit", "beam_PostCommit_XVR_Samza"),
    ("Run XVR_Spark PostCommit", "beam_PostCommit_XVR_Spark"),
    ("Run XVR_Spark3 PostCommit", "beam_PostCommit_XVR_Spark3"),
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
    if f'/{comment[1]}_Phrase/' not in check_urls:
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

  if not probeGitHubIsUp():
    print("GitHub is unavailable, skipping fetching data.")
    exit()

  print("GitHub is available start fetching data.")

  accessToken = input("Enter your Github access token: ")

  pr = input("Enter the Beam PR number to test (e.g. 11403): ")
  subjectId = getSubjectId(accessToken, pr)
  
  remainingComments = COMMENTS_TO_ADD
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
