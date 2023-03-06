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
    "Run CommunityMetrics PreCommit",
    "Run Dataflow Runner Nexmark Tests",
    "Run Dataflow Runner Tpcds Tests",
    "Run Dataflow Runner V2 Java 11 Nexmark Tests",
    "Run Dataflow Runner V2 Java 17 Nexmark Tests",
    "Run Dataflow Runner V2 Nexmark Tests",
    "Run Dataflow Streaming ValidatesRunner",
    "Run Dataflow ValidatesRunner Java 11",
    "Run Dataflow ValidatesRunner Java 17",
    "Run Dataflow ValidatesRunner",
    "Run Direct Runner Nexmark Tests",
    "Run Direct ValidatesRunner Java 11",
    "Run Direct ValidatesRunner Java 17",
    "Run Direct ValidatesRunner in Java 11",
    "Run Direct ValidatesRunner",
    "Run Flink Runner Nexmark Tests",
    "Run Flink ValidatesRunner Java 11",
    "Run Flink Runner Tpcds Tests",
    "Run Flink ValidatesRunner",
    "Run Go Flink ValidatesRunner",
    "Run Go PostCommit",
    "Run Go PreCommit",
    "Run Go Samza ValidatesRunner",
    "Run Go Spark ValidatesRunner",
    "Run GoPortable PreCommit",
    "Run Java 11 Examples on Dataflow Runner V2",
    "Run Java 17 Examples on Dataflow Runner V2",
    "Run Java Dataflow V2 ValidatesRunner Streaming",
    "Run Java Dataflow V2 ValidatesRunner",
    "Run Java Examples on Dataflow Runner V2",
    "Run Java Examples_Direct",
    "Run Java Examples_Flink",
    "Run Java Examples_Spark",
    "Run Java Flink PortableValidatesRunner Streaming",
    "Run Java PostCommit",
    "Run Java PreCommit",
    "Run Java Samza PortableValidatesRunner",
    "Run Java Spark PortableValidatesRunner Batch",
    "Run Java Spark v2 PortableValidatesRunner Streaming",
    "Run Java Spark v3 PortableValidatesRunner Streaming",
    "Run Java examples on Dataflow Java 11",
    "Run Java examples on Dataflow Java 17",
    "Run Java examples on Dataflow with Java 11",
    "Run Java_Amazon-Web-Services2_IO_Direct PreCommit",
    "Run Java_Amazon-Web-Services_IO_Direct PreCommit",
    "Run Java_Amqp_IO_Direct PreCommit",
    "Run Java_Azure_IO_Direct PreCommit",
    "Run Java_Cassandra_IO_Direct PreCommit",
    "Run Java_Cdap_IO_Direct PreCommit",
    "Run Java_Clickhouse_IO_Direct PreCommit",
    "Run Java_Debezium_IO_Direct PreCommit",
    "Run Java_ElasticSearch_IO_Direct PreCommit",
    "Run Java_Examples_Dataflow PreCommit",
    "Run Java_Examples_Dataflow_Java11 PreCommit",
    "Run Java_Examples_Dataflow_Java17 PreCommit",
    "Run Java_GCP_IO_Direct PreCommit",
    "Run Java_HBase_IO_Direct PreCommit",
    "Run Java_HCatalog_IO_Direct PreCommit",
    "Run Java_Hadoop_IO_Direct PreCommit",
    "Run Java_InfluxDb_IO_Direct PreCommit",
    "Run Java_JDBC_IO_Direct PreCommit",
    "Run Java_Jms_IO_Direct PreCommit",
    "Run Java_Kafka_IO_Direct PreCommit",
    "Run Java_Kinesis_IO_Direct PreCommit",
    "Run Java_Kudu_IO_Direct PreCommit",
    "Run Java_MongoDb_IO_Direct PreCommit",
    "Run Java_Mqtt_IO_Direct PreCommit",
    "Run Java_Neo4j_IO_Direct PreCommit",
    "Run Java_PVR_Flink_Batch PreCommit",
    "Run Java_PVR_Flink_Docker PreCommit",
    "Run Java_Parquet_IO_Direct PreCommit",
    "Run Java_Pulsar_IO_Direct PreCommit",
    "Run Java_RabbitMq_IO_Direct PreCommit",
    "Run Java_Redis_IO_Direct PreCommit",
    "Run Java_SingleStore_IO_Direct PreCommit",
    "Run Java_Snowflake_IO_Direct PreCommit",
    "Run Java_Solr_IO_Direct PreCommit",
    "Run Java_Spark3_Versions PreCommit",
    "Run Java_Splunk_IO_Direct PreCommit",
    "Run Java_Thrift_IO_Direct PreCommit",
    "Run Java_Tika_IO_Direct PreCommit",
    "Run Javadoc PostCommit",
    "Run Jpms Dataflow Java 11 PostCommit",
    "Run Jpms Dataflow Java 17 PostCommit",
    "Run Jpms Direct Java 11 PostCommit",
    "Run Jpms Direct Java 17 PostCommit",
    "Run Jpms Flink Java 11 PostCommit",
    "Run Jpms Spark Java 11 PostCommit",
    "Run Kotlin_Examples PreCommit",
    "Run PortableJar_Flink PostCommit",
    "Run PortableJar_Spark PostCommit",
    "Run Portable_Python PreCommit",
    "Run PostCommit_Java_Dataflow",
    "Run PostCommit_Java_DataflowV2",
    "Run PostCommit_Java_Hadoop_Versions",
    "Run Python 3.7 PostCommit",
    "Run Python 3.8 PostCommit",
    "Run Python 3.9 PostCommit",
    "Run Python 3.10 PostCommit",
    "Run Python Dataflow V2 ValidatesRunner",
    "Run Python Dataflow ValidatesContainer",
    "Run Python Dataflow ValidatesRunner",
    "Run Python Direct Runner Nexmark Tests",
    "Run Python Examples_Dataflow",
    "Run Python Examples_Direct",
    "Run Python Examples_Flink",
    "Run Python Examples_Spark",
    "Run Python Flink ValidatesRunner",
    "Run Python PreCommit",
    "Run Python Samza ValidatesRunner",
    "Run Python Spark ValidatesRunner",
    "Run PythonDocker PreCommit",
    "Run PythonDocs PreCommit",
    "Run PythonFormatter PreCommit",
    "Run PythonLint PreCommit",
    "Run Python_Coverage PreCommit",
    "Run Python_Dataframes PreCommit",
    "Run Python_Examples PreCommit",
    "Run Python_Integration PreCommit",
    "Run Python_PVR_Flink PreCommit",
    "Run Python_Runners PreCommit",
    "Run Python_Transforms PreCommit",
    "Run RAT PreCommit",
    "Run Release Gradle Build",
    "Run SQL PostCommit",
    "Run SQL PreCommit",
    "Run SQL_Java11 PreCommit",
    "Run SQL_Java17 PreCommit",
    "Run Samza ValidatesRunner",
    "Run Spark Runner Nexmark Tests",
    "Run Spark Runner Tpcds Tests",
    "Run Spark StructuredStreaming ValidatesRunner",
    "Run Spark ValidatesRunner Java 11",
    "Run Spark ValidatesRunner",
    "Run Spotless PreCommit",
    "Run Twister2 ValidatesRunner",
    "Run Typescript PreCommit",
    "Run ULR Loopback ValidatesRunner",
    "Run Whitespace PreCommit",
    "Run XVR_Direct PostCommit",
    "Run XVR_Flink PostCommit",
    "Run XVR_GoUsingJava_Dataflow PostCommit",
    "Run XVR_JavaUsingPython_Dataflow PostCommit",
    "Run XVR_PythonUsingJavaSQL_Dataflow PostCommit",
    "Run XVR_PythonUsingJava_Dataflow PostCommit",
    "Run XVR_Samza PostCommit",
    "Run XVR_Spark PostCommit",
    "Run XVR_Spark3 PostCommit",
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
    # Space out comments 30 seconds apart to avoid overwhelming Jenkins
    time.sleep(30)
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
