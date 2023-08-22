/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import PrecommitJobBuilder

// exclude paths with their own PreCommit tasks
def excludePaths = [
  'extensions/avro',
  'extensions/sql',
  'io/amazon-web-services',
  'io/amazon-web-services2',
  'io/amqp',
  'io/azure',
  'io/cassandra',
  'io/cdap',
  'io/clickhouse',
  'io/csv',
  'io/debezium',
  'io/elasticsearch',
  'io/elasticsearch-tests',
  'io/file-schema-transform',
  'io/google-ads',
  'io/google-cloud-platform',
  'io/hadoop-common',
  'io/hadoop-file-system',
  'io/hadoop-format',
  'io/hbase',
  'io/hcatalog',
  'io/influxdb',
  'io/jdbc',
  'io/jms',
  'io/kafka',
  'io/kinesis',
  'io/kudu',
  'io/mqtt',
  'io/mongodb',
  'io/neo4j',
  'io/parquet',
  'io/pulsar',
  'io/rabbitmq',
  'io/redis',
  'io/singlestore',
  'io/snowflake',
  'io/solr',
  'io/splunk',
  'io/thrift',
  'io/tika',
]

// jacoco exclusion pattern works with class path (org/apache/beam/...) instead of
// code path (sdks/java/...). There is a few case the project name and module name
// are different so use this function to patch
private static def getModuleNameFromProject(String projectname) {
  def nameMap = [
    'io/amazon-web-services': 'io/aws',
    'io/amazon-web-services2': 'io/aws2',
    'io/google-cloud-platform': 'io/gcp',
    'io/hadoop-common': 'io/hadoop',
  ]
  String moduleName = nameMap.get(projectname, projectname)

  return "**/org/apache/beam/sdk/$moduleName/**" as String
}

PrecommitJobBuilder builder = new PrecommitJobBuilder(
    scope: this,
    nameBase: 'Java',
    gradleTask: ':javaPreCommit',
    gradleSwitches: [
      '-PdisableSpotlessCheck=true',
      '-PdisableCheckStyle=true'
    ], // spotless checked in separate pre-commit
    timeoutMins: 180,
    triggerPathPatterns: [
      '^model/.*$',
      '^sdks/java/.*$',
      '^runners/.*$',
      '^examples/java/.*$',
      '^examples/kotlin/.*$',
      '^release/.*$',
    ],
    excludePathPatterns: excludePaths.collect {entry ->
      "^sdks/java/" + entry + '/.*$'},
    numBuildsToRetain: 40
    )
builder.build {
  publishers {
    archiveJunit('**/build/test-results/**/*.xml')
    recordIssues {
      tools {
        errorProne()
        java()
        spotBugs {
          pattern('**/build/reports/spotbugs/*.xml')
        }
      }
      enabledForFailure(true)
    }
    jacocoCodeCoverage {
      execPattern('**/build/jacoco/*.exec')
      exclusionPattern('**/org/apache/beam/gradle/**,**/org/apache/beam/model/**,' +
          '**/org/apache/beam/runners/dataflow/worker/windmill/**,**/AutoValue_*,' +
          excludePaths.collect {entry -> getModuleNameFromProject(entry) }.join(",") )
    }
  }
}
