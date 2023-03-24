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

// Define a PreCommit job running IO unit tests that were excluded in the default job_PreCommit_Java.groovy
PrecommitJobBuilder builder = new PrecommitJobBuilder(
    scope: this,
    nameBase: 'Java_IOs_Direct',
    gradleTasks: [
      ':javaioPreCommit',
    ],
    gradleSwitches: [
      '-PdisableSpotlessCheck=true',
      '-PdisableCheckStyle=true'
    ], // spotless checked in separate pre-commit
    triggerPathPatterns: [
      '^sdks/java/io/common/.*$',
      '^sdks/java/core/src/main/.*$',
    ],
    // disable cron run because the tasks are covered by the single IO precommits below
    cronTriggering: false,
    timeoutMins: 120,
    )
builder.build {
  publishers {
    archiveJunit('**/build/test-results/**/*.xml')
  }
}

// define precommit jobs for each of these IO only run on corresponding module code change
def ioModules = [
  'amqp',
  'cassandra',
  'cdap',
  'clickhouse',
  'csv',
  'debezium',
  'elasticsearch',
  'file-schema-transform',
  'hbase',
  'hcatalog',
  'influxdb',
  'jms',
  'kudu',
  'mqtt',
  'neo4j',
  'rabbitmq',
  'redis',
  'singlestore',
  'snowflake',
  'solr',
  'splunk',
  'thrift',
  'tika'
]

// any additional trigger path besides the module path and 'sdk/io/common'
def additionalTriggerPaths = [
  cdap: [
    '^sdks/java/io/hadoop-common/.*$',
    '^sdks/java/io/hadoop-format/.*$',
  ],
  elasticsearch: [
    '^sdks/java/io/elasticsearch-tests/.*$',
  ],
  hbase: [
    '^sdks/java/io/hadoop-common/.*$',
  ],
  hcatalog: [
    '^sdks/java/io/hadoop-common/.*$',
  ],
  neo4j: [
    '^sdks/java/testing/test-utils/.*$',
  ],
  singlestore: [
    '^sdks/java/testing/test-utils/.*$',
  ],
  snowflake: [
    '^sdks/java/extensions/google-cloud-platform-core/.*$',
    '^sdks/java/testing/test-utils/.*$',]
]

// any additional tasks besides 'build'.
// Additional :build tasks should be made sync with build.gradle:kts's :javaioPreCommit task which will be triggered on commit to java core and buildSrc
// While integration tasks (e.g. :integrationTest) does not need to add there.
def additionalTasks = [
  debezium: [
    ':sdks:java:io:debezium:expansion-service:build',
    ':sdks:java:io:debezium:integrationTest',
  ],
  elasticsearch: [
    ':sdks:java:io:elasticsearch-tests:elasticsearch-tests-5:build',
    ':sdks:java:io:elasticsearch-tests:elasticsearch-tests-6:build',
    ':sdks:java:io:elasticsearch-tests:elasticsearch-tests-7:build',
    ':sdks:java:io:elasticsearch-tests:elasticsearch-tests-8:build',
    ':sdks:java:io:elasticsearch-tests:elasticsearch-tests-common:build',
  ],
  neo4j: [
    ':sdks:java:io:kinesis:integrationTest',
  ],
  snowflake: [
    ':sdks:java:io:snowflake:expansion-service:build',
  ],
]

ioModules.forEach {
  def triggerPaths = [
    '^sdks/java/io/' + it + '/.*$',
  ]
  triggerPaths.addAll(additionalTriggerPaths.get(it, []))
  def tasks = [
    ':sdks:java:io:' + it + ':build'
  ]
  tasks.addAll(additionalTasks.get(it, []))
  PrecommitJobBuilder builderSingle = new PrecommitJobBuilder(
      scope: this,
      nameBase: 'Java_' + it.capitalize() + '_IO_Direct',
      gradleTasks: tasks,
      gradleSwitches: [
        '-PdisableSpotlessCheck=true',
        '-PdisableCheckStyle=true'
      ], // spotless checked in separate pre-commit
      triggerPathPatterns: triggerPaths,
      defaultPathTriggering: false,
      timeoutMins: 60,
      )
  builderSingle.build {
    publishers {
      archiveJunit('**/build/test-results/**/*.xml')
    }
  }
}
