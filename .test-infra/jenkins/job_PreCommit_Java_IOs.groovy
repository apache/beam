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

def commonTriggerPatterns = [
  '^sdks/java/io/common/.*$',
  '^sdks/java/core/src/main/.*$',
]

// Define an umbrella PreCommit job running IO unit tests when common pattern
// (e.g. java core) is triggered. This avoids triggering too much PreCommit at
// the same time which may reach GitHub API rate limit. The projects are defined
// in the gradle task ':javaioPreCommit'.
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
    triggerPathPatterns: commonTriggerPatterns,
    // disable cron run because the tasks are covered by the single IO precommits below
    cronTriggering: false,
    timeoutMins: 120,
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
  }
}

// Define precommit jobs for each of these IO only run on corresponding module code change
def ioModulesMap = [
  // These projects are split from the umbrella 'Java_IOs_Direct' and will trigger on default patterns.
  // These are usually more dedicated IO (having more tests) or tests show known flakinesses.
  true: [
    'amazon-web-services',
    'amazon-web-services2',
    'azure',
    'hadoop-file-system',
    'kinesis',
    'pulsar',
  ],

  // These projects are also covered by 'Java_IOs_Direct', and won't trigger on default patterns.
  false: [
    'amqp',
    'cassandra',
    'cdap',
    'clickhouse',
    'csv',
    'debezium',
    'elasticsearch',
    'file-schema-transform',
    'google-ads',
    'hbase',
    'hcatalog',
    'influxdb',
    'jdbc',
    'jms',
    'kafka',
    'kudu',
    'mongodb',
    'mqtt',
    'neo4j',
    'parquet',
    'rabbitmq',
    'redis',
    'singlestore',
    'snowflake',
    'solr',
    'splunk',
    'thrift',
    'tika'
  ]
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
  'hadoop-file-system': [
    '^examples/java/.*$',
    '^sdks/java/testing/test-utils/.*$',
    '^sdks/java/io/hadoop-common/.*$',
    '^sdks/java/io/hadoop-format/.*$',
  ],
  hbase: [
    '^sdks/java/io/hadoop-common/.*$',
  ],
  hcatalog: [
    '^sdks/java/io/hadoop-common/.*$',
  ],
  kafka: [
    '^sdks/java/testing/test-utils/.*$',
    '^sdks/java/expansion-service/.*$',
    '^sdks/java/io/synthetic/.*$',
    '^sdks/java/io/expansion-service/.*$',
  ],
  neo4j: [
    '^sdks/java/testing/test-utils/.*$',
  ],
  parquet: [
    '^sdks/java/io/hadoop-common/.*$',
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
  'amazon-web-services': [
    ':sdks:java:io:amazon-web-services:integrationTest',
  ],
  'amazon-web-services2': [
    ':sdks:java:io:amazon-web-services2:integrationTest',
  ],
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
  'hadoop-file-system': [
    ':sdks:java:io:hadoop-common:build',
    ':sdks:java:io:hadoop-format:build',
  ],
  jdbc: [
    ':sdks:java:io:jdbc:integrationTest',
  ],
  kafka: [
    ':sdks:java:io:kafka:kafkaVersionsCompatibilityTest',
  ],
  kinesis: [
    ':sdks:java:io:kinesis:expansion-service:build',
    ':sdks:java:io:kinesis:integrationTest',
  ],
  neo4j: [
    ':sdks:java:io:kinesis:integrationTest',
  ],
  snowflake: [
    ':sdks:java:io:snowflake:expansion-service:build',
  ],
  jms: [
    ':sdks:java:io:jms:integrationTest',
  ],
]

// In case the test suite name is different from the project folder name
def aliasMap = [
  'amazon-web-services': 'Amazon-Web-Services',
  'amazon-web-services2': 'Amazon-Web-Services2',
  'jdbc': 'JDBC',
  'hadoop-file-system': 'hadoop',
  'mongodb': 'MongoDb',
]

// In case the package name is different from the project folder name
def packageNameMap = [
  'amazon-web-services': 'aws',
  'amazon-web-services2': 'aws2',
  'hadoop-file-system': 'hadoop',
]

ioModulesMap.forEach {cases, ioModules ->
  def hasDefaultTrigger = (cases == "true")
  ioModules.forEach {
    def triggerPaths = [
      '^sdks/java/io/' + it + '/.*$',
    ]
    if (hasDefaultTrigger) {
      triggerPaths.addAll(commonTriggerPatterns)
    }
    triggerPaths.addAll(additionalTriggerPaths.get(it, []))
    def tasks = [
      ':sdks:java:io:' + it + ':build'
    ]
    tasks.addAll(additionalTasks.get(it, []))
    def testName = aliasMap.get(it, it.capitalize())
    String jacocoPattern = "**/org/apache/beam/sdk/io/${packageNameMap.get(it,it)}/**"
    PrecommitJobBuilder builderSingle = new PrecommitJobBuilder(
        scope: this,
        nameBase: 'Java_' + testName + '_IO_Direct',
        gradleTasks: tasks,
        gradleSwitches: [
          '-PdisableSpotlessCheck=true',
          '-PdisableCheckStyle=true',
          // TODO(https://github.com/apache/beam/issues/26197) reenable code coverage
          // '-PenableJacocoReport'
        ], // spotless checked in separate pre-commit
        triggerPathPatterns: triggerPaths,
        defaultPathTriggering: hasDefaultTrigger,
        timeoutMins: 60,
        )
    builderSingle.build {
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
        // TODO(https://github.com/apache/beam/issues/26197) reenable code coverage
        // after resolving "no-space left on device" error running Jacoco plugin
        // jacocoCodeCoverage {
        //  execPattern('**/build/jacoco/*.exec')
        //  exclusionPattern('**/AutoValue_*')
        //  inclusionPattern(jacocoPattern)
        // }
      }
    }
  }
}
