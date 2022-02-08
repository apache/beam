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

import CommonJobProperties as commonJobProperties
import CommonTestProperties
import LoadTestsBuilder as loadTestsBuilder
import PhraseTriggeringPostCommitBuilder
import CronJobBuilder
import InfluxDBCredentialsHelper

def commonLoadTestConfig = { jobType, isStreaming ->
  [
    [
      title          : 'Load test: ParDo 2GB 100 byte records 10 times',
      test           : 'org.apache.beam.sdk.loadtests.ParDoLoadTest',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        project             : 'apache-beam-testing',
        region              : 'us-central1',
        appName             : "load_tests_Java11_Dataflow_V2_${jobType}_ParDo_1",
        tempLocation        : 'gs://temp-storage-for-perf-tests/loadtests',
        influxMeasurement   : "java_${jobType}_pardo_1",
        influxTags          : """
                              {
                                "runnerVersion": "v2",
                                "jdk": "java11"
                              }
                              """.trim().replaceAll("\\s", ""),
        publishToInfluxDB   : true,
        sourceOptions       : """
                                {
                                  "numRecords": 20000000,
                                  "keySizeBytes": 10,
                                  "valueSizeBytes": 90
                                }
                              """.trim().replaceAll("\\s", ""),
        iterations          : 10,
        numberOfCounters    : 1,
        numberOfCounterOperations: 0,
        numWorkers          : 5,
        autoscalingAlgorithm: "NONE",
        streaming           : isStreaming
      ]
    ],
    [
      title          : 'Load test: ParDo 2GB 100 byte records 200  times',
      test           : 'org.apache.beam.sdk.loadtests.ParDoLoadTest',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        project             : 'apache-beam-testing',
        region              : 'us-central1',
        appName             : "load_tests_Java11_Dataflow_V2_${jobType}_ParDo_2",
        tempLocation        : 'gs://temp-storage-for-perf-tests/loadtests',
        influxMeasurement   : "java_${jobType}_pardo_2",
        influxTags          : """
                              {
                                "runnerVersion": "v2",
                                "jdk": "java11"
                              }
                              """.trim().replaceAll("\\s", ""),
        publishToInfluxDB   : true,
        sourceOptions       : """
                                {
                                  "numRecords": 20000000,
                                  "keySizeBytes": 10,
                                  "valueSizeBytes": 90
                                }
                              """.trim().replaceAll("\\s", ""),
        iterations          : 200,
        numberOfCounters    : 1,
        numberOfCounterOperations: 0,
        numWorkers          : 5,
        autoscalingAlgorithm: "NONE",
        streaming           : isStreaming
      ]
    ],
    [

      title          : 'Load test: ParDo 2GB 100 byte records 10 counters',
      test           : 'org.apache.beam.sdk.loadtests.ParDoLoadTest',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        project             : 'apache-beam-testing',
        region              : 'us-central1',
        appName             : "load_tests_Java11_Dataflow_V2_${jobType}_ParDo_3",
        tempLocation        : 'gs://temp-storage-for-perf-tests/loadtests',
        influxMeasurement   : "java_${jobType}_pardo_3",
        influxTags          : """
                              {
                                "runnerVersion": "v2",
                                "jdk": "java11"
                              }
                              """.trim().replaceAll("\\s", ""),
        publishToInfluxDB   : true,
        sourceOptions       : """
                                {
                                  "numRecords": 20000000,
                                  "keySizeBytes": 10,
                                  "valueSizeBytes": 90
                                }
                              """.trim().replaceAll("\\s", ""),
        iterations          : 1,
        numberOfCounters    : 1,
        numberOfCounterOperations: 10,
        numWorkers          : 5,
        autoscalingAlgorithm: "NONE",
        streaming           : isStreaming
      ]

    ],
    [
      title          : 'Load test: ParDo 2GB 100 byte records 100 counters',
      test           : 'org.apache.beam.sdk.loadtests.ParDoLoadTest',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        project             : 'apache-beam-testing',
        region              : 'us-central1',
        appName             : "load_tests_Java11_Dataflow_V2_${jobType}_ParDo_4",
        tempLocation        : 'gs://temp-storage-for-perf-tests/loadtests',
        influxMeasurement   : "java_${jobType}_pardo_4",
        influxTags          : """
                              {
                                "runnerVersion": "v2",
                                "jdk": "java11"
                              }
                              """.trim().replaceAll("\\s", ""),
        publishToInfluxDB   : true,
        sourceOptions       : """
                                {
                                  "numRecords": 20000000,
                                  "keySizeBytes": 10,
                                  "valueSizeBytes": 90
                                }
                              """.trim().replaceAll("\\s", ""),
        iterations          : 1,
        numberOfCounters    : 1,
        numberOfCounterOperations: 100,
        numWorkers          : 5,
        autoscalingAlgorithm: "NONE",
        streaming           : isStreaming
      ]
    ]
  ].each { test -> test.pipelineOptions.putAll(additionalPipelineArgs) }
}

def final JOB_SPECIFIC_SWITCHES = [
  '-Prunner.version="V2"',
  '-PcompileAndRunTestsWithJava11',
  "-Pjava11Home=${commonJobProperties.JAVA_11_HOME}"
]

def batchLoadTestJob = { scope, triggeringContext ->
  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.JAVA, commonLoadTestConfig('batch', false),
      "ParDo", "batch", JOB_SPECIFIC_SWITCHES)
}

def streamingLoadTestJob = {scope, triggeringContext ->
  scope.description('Runs Java 11 ParDo load tests on Dataflow runner V2 in streaming mode')
  commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 240)

  for (testConfiguration in commonLoadTestConfig('streaming', true)) {
    testConfiguration.pipelineOptions << [inputWindowDurationSec: 1200]
    loadTestsBuilder.loadTest(scope, testConfiguration.title, testConfiguration.runner, CommonTestProperties.SDK.JAVA,
        testConfiguration.pipelineOptions, testConfiguration.test, JOB_SPECIFIC_SWITCHES)
  }
}

CronJobBuilder.cronJob('beam_LoadTests_Java_ParDo_Dataflow_V2_Batch_Java11', 'H 12 * * *', this) {
  additionalPipelineArgs = [
    influxDatabase: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influxHost: InfluxDBCredentialsHelper.InfluxDBHostUrl,
  ]
  batchLoadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT)
}

CronJobBuilder.cronJob('beam_LoadTests_Java_ParDo_Dataflow_V2_Streaming_Java11', 'H 12 * * *', this) {
  additionalPipelineArgs = [
    influxDatabase: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influxHost: InfluxDBCredentialsHelper.InfluxDBHostUrl,
  ]
  streamingLoadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Java_ParDo_Dataflow_V2_Batch_Java11',
    'Run Load Tests Java 11 ParDo Dataflow V2 Batch',
    'Load Tests Java 11 ParDo Dataflow V2 Batch suite',
    this
    ) {
      additionalPipelineArgs = [:]
      batchLoadTestJob(delegate, CommonTestProperties.TriggeringContext.PR)
    }

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Java_ParDo_Dataflow_V2_Streaming_Java11',
    'Run Load Tests Java 11 ParDo Dataflow V2 Streaming',
    'Load Tests Java 11 ParDo Dataflow V2 Streaming suite',
    this
    ) {
      additionalPipelineArgs = [:]
      streamingLoadTestJob(delegate, CommonTestProperties.TriggeringContext.PR)
    }
