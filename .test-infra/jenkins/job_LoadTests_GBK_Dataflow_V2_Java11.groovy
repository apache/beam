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

def loadTestConfigurations = { mode, isStreaming ->
  [
    [
      title          : 'Load test: 2GB of 10B records',
      test           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        project               : 'apache-beam-testing',
        region                : 'us-central1',
        appName               : "load_tests_Java11_Dataflow_V2_${mode}_GBK_1",
        tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
        influxMeasurement     : "java_${mode}_gbk_1",
        influxTags            : """
                                  {
                                    "runnerVersion": "v2",
                                    "jdk": "java11"
                                  }
                                """.trim().replaceAll("\\s", ""),
        publishToInfluxDB     : true,
        sourceOptions         : """
                                  {
                                    "numRecords": 200000000,
                                    "keySizeBytes": 1,
                                    "valueSizeBytes": 9
                                  }
                                """.trim().replaceAll("\\s", ""),
        fanout                : 1,
        iterations            : 1,
        numWorkers            : 5,
        autoscalingAlgorithm  : "NONE",
        streaming             : isStreaming
      ]
    ],
    [
      title          : 'Load test: 2GB of 100B records',
      test           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        project               : 'apache-beam-testing',
        region                : 'us-central1',
        appName               : "load_tests_Java11_Dataflow_V2_${mode}_GBK_2",
        tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
        influxMeasurement     : "java_${mode}_gbk_2",
        influxTags            : """
                                  {
                                    "runnerVersion": "v2",
                                    "jdk": "java11"
                                  }
                                """.trim().replaceAll("\\s", ""),
        publishToInfluxDB     : true,
        sourceOptions         : """
                                  {
                                    "numRecords": 20000000,
                                    "keySizeBytes": 10,
                                    "valueSizeBytes": 90
                                  }
                                """.trim().replaceAll("\\s", ""),
        fanout                : 1,
        iterations            : 1,
        numWorkers            : 5,
        autoscalingAlgorithm  : "NONE",
        streaming             : isStreaming
      ]
    ],
    [

      title          : 'Load test: 2GB of 100kB records',
      test           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        project               : 'apache-beam-testing',
        region                : 'us-central1',
        appName               : "load_tests_Java11_Dataflow_V2_${mode}_GBK_3",
        tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
        influxMeasurement     : "java_${mode}_gbk_3",
        influxTags            : """
                                  {
                                    "runnerVersion": "v2",
                                    "jdk": "java11"
                                  }
                                """.trim().replaceAll("\\s", ""),
        publishToInfluxDB     : true,
        sourceOptions         : """
                                  {
                                    "numRecords": 20000,
                                    "keySizeBytes": 10000,
                                    "valueSizeBytes": 90000
                                  }
                                """.trim().replaceAll("\\s", ""),
        fanout                : 1,
        iterations            : 1,
        numWorkers            : 5,
        autoscalingAlgorithm  : "NONE",
        streaming             : isStreaming
      ]

    ],
    [
      title          : 'Load test: fanout 4 times with 2GB 10-byte records total',
      test           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        project               : 'apache-beam-testing',
        region                : 'us-central1',
        appName               : "load_tests_Java11_Dataflow_V2_${mode}_GBK_4",
        tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
        influxMeasurement     : "java_${mode}_gbk_4",
        influxTags            : """
                                  {
                                    "runnerVersion": "v2",
                                    "jdk": "java11"
                                  }
                                """.trim().replaceAll("\\s", ""),
        publishToInfluxDB     : true,
        sourceOptions         : """
                                  {
                                    "numRecords": 5000000,
                                    "keySizeBytes": 10,
                                    "valueSizeBytes": 90
                                  }
                                """.trim().replaceAll("\\s", ""),
        fanout                : 4,
        iterations            : 1,
        numWorkers            : 16,
        autoscalingAlgorithm  : "NONE",
        streaming             : isStreaming
      ]
    ],
    [
      title          : 'Load test: fanout 8 times with 2GB 10-byte records total',
      test           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        project               : 'apache-beam-testing',
        region                : 'us-central1',
        appName               : "load_tests_Java11_Dataflow_V2_${mode}_GBK_5",
        tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
        influxMeasurement     : "java_${mode}_gbk_5",
        influxTags            : """
                                  {
                                    "runnerVersion": "v2",
                                    "jdk": "java11"
                                  }
                                """.trim().replaceAll("\\s", ""),
        publishToInfluxDB     : true,
        sourceOptions         : """
                                  {
                                    "numRecords": 2500000,
                                    "keySizeBytes": 10,
                                    "valueSizeBytes": 90
                                  }
                                """.trim().replaceAll("\\s", ""),
        fanout                : 8,
        iterations            : 1,
        numWorkers            : 16,
        autoscalingAlgorithm  : "NONE",
        streaming             : isStreaming
      ]
    ],
    [
      title          : 'Load test: reiterate 4 times 10kB values',
      test           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        project               : 'apache-beam-testing',
        region                : 'us-central1',
        appName               : "load_tests_Java11_Dataflow_V2_${mode}_GBK_6",
        tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
        influxMeasurement     : "java_${mode}_gbk_6",
        influxTags            : """
                                  {
                                    "runnerVersion": "v2",
                                    "jdk": "java11"
                                  }
                                """.trim().replaceAll("\\s", ""),
        publishToInfluxDB     : true,
        sourceOptions         : """
                                  {
                                    "numRecords": 20000000,
                                    "keySizeBytes": 10,
                                    "valueSizeBytes": 90,
                                    "numHotKeys": 200,
                                    "hotKeyFraction": 1
                                  }
                                """.trim().replaceAll("\\s", ""),
        fanout                : 1,
        iterations            : 4,
        numWorkers            : 5,
        autoscalingAlgorithm  : "NONE",
        streaming             : isStreaming
      ]
    ],
    [
      title          : 'Load test: reiterate 4 times 2MB values',
      test           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        project               : 'apache-beam-testing',
        region                : 'us-central1',
        appName               : "load_tests_Java11_Dataflow_V2_${mode}_GBK_7",
        tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
        influxMeasurement     : "java_${mode}_gbk_7",
        influxTags            : """
                                  {
                                    "runnerVersion": "v2",
                                    "jdk": "java11"
                                  }
                                """.trim().replaceAll("\\s", ""),
        publishToInfluxDB     : true,
        sourceOptions         : """
                                  {
                                    "numRecords": 20000000,
                                    "keySizeBytes": 10,
                                    "valueSizeBytes": 90,
                                    "numHotKeys": 10,
                                    "hotKeyFraction": 1
                                  }
                                """.trim().replaceAll("\\s", ""),
        fanout                : 1,
        iterations            : 4,
        numWorkers            : 5,
        autoscalingAlgorithm  : "NONE",
        streaming             : isStreaming
      ]
    ]
  ].each { test -> test.pipelineOptions.putAll(additionalPipelineArgs) }
}

def final JOB_SPECIFIC_SWITCHES = [
  '-Prunner.version="V2"',
  '-PcompileAndRunTestsWithJava11',
  "-Pjava11Home=${commonJobProperties.JAVA_11_HOME}"
]

def streamingLoadTestJob = { scope, triggeringContext ->
  scope.description('Runs Java 11 GBK load tests on Dataflow runner V2 in streaming mode')
  commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 240)

  for (testConfiguration in loadTestConfigurations('streaming', true)) {
    testConfiguration.pipelineOptions << [inputWindowDurationSec: 1200]
    loadTestsBuilder.loadTest(scope, testConfiguration.title, testConfiguration.runner, CommonTestProperties.SDK.JAVA,
        testConfiguration.pipelineOptions, testConfiguration.test, JOB_SPECIFIC_SWITCHES)
  }
}

CronJobBuilder.cronJob('beam_LoadTests_Java_GBK_Dataflow_V2_Streaming_Java11', 'H H * * *', this) {
  additionalPipelineArgs = [
    influxDatabase: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influxHost: InfluxDBCredentialsHelper.InfluxDBHostUrl,
  ]
  streamingLoadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Java_GBK_Dataflow_V2_Streaming_Java11',
    'Run Load Tests Java 11 GBK Dataflow V2 Streaming',
    'Load Tests Java 11 GBK Dataflow V2 Streaming suite',
    this
    ) {
      additionalPipelineArgs = [:]
      streamingLoadTestJob(delegate, CommonTestProperties.TriggeringContext.PR)
    }


def batchLoadTestJob = { scope, triggeringContext ->
  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.JAVA, loadTestConfigurations('batch', false),
      "GBK", "batch", JOB_SPECIFIC_SWITCHES)
}

CronJobBuilder.cronJob('beam_LoadTests_Java_GBK_Dataflow_V2_Batch_Java11', 'H H * * *', this) {
  additionalPipelineArgs = [
    influxDatabase: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influxHost: InfluxDBCredentialsHelper.InfluxDBHostUrl,
  ]
  batchLoadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Java_GBK_Dataflow_V2_Batch_Java11',
    'Run Load Tests Java 11 GBK Dataflow V2 Batch',
    'Load Tests Java 11 GBK Dataflow V2 Batch suite',
    this
    ) {
      additionalPipelineArgs = [:]
      batchLoadTestJob(delegate, CommonTestProperties.TriggeringContext.PR)
    }
