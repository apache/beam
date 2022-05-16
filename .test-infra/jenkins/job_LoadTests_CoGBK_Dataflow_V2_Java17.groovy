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
      title          : 'Load test: CoGBK 2GB 100  byte records - single key',
      test           : 'org.apache.beam.sdk.loadtests.CoGroupByKeyLoadTest',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        project               : 'apache-beam-testing',
        region                : 'us-central1',
        appName               : "load_tests_Java17_Dataflow_V2_${mode}_CoGBK_1",
        tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
        influxMeasurement     : "java_${mode}_cogbk_1",
        influxTags            : """
                                  {
                                    "runnerVersion": "v2",
                                    "jdk": "java17"
                                  }
                                """.trim().replaceAll("\\s", ""),
        publishToInfluxDB     : true,
        sourceOptions         : """
          {
            "numRecords": 20000000,
            "keySizeBytes": 10,
            "valueSizeBytes": 90,
            "numHotKeys": 1
          }
        """.trim().replaceAll("\\s", ""),
        coSourceOptions       : """
              {
                "numRecords": 2000000,
                "keySizeBytes": 10,
                "valueSizeBytes": 90,
                "numHotKeys": 1000
              }
            """.trim().replaceAll("\\s", ""),
        iterations            : 1,
        numWorkers            : 5,
        autoscalingAlgorithm  : "NONE",
        streaming             : isStreaming
      ]
    ],
    [
      title          : 'Load test: CoGBK 2GB 100 byte records - multiple keys',
      test           : 'org.apache.beam.sdk.loadtests.CoGroupByKeyLoadTest',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        project               : 'apache-beam-testing',
        region                : 'us-central1',
        appName               : "load_tests_Java17_Dataflow_V2_${mode}_CoGBK_2",
        tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
        influxMeasurement     : "java_${mode}_cogbk_2",
        influxTags            : """
                                  {
                                    "runnerVersion": "v2",
                                    "jdk": "java17"
                                  }
                                """.trim().replaceAll("\\s", ""),
        publishToInfluxDB     : true,
        sourceOptions         : """
                  {
                    "numRecords": 20000000,
                    "keySizeBytes": 10,
                    "valueSizeBytes": 90,
                    "numHotKeys": 5
                  }
                """.trim().replaceAll("\\s", ""),
        coSourceOptions       : """
                          {
                            "numRecords": 2000000,
                            "keySizeBytes": 10,
                            "valueSizeBytes": 90,
                            "numHotKeys": 1000
                          }
                        """.trim().replaceAll("\\s", ""),
        iterations            : 1,
        numWorkers            : 5,
        autoscalingAlgorithm  : "NONE",
        streaming             : isStreaming
      ]
    ],
    [

      title          : 'Load test: CoGBK 2GB reiteration 10kB value',
      test           : 'org.apache.beam.sdk.loadtests.CoGroupByKeyLoadTest',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        project               : 'apache-beam-testing',
        region                : 'us-central1',
        appName               : "load_tests_Java17_Dataflow_V2_${mode}_CoGBK_3",
        tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
        influxMeasurement     : "java_${mode}_cogbk_3",
        influxTags            : """
                                  {
                                    "runnerVersion": "v2",
                                    "jdk": "java17"
                                  }
                                """.trim().replaceAll("\\s", ""),
        publishToInfluxDB     : true,
        sourceOptions         : """
                              {
                                "numRecords": 2000000,
                                "keySizeBytes": 10,
                                "valueSizeBytes": 90,
                                "numHotKeys": 200000
                              }
                            """.trim().replaceAll("\\s", ""),
        coSourceOptions       : """
                              {
                                "numRecords": 2000000,
                                "keySizeBytes": 10,
                                "valueSizeBytes": 90,
                                "numHotKeys": 1000
                              }
                            """.trim().replaceAll("\\s", ""),
        iterations            : 4,
        numWorkers            : 5,
        autoscalingAlgorithm  : "NONE",
        streaming             : isStreaming
      ]

    ],
    [
      title          : 'Load test: CoGBK 2GB reiteration 2MB value',
      test           : 'org.apache.beam.sdk.loadtests.CoGroupByKeyLoadTest',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        project               : 'apache-beam-testing',
        region                : 'us-central1',
        appName               : "load_tests_Java17_Dataflow_V2_${mode}_CoGBK_4",
        tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
        influxMeasurement     : "java_${mode}_cogbk_4",
        influxTags            : """
                                  {
                                    "runnerVersion": "v2",
                                    "jdk": "java17"
                                  }
                                """.trim().replaceAll("\\s", ""),
        publishToInfluxDB     : true,
        sourceOptions         : """
                              {
                                "numRecords": 2000000,
                                "keySizeBytes": 10,
                                "valueSizeBytes": 90,
                                "numHotKeys": 1000
                              }
                            """.trim().replaceAll("\\s", ""),
        coSourceOptions       : """
                              {
                                "numRecords": 2000000,
                                "keySizeBytes": 10,
                                "valueSizeBytes": 90,
                                "numHotKeys": 1000
                              }
                            """.trim().replaceAll("\\s", ""),
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
  '-PcompileAndRunTestsWithJava17',
  "-Pjava17Home=${commonJobProperties.JAVA_17_HOME}"
]

def streamingLoadTestJob = { scope, triggeringContext ->
  scope.description('Runs Java 17 CoGBK load tests on Dataflow runner V2 in streaming mode')
  commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 240)

  for (testConfiguration in loadTestConfigurations('streaming', true)) {
    testConfiguration.pipelineOptions << [inputWindowDurationSec: 1200, coInputWindowDurationSec: 1200]
    loadTestsBuilder.loadTest(scope, testConfiguration.title, testConfiguration.runner, CommonTestProperties.SDK.JAVA,
        testConfiguration.pipelineOptions, testConfiguration.test, JOB_SPECIFIC_SWITCHES)
  }
}

CronJobBuilder.cronJob('beam_LoadTests_Java_CoGBK_Dataflow_V2_Streaming_Java17', 'H 12 * * *', this) {
  additionalPipelineArgs = [
    influxDatabase: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influxHost: InfluxDBCredentialsHelper.InfluxDBHostUrl,
  ]
  streamingLoadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Java_CoGBK_Dataflow_V2_Streaming_Java17',
    'Run Load Tests Java 17 CoGBK Dataflow V2 Streaming',
    'Load Tests Java 17 CoGBK Dataflow V2 Streaming suite',
    this
    ) {
      additionalPipelineArgs = [:]
      streamingLoadTestJob(delegate, CommonTestProperties.TriggeringContext.PR)
    }


def batchLoadTestJob = { scope, triggeringContext ->
  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.JAVA, loadTestConfigurations('batch', false),
      "CoGBK", "batch", JOB_SPECIFIC_SWITCHES)
}

CronJobBuilder.cronJob('beam_LoadTests_Java_CoGBK_Dataflow_V2_Batch_Java17', 'H 14 * * *', this) {
  additionalPipelineArgs = [
    influxDatabase: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influxHost: InfluxDBCredentialsHelper.InfluxDBHostUrl,
  ]
  batchLoadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Java_CoGBK_Dataflow_V2_Batch_Java17',
    'Run Load Tests Java 17 CoGBK Dataflow V2 Batch',
    'Load Tests Java 17 CoGBK Dataflow V2 Batch suite',
    this
    ) {
      additionalPipelineArgs = [:]
      batchLoadTestJob(delegate, CommonTestProperties.TriggeringContext.PR)
    }
