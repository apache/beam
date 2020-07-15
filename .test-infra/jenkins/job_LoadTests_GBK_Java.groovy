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

def loadTestConfigurations = { mode, isStreaming, datasetName ->
  [
    [
      title          : 'Load test: 2GB of 10B records',
      test           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        project               : 'apache-beam-testing',
        region                : 'us-central1',
        appName               : "load_tests_Java_Dataflow_${mode}_GBK_1",
        tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
        publishToBigQuery     : true,
        bigQueryDataset       : datasetName,
        bigQueryTable         : "java_dataflow_${mode}_GBK_1",
        influxMeasurement     : "java_${mode}_gbk_1",
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
        appName               : "load_tests_Java_Dataflow_${mode}_GBK_2",
        tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
        publishToBigQuery     : true,
        bigQueryDataset       : datasetName,
        bigQueryTable         : "java_dataflow_${mode}_GBK_2",
        influxMeasurement     : "java_${mode}_gbk_2",
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
        appName               : "load_tests_Java_Dataflow_${mode}_GBK_3",
        tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
        publishToBigQuery     : true,
        bigQueryDataset       : datasetName,
        bigQueryTable         : "java_dataflow_${mode}_GBK_3",
        influxMeasurement     : "java_${mode}_gbk_3",
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
        appName               : 'load_tests_Java_Dataflow_${mode}_GBK_4',
        tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
        publishToBigQuery     : true,
        bigQueryDataset       : datasetName,
        bigQueryTable         : "java_dataflow_${mode}_GBK_4",
        influxMeasurement     : "java_${mode}_gbk_4",
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
        appName               : "load_tests_Java_Dataflow_${mode}_GBK_5",
        tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
        publishToBigQuery     : true,
        bigQueryDataset       : datasetName,
        bigQueryTable         : "java_dataflow_${mode}_GBK_5",
        influxMeasurement     : "java_${mode}_gbk_5",
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
        appName               : "load_tests_Java_Dataflow_${mode}_GBK_6",
        tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
        publishToBigQuery     : true,
        bigQueryDataset       : datasetName,
        bigQueryTable         : "java_dataflow_${mode}_GBK_6",
        influxMeasurement     : "java_${mode}_gbk_6",
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
        appName               : "load_tests_Java_Dataflow_${mode}_GBK_7",
        tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
        publishToBigQuery     : true,
        bigQueryDataset       : datasetName,
        bigQueryTable         : "java_dataflow_${mode}_GBK_7",
        influxMeasurement     : "java_${mode}_gbk_7",
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

def streamingLoadTestJob = { scope, triggeringContext ->
  scope.description('Runs Java GBK load tests on Dataflow runner in streaming mode')
  commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 240)

  def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', triggeringContext)
  for (testConfiguration in loadTestConfigurations('streaming', true, datasetName)) {
    testConfiguration.pipelineOptions << [inputWindowDurationSec: 1200]
    loadTestsBuilder.loadTest(scope, testConfiguration.title, testConfiguration.runner, CommonTestProperties.SDK.JAVA, testConfiguration.pipelineOptions, testConfiguration.test)
  }
}

CronJobBuilder.cronJob('beam_LoadTests_Java_GBK_Dataflow_Streaming', 'H 12 * * *', this) {
  additionalPipelineArgs = [
    influxDatabase: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influxHost: InfluxDBCredentialsHelper.InfluxDBHostname,
  ]
  streamingLoadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Java_GBK_Dataflow_Streaming',
    'Run Load Tests Java GBK Dataflow Streaming',
    'Load Tests Java GBK Dataflow Streaming suite',
    this
    ) {
      additionalPipelineArgs = [:]
      streamingLoadTestJob(delegate, CommonTestProperties.TriggeringContext.PR)
    }


def batchLoadTestJob = { scope, triggeringContext ->
  def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', triggeringContext)
  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.JAVA, loadTestConfigurations('batch', false, datasetName), "GBK", "batch")
}

CronJobBuilder.cronJob('beam_LoadTests_Java_GBK_Dataflow_Batch', 'H 14 * * *', this) {
  additionalPipelineArgs = [
    influxDatabase: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influxHost: InfluxDBCredentialsHelper.InfluxDBHostname,
  ]
  batchLoadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Java_GBK_Dataflow_Batch',
    'Run Load Tests Java GBK Dataflow Batch',
    'Load Tests Java GBK Dataflow Batch suite',
    this
    ) {
      additionalPipelineArgs = [:]
      batchLoadTestJob(delegate, CommonTestProperties.TriggeringContext.PR)
    }
