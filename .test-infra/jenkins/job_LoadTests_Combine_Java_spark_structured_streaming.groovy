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

import CommonTestProperties
import CronJobBuilder
import LoadTestsBuilder as loadTestsBuilder
import PhraseTriggeringPostCommitBuilder
import InfluxDBCredentialsHelper

def commonLoadTestConfig = { jobType, isStreaming, datasetName ->
  [
    [
      title          : 'Load test: 2GB of 10B records',
      test           : 'org.apache.beam.sdk.loadtests.CombineLoadTest',
      runner         : CommonTestProperties.Runner.SPARK_STRUCTURED_STREAMING,
      pipelineOptions: [
        project             : 'apache-beam-testing',
        appName             : "load_tests_Java_SparkStructuredStreaming_${jobType}_Combine_1",
        tempLocation        : 'gs://temp-storage-for-perf-tests/loadtests',
        publishToBigQuery   : true,
        bigQueryDataset     : datasetName,
        bigQueryTable       : "java_sparkstructuredstreaming_${jobType}_Combine_1",
        influxMeasurement   : "java_${jobType}_combine_1",
        publishToInfluxDB   : true,
        sourceOptions       : """
                                {
                                  "numRecords": 200000000,
                                  "keySizeBytes": 1,
                                  "valueSizeBytes": 9
                                }
                              """.trim().replaceAll("\\s", ""),
        fanout              : 1,
        iterations          : 1,
        topCount            : 20,
        perKeyCombiner      : "TOP_LARGEST",
        streaming           : isStreaming
      ]
    ],
    [
      title          : 'Load test: fanout 4 times with 2GB 10-byte records total',
      test           : 'org.apache.beam.sdk.loadtests.CombineLoadTest',
      runner         : CommonTestProperties.Runner.SPARK_STRUCTURED_STREAMING,
      pipelineOptions: [
        project             : 'apache-beam-testing',
        appName             : "load_tests_Java_SparkStructuredStreaming_${jobType}_Combine_4",
        tempLocation        : 'gs://temp-storage-for-perf-tests/loadtests',
        publishToBigQuery   : true,
        bigQueryDataset     : datasetName,
        bigQueryTable       : "java_sparkstructuredstreaming_${jobType}_Combine_4",
        influxMeasurement   : "java_${jobType}_combine_4",
        publishToInfluxDB   : true,
        sourceOptions       : """
                                {
                                  "numRecords": 5000000,
                                  "keySizeBytes": 10,
                                  "valueSizeBytes": 90
                                }
                              """.trim().replaceAll("\\s", ""),
        fanout              : 4,
        iterations          : 1,
        topCount            : 20,
        perKeyCombiner      : "TOP_LARGEST",
        streaming           : isStreaming
      ]
    ],
    [
      title          : 'Load test: fanout 8 times with 2GB 10-byte records total',
      test           : 'org.apache.beam.sdk.loadtests.CombineLoadTest',
      runner         : CommonTestProperties.Runner.SPARK_STRUCTURED_STREAMING,
      pipelineOptions: [
        project             : 'apache-beam-testing',
        appName             : "load_tests_Java_SparkStructuredStreaming_${jobType}_Combine_5",
        tempLocation        : 'gs://temp-storage-for-perf-tests/loadtests',
        publishToBigQuery   : true,
        bigQueryDataset     : datasetName,
        bigQueryTable       : "java_sparkstructuredstreaming_${jobType}_Combine_5",
        influxMeasurement   : "java_${jobType}_combine_5",
        publishToInfluxDB   : true,
        sourceOptions       : """
                                {
                                  "numRecords": 2500000,
                                  "keySizeBytes": 10,
                                  "valueSizeBytes": 90
                                }
                              """.trim().replaceAll("\\s", ""),
        fanout              : 8,
        iterations          : 1,
        topCount            : 20,
        perKeyCombiner      : "TOP_LARGEST",
        streaming           : isStreaming
      ]
    ]
  ].each { test -> test.pipelineOptions.putAll(additionalPipelineArgs) }
}


def batchLoadTestJob = { scope, triggeringContext ->
  def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', triggeringContext)
  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.JAVA, commonLoadTestConfig('batch', false, datasetName), "Combine", "batch")
}

CronJobBuilder.cronJob('beam_LoadTests_Java_Combine_SparkStructuredStreaming_Batch', 'H 12 * * *', this) {
  additionalPipelineArgs = [
    influxDatabase: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influxHost: InfluxDBCredentialsHelper.InfluxDBHostname,
  ]
  batchLoadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Java_Combine_SparkStructuredStreaming_Batch',
    'Run Load Tests Java Combine SparkStructuredStreaming Batch',
    'Load Tests Java Combine SparkStructuredStreaming Batch suite',
    this
    ) {
      additionalPipelineArgs = [:]
      batchLoadTestJob(delegate, CommonTestProperties.TriggeringContext.PR)
    }
