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

def loadTestConfigurations = { mode, isStreaming, datasetName ->
  [
    [
      title          : 'Load test: CoGBK 2GB 100  byte records - single key',
      test           : 'org.apache.beam.sdk.loadtests.CoGroupByKeyLoadTest',
      runner         : CommonTestProperties.Runner.SPARK_STRUCTURED_STREAMING,
      pipelineOptions: [
        project               : 'apache-beam-testing',
        appName               : "load_tests_Java_SparkStructuredStreaming_${mode}_CoGBK_1",
        tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
        publishToBigQuery     : true,
        bigQueryDataset       : datasetName,
        bigQueryTable         : "java_sparkstructuredstreaming_${mode}_CoGBK_1",
        influxMeasurement     : "java_${mode}_cogbk_1",
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
        streaming             : isStreaming
      ]
    ],
    [
      title          : 'Load test: CoGBK 2GB 100 byte records - multiple keys',
      test           : 'org.apache.beam.sdk.loadtests.CoGroupByKeyLoadTest',
      runner         : CommonTestProperties.Runner.SPARK_STRUCTURED_STREAMING,
      pipelineOptions: [
        project               : 'apache-beam-testing',
        appName               : "load_tests_Java_SparkStructuredStreaming_${mode}_CoGBK_2",
        tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
        publishToBigQuery     : true,
        bigQueryDataset       : datasetName,
        bigQueryTable         : "java_sparkstructuredstreaming_${mode}_CoGBK_2",
        influxMeasurement     : "java_${mode}_cogbk_2",
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
        streaming             : isStreaming
      ]
    ],
    [

      title          : 'Load test: CoGBK 2GB reiteration 10kB value',
      test           : 'org.apache.beam.sdk.loadtests.CoGroupByKeyLoadTest',
      runner         : CommonTestProperties.Runner.SPARK_STRUCTURED_STREAMING,
      pipelineOptions: [
        project               : 'apache-beam-testing',
        appName               : "load_tests_Java_SparkStructuredStreaming_${mode}_CoGBK_3",
        tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
        publishToBigQuery     : true,
        bigQueryDataset       : datasetName,
        bigQueryTable         : "java_sparkstructuredstreaming_${mode}_CoGBK_3",
        influxMeasurement     : "java_${mode}_cogbk_3",
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
        streaming             : isStreaming
      ]

    ],
    [
      title          : 'Load test: CoGBK 2GB reiteration 2MB value',
      test           : 'org.apache.beam.sdk.loadtests.CoGroupByKeyLoadTest',
      runner         : CommonTestProperties.Runner.SPARK_STRUCTURED_STREAMING,
      pipelineOptions: [
        project               : 'apache-beam-testing',
        appName               : "load_tests_Java_SparkStructuredStreaming_${mode}_CoGBK_4",
        tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
        publishToBigQuery     : true,
        bigQueryDataset       : datasetName,
        bigQueryTable         : "java_sparkstructuredstreaming_${mode}_CoGBK_4",
        influxMeasurement     : "java_${mode}_cogbk_4",
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
        streaming             : isStreaming
      ]
    ]
  ].each { test -> test.pipelineOptions.putAll(additionalPipelineArgs) }
}

def batchLoadTestJob = { scope, triggeringContext ->
  def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', triggeringContext)
  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.JAVA, loadTestConfigurations('batch', false, datasetName), "CoGBK", "batch")
}

CronJobBuilder.cronJob('beam_LoadTests_Java_CoGBK_SparkStructuredStreaming_Batch', 'H 14 * * *', this) {
  additionalPipelineArgs = [
    influxDatabase: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influxHost: InfluxDBCredentialsHelper.InfluxDBHostUrl,
  ]
  batchLoadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Java_CoGBK_SparkStructuredStreaming_Batch',
    'Run Load Tests Java CoGBK SparkStructuredStreaming Batch',
    'Load Tests Java CoGBK SparkStructuredStreaming Batch suite',
    this
    ) {
      additionalPipelineArgs = [:]
      batchLoadTestJob(delegate, CommonTestProperties.TriggeringContext.PR)
    }
