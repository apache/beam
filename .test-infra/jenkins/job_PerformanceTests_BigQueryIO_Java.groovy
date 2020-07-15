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

import CommonJobProperties as common
import PhraseTriggeringPostCommitBuilder
import InfluxDBCredentialsHelper

def now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

def jobConfigs = [
  [
    title        : 'BigQueryIO Streaming Performance Test Java 10 GB',
    triggerPhrase: 'Run BigQueryIO Streaming Performance Test Java',
    name         : 'beam_BiqQueryIO_Streaming_Performance_Test_Java',
    itClass      : 'org.apache.beam.sdk.bigqueryioperftests.BigQueryIOIT',
    properties: [
      project               : 'apache-beam-testing',
      tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
      tempRoot              : 'gs://temp-storage-for-perf-tests/loadtests',
      writeMethod           : 'STREAMING_INSERTS',
      writeFormat           : 'JSON',
      testBigQueryDataset   : 'beam_performance',
      testBigQueryTable     : 'bqio_write_10GB_java_stream_' + now,
      metricsBigQueryDataset: 'beam_performance',
      metricsBigQueryTable  : 'bqio_10GB_results_java_stream',
      influxMeasurement     : 'bqio_10GB_results_java_stream',
      sourceOptions         : """
                                {
                                  "numRecords": "10485760",
                                  "keySizeBytes": "1",
                                  "valueSizeBytes": "1024"
                                }
                              """.trim().replaceAll("\\s", ""),
      runner                : 'DataflowRunner',
      maxNumWorkers         : '5',
      numWorkers            : '5',
      autoscalingAlgorithm  : 'NONE',
    ]
  ],
  [
    title        : 'BigQueryIO Batch Performance Test Java 10 GB JSON',
    triggerPhrase: 'Run BigQueryIO Batch Performance Test Java Json',
    name         : 'beam_BiqQueryIO_Batch_Performance_Test_Java_Json',
    itClass      : 'org.apache.beam.sdk.bigqueryioperftests.BigQueryIOIT',
    properties: [
      project               : 'apache-beam-testing',
      tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
      tempRoot              : 'gs://temp-storage-for-perf-tests/loadtests',
      writeMethod           : 'FILE_LOADS',
      writeFormat           : 'JSON',
      testBigQueryDataset   : 'beam_performance',
      testBigQueryTable     : 'bqio_write_10GB_java_json_' + now,
      metricsBigQueryDataset: 'beam_performance',
      metricsBigQueryTable  : 'bqio_10GB_results_java_batch_json',
      influxMeasurement     : 'bqio_10GB_results_java_batch_json',
      sourceOptions         : """
                                {
                                  "numRecords": "10485760",
                                  "keySizeBytes": "1",
                                  "valueSizeBytes": "1024"
                                }
                              """.trim().replaceAll("\\s", ""),
      runner                : "DataflowRunner",
      maxNumWorkers         : '5',
      numWorkers            : '5',
      autoscalingAlgorithm  : 'NONE',
    ]
  ],
  [
    title        : 'BigQueryIO Batch Performance Test Java 10 GB AVRO',
    triggerPhrase: 'Run BigQueryIO Batch Performance Test Java Avro',
    name         : 'beam_BiqQueryIO_Batch_Performance_Test_Java_Avro',
    itClass      : 'org.apache.beam.sdk.bigqueryioperftests.BigQueryIOIT',
    properties: [
      project               : 'apache-beam-testing',
      tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
      tempRoot              : 'gs://temp-storage-for-perf-tests/loadtests',
      writeMethod           : 'FILE_LOADS',
      writeFormat           : 'AVRO',
      testBigQueryDataset   : 'beam_performance',
      testBigQueryTable     : 'bqio_write_10GB_java_avro_' + now,
      metricsBigQueryDataset: 'beam_performance',
      metricsBigQueryTable  : 'bqio_10GB_results_java_batch_avro',
      influxMeasurement     : 'bqio_10GB_results_java_batch_avro',
      sourceOptions         : """
                                {
                                  "numRecords": "10485760",
                                  "keySizeBytes": "1",
                                  "valueSizeBytes": "1024"
                                }
                              """.trim().replaceAll("\\s", ""),
      runner                : "DataflowRunner",
      maxNumWorkers         : '5',
      numWorkers            : '5',
      autoscalingAlgorithm  : 'NONE',
    ]
  ]
]

jobConfigs.forEach { jobConfig -> createPostCommitJob(jobConfig)}

private void createPostCommitJob(jobConfig) {
  job(jobConfig.name) {
    description(jobConfig.description)
    common.setTopLevelMainJobProperties(delegate)
    common.enablePhraseTriggeringFromPullRequest(delegate, jobConfig.title, jobConfig.triggerPhrase)
    common.setAutoJob(delegate, 'H */6 * * *')
    publishers {
      archiveJunit('**/build/test-results/**/*.xml')
    }
    InfluxDBCredentialsHelper.useCredentials(delegate)
    additionalPipelineArgs = [
      influxDatabase: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
      influxHost: InfluxDBCredentialsHelper.InfluxDBHostname,
    ]
    jobConfig.properties.putAll(additionalPipelineArgs)

    steps {
      gradle {
        rootBuildScriptDir(common.checkoutDir)
        common.setGradleSwitches(delegate)
        switches("--info")
        switches("-DintegrationTestPipelineOptions=\'${common.joinOptionsWithNestedJsonValues(jobConfig.properties)}\'")
        switches("-DintegrationTestRunner=dataflow")
        tasks(":sdks:java:io:bigquery-io-perf-tests:integrationTest --tests ${jobConfig.itClass}")
      }
    }
  }
}
