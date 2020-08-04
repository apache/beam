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
import LoadTestsBuilder as loadTestsBuilder
import PhraseTriggeringPostCommitBuilder
import InfluxDBCredentialsHelper

def now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

def loadTestConfigurations = { datasetName ->
  [
    [
      title          : 'Combine Python Load test: 2GB 10 byte records',
      test           : 'apache_beam.testing.load_tests.combine_test',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : 'load-tests-python-dataflow-batch-combine-1-' + now,
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/smoketests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : 'python_dataflow_batch_combine_1',
        influx_measurement   : 'python_batch_combine_1',
        input_options        : '\'{' +
        '"num_records": 200000000,' +
        '"key_size": 1,' +
        '"value_size": 9}\'',
        num_workers          : 5,
        autoscaling_algorithm: "NONE",
        top_count            : 20,
      ]
    ],
    [
      title          : 'Combine Python Load test: 2GB Fanout 4',
      test           : 'apache_beam.testing.load_tests.combine_test',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : 'load-tests-python-dataflow-batch-combine-4-' + now,
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/smoketests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : 'python_dataflow_batch_combine_4',
        influx_measurement   : 'python_batch_combine_4',
        input_options        : '\'{' +
        '"num_records": 5000000,' +
        '"key_size": 10,' +
        '"value_size": 90}\'',
        num_workers          : 16,
        autoscaling_algorithm: "NONE",
        fanout               : 4,
        top_count            : 20,
      ]
    ],
    [
      title          : 'Combine Python Load test: 2GB Fanout 8',
      test           : 'apache_beam.testing.load_tests.combine_test',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : 'load-tests-python-dataflow-batch-combine-5-' + now,
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/smoketests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : 'python_dataflow_batch_combine_5',
        influx_measurement   : 'python_batch_combine_5',
        input_options        : '\'{' +
        '"num_records": 2500000,' +
        '"key_size": 10,' +
        '"value_size": 90}\'',
        num_workers          : 16,
        autoscaling_algorithm: "NONE",
        fanout               : 8,
        top_count            : 20,
      ]
    ],
  ].each { test -> test.pipelineOptions.putAll(additionalPipelineArgs) }
}

def batchLoadTestJob = { scope, triggeringContext ->
  scope.description('Runs Python Combine load tests on Dataflow runner in batch mode')
  commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 120)

  def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', triggeringContext)
  for (testConfiguration in loadTestConfigurations(datasetName)) {
    loadTestsBuilder.loadTest(scope, testConfiguration.title, testConfiguration.runner, CommonTestProperties.SDK.PYTHON_37, testConfiguration.pipelineOptions, testConfiguration.test)
  }
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Python_Combine_Dataflow_Batch',
    'Run Python Load Tests Combine Dataflow Batch',
    'Load Tests Python Combine Dataflow Batch suite',
    this
    ) {
      additionalPipelineArgs = [:]
      batchLoadTestJob(delegate, CommonTestProperties.TriggeringContext.PR)
    }

CronJobBuilder.cronJob('beam_LoadTests_Python_Combine_Dataflow_Batch', 'H 15 * * *', this) {
  additionalPipelineArgs = [
    influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostname,
  ]
  batchLoadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT)
}
