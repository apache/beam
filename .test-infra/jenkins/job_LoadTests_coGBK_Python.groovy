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

def now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

def loadTestConfigurations = { mode, datasetName ->
  [
    [
      title          : 'CoGroupByKey Python Load test: 2GB of 100B records with a single key',
      test           : 'apache_beam.testing.load_tests.co_group_by_key_test',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        job_name             : "load-tests-python-dataflow-${mode}-cogbk-1-${now}",
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : "python_dataflow_${mode}_cogbk_1",
        influx_measurement   : "python_${mode}_cogbk_1",
        input_options        : '\'{' +
        '"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 1,' +
        '"hot_key_fraction": 1}\'',
        co_input_options      : '\'{' +
        '"num_records": 2000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 1000,' +
        '"hot_key_fraction": 1}\'',
        iterations           : 1,
        num_workers          : 5,
        autoscaling_algorithm: 'NONE'
      ]
    ],
    [
      title          : 'CoGroupByKey Python Load test: 2GB of 100B records with multiple keys',
      test           : 'apache_beam.testing.load_tests.co_group_by_key_test',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        job_name             : "load-tests-python-dataflow-${mode}-cogbk-2-${now}",
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : "python_dataflow_${mode}_cogbk_2",
        influx_measurement   : "python_${mode}_cogbk_2",
        input_options        : '\'{' +
        '"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 5,' +
        '"hot_key_fraction": 1}\'',
        co_input_options      : '\'{' +
        '"num_records": 2000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 1000,' +
        '"hot_key_fraction": 1}\'',
        iterations           : 1,
        num_workers          : 5,
        autoscaling_algorithm: 'NONE'
      ]
    ],
    [
      title          : 'CoGroupByKey Python Load test: reiterate 4 times 10kB values',
      test           : 'apache_beam.testing.load_tests.co_group_by_key_test',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        job_name             : "load-tests-python-dataflow-${mode}-cogbk-3-${now}",
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : "python_dataflow_${mode}_cogbk_3",
        influx_measurement   : "python_${mode}_cogbk_3",
        input_options        : '\'{' +
        '"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 200000,' +
        '"hot_key_fraction": 1}\'',
        co_input_options      : '\'{' +
        '"num_records": 2000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 1000,' +
        '"hot_key_fraction": 1}\'',
        iterations           : 4,
        num_workers          : 5,
        autoscaling_algorithm: 'NONE'
      ]
    ],
    [
      title          : 'CoGroupByKey Python Load test: reiterate 4 times 2MB values',
      test           : 'apache_beam.testing.load_tests.co_group_by_key_test',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        job_name             : "load-tests-python-dataflow-${mode}-cogbk-4-${now}",
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : "python_dataflow_${mode}_cogbk_4",
        influx_measurement   : "python_${mode}_cogbk_4",
        input_options        : '\'{' +
        '"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 1000,' +
        '"hot_key_fraction": 1}\'',
        co_input_options      : '\'{' +
        '"num_records": 2000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 1000,' +
        '"hot_key_fraction": 1}\'',
        iterations           : 4,
        num_workers          : 5,
        autoscaling_algorithm: 'NONE'
      ]
    ],
  ]
  .each { test -> test.pipelineOptions.putAll(additionalPipelineArgs) }
  .each { test -> (mode != 'streaming') ?: addStreamingOptions(test) }
}

def addStreamingOptions(test) {
  // Use highmem workers to prevent out of memory issues.
  test.pipelineOptions << [streaming: null,
    worker_machine_type: 'n1-highmem-4'
  ]
}

def loadTestJob = { scope, triggeringContext, mode ->
  def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', triggeringContext)
  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.PYTHON_37,
      loadTestConfigurations(mode, datasetName), 'CoGBK', mode)
}

CronJobBuilder.cronJob('beam_LoadTests_Python_CoGBK_Dataflow_Batch', 'H 16 * * *', this) {
  additionalPipelineArgs = [
    influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostname,
  ]
  loadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT, 'batch')
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Python_CoGBK_Dataflow_Batch',
    'Run Load Tests Python CoGBK Dataflow Batch',
    'Load Tests Python CoGBK Dataflow Batch suite',
    this
    ) {
      additionalPipelineArgs = [:]
      loadTestJob(delegate, CommonTestProperties.TriggeringContext.PR, 'batch')
    }

CronJobBuilder.cronJob('beam_LoadTests_Python_CoGBK_Dataflow_Streaming', 'H 16 * * *', this) {
  additionalPipelineArgs = [
    influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostname,
  ]
  loadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT, 'streaming')
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Python_CoGBK_Dataflow_Streaming',
    'Run Load Tests Python CoGBK Dataflow Streaming',
    'Load Tests Python CoGBK Dataflow Streaming suite',
    this
    ) {
      additionalPipelineArgs = [:]
      loadTestJob(delegate, CommonTestProperties.TriggeringContext.PR, 'streaming')
    }
