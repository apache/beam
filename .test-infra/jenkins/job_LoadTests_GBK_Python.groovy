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

import LoadTestsBuilder as loadTestsBuilder
import PhraseTriggeringPostCommitBuilder
import InfluxDBCredentialsHelper

def now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

def loadTestConfigurations = { datasetName ->
  [
    [
      title          : 'GroupByKey Python Load test: 2GB of 10B records',
      test           : 'apache_beam.testing.load_tests.group_by_key_test',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : 'load-tests-python-dataflow-batch-gbk-1-' + now,
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : 'python_dataflow_batch_gbk_1',
        influx_measurement   : 'python_batch_gbk_1',
        input_options        : '\'{"num_records": 200000000,' +
        '"key_size": 1,' +
        '"value_size": 9}\'',
        iterations           : 1,
        fanout               : 1,
        num_workers          : 5,
        autoscaling_algorithm: "NONE"
      ]
    ],
    [
      title          : 'GroupByKey Python Load test: 2GB of 100B records',
      test           : 'apache_beam.testing.load_tests.group_by_key_test',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : 'load-tests-python-dataflow-batch-gbk-2-' + now,
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : 'python_dataflow_batch_gbk_2',
        influx_measurement   : 'python_batch_gbk_2',
        input_options        : '\'{"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90}\'',
        iterations           : 1,
        fanout               : 1,
        num_workers          : 5,
        autoscaling_algorithm: "NONE"
      ]
    ],
    [
      title          : 'GroupByKey Python Load test: 2GB of 100kB records',
      test           : 'apache_beam.testing.load_tests.group_by_key_test',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : 'load-tests-python-dataflow-batch-gbk-3-' + now,
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : 'python_dataflow_batch_gbk_3',
        influx_measurement   : 'python_batch_gbk_3',
        input_options        : '\'{"num_records": 20000,' +
        '"key_size": 10000,' +
        '"value_size": 90000}\'',
        iterations           : 1,
        fanout               : 1,
        num_workers          : 5,
        autoscaling_algorithm: "NONE"
      ]
    ],
    [
      title          : 'GroupByKey Python Load test: fanout 4 times with 2GB 10-byte records total',
      test           : 'apache_beam.testing.load_tests.group_by_key_test',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : 'load-tests-python-dataflow-batch-gbk-4-' + now,
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : 'python_dataflow_batch_gbk_4',
        influx_measurement   : 'python_batch_gbk_4',
        input_options        : '\'{"num_records": 5000000,' +
        '"key_size": 10,' +
        '"value_size": 90}\'',
        iterations           : 1,
        fanout               : 4,
        num_workers          : 5,
        autoscaling_algorithm: "NONE"
      ]
    ],
    [
      title          : 'GroupByKey Python Load test: fanout 8 times with 2GB 10-byte records total',
      test           : 'apache_beam.testing.load_tests.group_by_key_test',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : 'load-tests-python-dataflow-batch-gbk-5-' + now,
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : 'python_dataflow_batch_gbk_5',
        influx_measurement   : 'python_batch_gbk_5',
        input_options        : '\'{"num_records": 2500000,' +
        '"key_size": 10,' +
        '"value_size": 90}\'',
        iterations           : 1,
        fanout               : 8,
        num_workers          : 5,
        autoscaling_algorithm: "NONE"
      ]
    ],
  ].each { test -> test.pipelineOptions.putAll(additionalPipelineArgs) }
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Python_GBK_Dataflow_Batch',
    'Run Load Tests Python GBK Dataflow Batch',
    'Load Tests Python GBK Dataflow Batch suite',
    this
    ) {
      additionalPipelineArgs = [:]
      def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', CommonTestProperties.TriggeringContext.PR)
      loadTestsBuilder.loadTests(delegate, CommonTestProperties.SDK.PYTHON_37, loadTestConfigurations(datasetName), "GBK", "batch")
    }

CronJobBuilder.cronJob('beam_LoadTests_Python_GBK_Dataflow_Batch', 'H 12 * * *', this) {
  additionalPipelineArgs = [
    influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostname,
  ]
  def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', CommonTestProperties.TriggeringContext.POST_COMMIT)
  loadTestsBuilder.loadTests(delegate, CommonTestProperties.SDK.PYTHON_37, loadTestConfigurations(datasetName), "GBK", "batch")
}

