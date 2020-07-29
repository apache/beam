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
      title          : 'Runtime Type Checking Python Load Test: On | Simple Type Hints',
      test           : 'python -m apache_beam.testing.load_tests.runtime_type_check_on_test_py3',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : 'load-tests-python-dataflow-batch-runtime-type-check-1-' + now,
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : 'python_dataflow_batch_runtime_type_check_1',
        influx_measurement   : 'python_batch_runtime_type_check_1',
        input_options        : '\'{"num_records": 300,' +
        '"key_size": 5,' +
        '"value_size": 15}\'',
        fanout               : 300,
        num_workers          : 5,
        autoscaling_algorithm: "NONE",
        nested_typehint: 0  // False
      ]
    ],
    [
      title          : 'Runtime Type Checking Python Load Test: Off | Simple Type Hints',
      test           : 'python -m apache_beam.testing.load_tests.runtime_type_check_off_test_py3',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : 'load-tests-python-dataflow-batch-runtime-type-check-2-' + now,
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : 'python_dataflow_batch_runtime_type_check_2',
        influx_measurement   : 'python_batch_runtime_type_check_2',
        input_options        : '\'{"num_records": 300,' +
        '"key_size": 5,' +
        '"value_size": 15}\'',
        fanout               : 300,
        num_workers          : 5,
        autoscaling_algorithm: "NONE",
        nested_typehint: 0  // False
      ]
    ],
    [
      title          : 'Runtime Type Checking Python Load Test: On | Nested Type Hints',
      test           : 'python -m apache_beam.testing.load_tests.runtime_type_check_on_test_py3',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : 'load-tests-python-dataflow-batch-runtime-type-check-3-' + now,
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : 'python_dataflow_batch_runtime_type_check_3',
        influx_measurement   : 'python_batch_runtime_type_check_3',
        input_options        : '\'{"num_records": 300,' +
        '"key_size": 5,' +
        '"value_size": 15}\'',
        fanout               : 300,
        num_workers          : 5,
        autoscaling_algorithm: "NONE",
        nested_typehint: 1  // True
      ]
    ],
    [
      title          : 'Runtime Type Checking Python Load Test: Off | Nested Type Hints',
      test           : 'python -m apache_beam.testing.load_tests.runtime_type_check_off_test_py3',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : 'load-tests-python-dataflow-batch-runtime-type-check-4-' + now,
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : 'python_dataflow_batch_runtime_type_check_4',
        influx_measurement   : 'python_batch_runtime_type_check_4',
        input_options        : '\'{"num_records": 300,' +
        '"key_size": 5,' +
        '"value_size": 15}\'',
        fanout               : 300,
        num_workers          : 5,
        autoscaling_algorithm: "NONE",
        nested_typehint: 1  // True
      ]
    ],
  ].each { test -> test.pipelineOptions.putAll(additionalPipelineArgs) }
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Python_RuntimeTypeChecking_Dataflow_Batch',
    'Run Load Tests Python RuntimeTypeChecking Dataflow Batch',
    'Load Tests Python RuntimeTypeChecking Dataflow Batch suite',
    this
    ) {
      additionalPipelineArgs = [:]
      def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', CommonTestProperties.TriggeringContext.PR)
      loadTestsBuilder.loadTests(delegate, CommonTestProperties.SDK.PYTHON_37, loadTestConfigurations(datasetName), "GBK", "batch")
    }

CronJobBuilder.cronJob('beam_LoadTests_Python_RuntimeTypeChecking_Dataflow_Batch', 'H 12 * * *', this) {
  additionalPipelineArgs = [
    influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostname,
  ]
  def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', CommonTestProperties.TriggeringContext.POST_COMMIT)
  loadTestsBuilder.loadTests(delegate, CommonTestProperties.SDK.PYTHON_37, loadTestConfigurations(datasetName), "GBK", "batch")
}

