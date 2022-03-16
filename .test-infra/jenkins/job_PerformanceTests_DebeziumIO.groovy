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
import InfluxDBCredentialsHelper

def now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

def loadTestConfigurations = { datasetName ->
  [
    [
      title          : 'Debezium  Python Load test: write and read data for 20 minutes',
      test           : 'apache_beam.testing.load_tests.debeziumIO_test',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        job_name             : "performance-tests-python-debezium-IO-${now}",
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        staging_location     : 'gs://temp-storage-for-perf-tests/loadtests',
        metrics_dataset     : datasetName,
        metrics_table       : 'python_direct_microbenchmarks',
        input_options        : '\'{}\'',
        iterations           : 1,
        num_workers          : 5,
        autoscaling_algorithm: 'NONE',
      ]
    ]
  ],    
}

def loadTestJob = { scope, triggeringContext ->
  scope.description("Performance test Debezium IO")

  def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', triggeringContext)
  for (testConfiguration in loadTestConfigurations(datasetName)) {
    loadTestsBuilder.loadTest(scope, testConfiguration.title, testConfiguration.runner, CommonTestProperties.SDK.PYTHON, testConfiguration.pipelineOptions, testConfiguration.test)
  }
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_Python_Performance_Test_DebeziumIO',
    'Run Python Performance Tests DebeziumIO',
    'Python Performance Tests DebeziumIO',
    this
    ) {
      additionalPipelineArgs = [:]
      loadTestJob(delegate, CommonTestProperties.TriggeringContext.PR)
    }

