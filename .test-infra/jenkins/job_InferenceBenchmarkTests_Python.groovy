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
import CronJobBuilder

def now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

// Common pipeline args for Dataflow job.
def dataflowPipelineArgs = [
  project         : 'apache-beam-testing',
  region          : 'us-central1',
  staging_location: 'gs://temp-storage-for-perf-tests/loadtests',
]

// define all the inference benchmarks here
def torch_imagenet_classification = [
  title       : 'Pytorch Imagenet Classification',
  test        : 'apache_beam.testing.benchmarks.pytorch_benchmark_test',
  runner      : CommonTestProperties.Runner.DATAFLOW,
  pipelineOptions: [
    job_name              : 'performance-tests-pytorch-imagenet-python' + now,
    project               : 'apache-beam-testing',
    region                : 'us-central1',
    temp_location         : 'gs://temp-storage-for-perf-tests/loadtests',
    publish_to_bigquery   : true,
    metrics_dataset       : 'beam_performance',
    influx_measurement    : 'python_torch_imagenet_mobilenet_results',
    influx_db_name        : InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname       : InfluxDBCredentialsHelper.InfluxDBHostUrl,
    input_options         : '{}' // option is required in LoadTest. Bypass this.
    // args defined in the example.
    input_file            : 'gs://apache-beam-ml/testing/inputs/imagenet_validation_inputs.txt',
    model_state_dict_path : 'gs://apache-beam-ml/models/imagenet_classification_mobilenet_v2.pt',
  ]
]

def executeJob = { scope, testConfig ->
  commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 480)
  loadTestsBuilder.loadTest(scope, testConfig.title, testConfig.runner, CommonTestProperties.SDK.PYTHON, testConfig.pipelineOptions, testConfig.test)
}

// Benchmark tests
PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_Inference_Python_Benchmarks_Dataflow',
    'Run Inference Benchmarks',
    'Inference benchmarks on Dataflow(\"Run Inference Benchmarks"\"")',
    this
    ) {
      executeJob(delegate, torch_imagenet_classification)
    }

CronJobBuilder.cronJob('beam_Inference_Python_Benchmarks_Dataflow', 'H 12 * * *', this) {
  executeJob(delegate, torch_imagenet_classification)
}


