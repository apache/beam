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

// (TODO): Add model name to experiments once decided on which models to use.
// Benchmark test config. Add multiple configs for multiple models.
def torch_imagenet_classification_resnet50 = [
  title             : 'Pytorch Imagenet Classification',
  test              : 'apache_beam.testing.benchmarks.inference.pytorch_benchmarks',
  runner            : CommonTestProperties.Runner.DATAFLOW,
  pipelineOptions: [
    job_name              : 'benchmark-tests-pytorch-imagenet-python' + now,
    project               : 'apache-beam-testing',
    region                : 'us-central1',
    staging_location      : 'gs://temp-storage-for-perf-tests/loadtests',
    temp_location         : 'gs://temp-storage-for-perf-tests/loadtests',
    requirements_file     : 'apache_beam/ml/inference/torch_tests_requirements.txt',
    publish_to_big_query  : true,
    metrics_dataset       : 'beam_run_inference',
    metrics_table         : 'torch_inference_imagenet_results_resnet50',
    input_options         : '{}', // this option is not required for RunInference tests.
    influx_measurement    : 'python_runinference_imagenet_classification_resnet50',
    influx_db_name        : InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname       : InfluxDBCredentialsHelper.InfluxDBHostUrl,
    // args defined in the performance test
    pretrained_model_name : 'resnet50',
    // args defined in the example.
    input_file            : 'gs://apache-beam-ml/testing/inputs/inference_vision_data_tmp.txt',
    images_dir            : 'gs://apache-beam-ml/testing/inputs',
    // TODO: make sure the model_state_dict_path weights are accurate.
    model_state_dict_path : 'gs://apache-beam-ml/models/torchvision.models.resnet50.pt',
    output                : 'gs://temp-storage-for-end-to-end-tests/torch/result.txt'
  ]
]

def executeJob = { scope, testConfig ->
  // TODO: Change the branch to nightly branch since Python SDK doesn't have Nightly snapshot
  commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 180)
  loadTestsBuilder.loadTest(scope, testConfig.title, testConfig.runner, CommonTestProperties.SDK.PYTHON, testConfig.pipelineOptions, testConfig.test, null, testConfig.pipelineOptions.requirements_file)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_Inference_Python_Benchmarks_Dataflow',
    'Run Inference Benchmarks',
    'Inference benchmarks on Dataflow(\"Run Inference Benchmarks"\"")',
    this
    ) {
      executeJob(delegate, torch_imagenet_classification_resnet50)
    }

// TODO(anandinguva): Change the cron job to run once a day
CronJobBuilder.cronJob('beam_Inference_Python_Benchmarks_Dataflow', 'H 2 * * *', this) {
  executeJob(delegate, torch_imagenet_classification_resnet50)
}


