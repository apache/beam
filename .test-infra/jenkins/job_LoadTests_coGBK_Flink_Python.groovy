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
import Flink
import Docker

String now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

def scenarios = { datasetName, sdkHarnessImageTag -> [
        [
                title          : 'CoGroupByKey Python Load test: 2GB of 100B records with a single key',
                test           : 'apache_beam.testing.load_tests.co_group_by_key_test:CoGroupByKeyTest.testCoGroupByKey',
                runner         : CommonTestProperties.Runner.PORTABLE,
                pipelineOptions: [
                        project              : 'apache-beam-testing',
                        job_name             : 'load-tests-python-flink-batch-cogbk-1-' + now,
                        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
                        publish_to_big_query : true,
                        metrics_dataset      : datasetName,
                        metrics_table        : "python_flink_batch_cogbk_1",
                        input_options        : '\'{' +
                                '"num_records": 20000000,' +
                                '"key_size": 10,' +
                                '"value_size": 90,' +
                                '"num_hot_keys": 1,' +
                                '"hot_key_fraction": 1}\'',
                        co_input_options      : '\'{' +
                                '"num_records": 20000000,' +
                                '"key_size": 10,' +
                                '"value_size": 90,' +
                                '"num_hot_keys": 1,' +
                                '"hot_key_fraction": 1}\'',
                        iterations           : 1,
                        parallelism          : 5,
                        job_endpoint         : 'localhost:8099',
                        environment_config   : sdkHarnessImageTag,
                        environment_type     : 'DOCKER',
                ]
        ],
        [
                title          : 'CoGroupByKey Python Load test: 2GB of 100B records with multiple keys',
                test           : 'apache_beam.testing.load_tests.co_group_by_key_test:CoGroupByKeyTest.testCoGroupByKey',
                runner         : CommonTestProperties.Runner.PORTABLE,
                pipelineOptions: [
                        project              : 'apache-beam-testing',
                        job_name             : 'load-tests-python-flink-batch-cogbk-2-' + now,
                        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
                        publish_to_big_query : true,
                        metrics_dataset      : datasetName,
                        metrics_table        : 'python_flink_batch_cogbk_2',
                        input_options        : '\'{' +
                                '"num_records": 20000000,' +
                                '"key_size": 10,' +
                                '"value_size": 90,' +
                                '"num_hot_keys": 5,' +
                                '"hot_key_fraction": 1}\'',
                        co_input_options      : '\'{' +
                                '"num_records": 20000000,' +
                                '"key_size": 10,' +
                                '"value_size": 90,' +
                                '"num_hot_keys": 5,' +
                                '"hot_key_fraction": 1}\'',
                        iterations           : 1,
                        parallelism          : 5,
                        job_endpoint         : 'localhost:8099',
                        environment_config   : sdkHarnessImageTag,
                        environment_type     : 'DOCKER',
                ]
        ],
        [
                title          : 'CoGroupByKey Python Load test: reiterate 4 times 10kB values',
                test           : 'apache_beam.testing.load_tests.co_group_by_key_test:CoGroupByKeyTest.testCoGroupByKey',
                runner         : CommonTestProperties.Runner.PORTABLE,
                pipelineOptions: [
                        project              : 'apache-beam-testing',
                        job_name             : 'load-tests-python-flink-batch-cogbk-3-' + now,
                        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
                        publish_to_big_query : true,
                        metrics_dataset      : datasetName,
                        metrics_table        : "python_flink_batch_cogbk_3",
                        input_options        : '\'{' +
                                '"num_records": 20000000,' +
                                '"key_size": 10,' +
                                '"value_size": 90,' +
                                '"num_hot_keys": 200000,' +
                                '"hot_key_fraction": 1}\'',
                        co_input_options      : '\'{' +
                                '"num_records": 20000000,' +
                                '"key_size": 10,' +
                                '"value_size": 90,' +
                                '"num_hot_keys": 200000,' +
                                '"hot_key_fraction": 1}\'',
                        iterations           : 4,
                        parallelism          : 5,
                        job_endpoint         : 'localhost:8099',
                        environment_config   : sdkHarnessImageTag,
                        environment_type     : 'DOCKER',
                ]
        ],
        [
                title          : 'CoGroupByKey Python Load test: reiterate 4 times 2MB values',
                test           : 'apache_beam.testing.load_tests.co_group_by_key_test:CoGroupByKeyTest.testCoGroupByKey',
                runner         : CommonTestProperties.Runner.PORTABLE,
                pipelineOptions: [
                        project              : 'apache-beam-testing',
                        job_name             : 'load-tests-python-flink-batch-cogbk-4-' + now,
                        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
                        publish_to_big_query : true,
                        metrics_dataset      : datasetName,
                        metrics_table        : 'python_flink_batch_cogbk_4',
                        input_options        : '\'{' +
                                '"num_records": 20000000,' +
                                '"key_size": 10,' +
                                '"value_size": 90,' +
                                '"num_hot_keys": 1000,' +
                                '"hot_key_fraction": 1}\'',
                        co_input_options      : '\'{' +
                                '"num_records": 20000000,' +
                                '"key_size": 10,' +
                                '"value_size": 90,' +
                                '"num_hot_keys": 1000,' +
                                '"hot_key_fraction": 1}\'',
                        iterations           : 4,
                        parallelism          : 5,
                        job_endpoint         : 'localhost:8099',
                        environment_config   : sdkHarnessImageTag,
                        environment_type     : 'DOCKER',
                ]
        ],
]}

def loadTest = { scope, triggeringContext ->
  Docker publisher = new Docker(scope, loadTestsBuilder.DOCKER_CONTAINER_REGISTRY)
  String pythonHarnessImageTag = publisher.getFullImageName('python2.7_sdk')

  def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', triggeringContext)
  def numberOfWorkers = 5
  List<Map> testScenarios = scenarios(datasetName, pythonHarnessImageTag)

  publisher.publish(':sdks:python:container:py2:docker', 'python2.7_sdk')
  publisher.publish(':runners:flink:1.9:job-server-container:docker', 'flink1.9_job_server')
  def flink = new Flink(scope, 'beam_LoadTests_Python_CoGBK_Flink_Batch')
  flink.setUp([pythonHarnessImageTag], numberOfWorkers, publisher.getFullImageName('flink1.9_job_server'))

  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.PYTHON, testScenarios, 'CoGBK', 'batch')
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
        'beam_LoadTests_Python_CoGBK_Flink_Batch',
        'Run Load Tests Python CoGBK Flink Batch',
        'Load Tests Python CoGBK Flink Batch suite',
        this
) {
  loadTest(delegate, CommonTestProperties.TriggeringContext.PR)
}

CronJobBuilder.cronJob('beam_LoadTests_Python_CoGBK_Flink_Batch', 'H 16 * * *', this) {
  loadTest(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT)
}
