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
                title          : 'ParDo Python Load test: 2GB 100 byte records 10 times',
                test           : 'apache_beam.testing.load_tests.pardo_test:ParDoTest.testParDo',
                runner         : CommonTestProperties.Runner.PORTABLE,
                pipelineOptions: [
                        job_name             : 'load-tests-python-flink-batch-pardo-1-' + now,
                        project              : 'apache-beam-testing',
                        publish_to_big_query : true,
                        metrics_dataset      : datasetName,
                        metrics_table        : 'python_flink_batch_pardo_1',
                        input_options        : '\'{' +
                                '"num_records": 20000000,' +
                                '"key_size": 10,' +
                                '"value_size": 90}\'',
                        iterations           : 10,
                        number_of_counter_operations: 0,
                        number_of_counters   : 0,
                        parallelism          : 5,
                        job_endpoint         : 'localhost:8099',
                        environment_config   : sdkHarnessImageTag,
                        environment_type     : 'DOCKER',
                ]
        ],
        [
                title          : 'ParDo Python Load test: 2GB 100 byte records 200 times',
                test           : 'apache_beam.testing.load_tests.pardo_test:ParDoTest.testParDo',
                runner         : CommonTestProperties.Runner.PORTABLE,
                pipelineOptions: [
                        job_name             : 'load-tests-python-flink-batch-pardo-2-' + now,
                        project              : 'apache-beam-testing',
                        publish_to_big_query : true,
                        metrics_dataset      : datasetName,
                        metrics_table        : 'python_flink_batch_pardo_2',
                        input_options        : '\'{' +
                                '"num_records": 20000000,' +
                                '"key_size": 10,' +
                                '"value_size": 90}\'',
                        iterations           : 200,
                        number_of_counter_operations: 0,
                        number_of_counters   : 0,
                        parallelism          : 5,
                        job_endpoint         : 'localhost:8099',
                        environment_config   : sdkHarnessImageTag,
                        environment_type     : 'DOCKER',
                ]
        ],
        [
                title          : 'ParDo Python Load test: 2GB 100 byte records 10 counters',
                test           : 'apache_beam.testing.load_tests.pardo_test:ParDoTest.testParDo',
                runner         : CommonTestProperties.Runner.PORTABLE,
                pipelineOptions: [
                        job_name             : 'load-tests-python-flink-batch-pardo-3-' + now,
                        project              : 'apache-beam-testing',
                        publish_to_big_query : true,
                        metrics_dataset      : datasetName,
                        metrics_table        : 'python_flink_batch_pardo_3',
                        input_options        : '\'{' +
                                '"num_records": 20000000,' +
                                '"key_size": 10,' +
                                '"value_size": 90}\'',
                        iterations           : 1,
                        number_of_counter_operations: 10,
                        number_of_counters   : 1,
                        parallelism          : 5,
                        job_endpoint         : 'localhost:8099',
                        environment_config   : sdkHarnessImageTag,
                        environment_type     : 'DOCKER',
                ]
        ],
        [
                title          : 'ParDo Python Load test: 2GB 100 byte records 100 counters',
                test           : 'apache_beam.testing.load_tests.pardo_test:ParDoTest.testParDo',
                runner         : CommonTestProperties.Runner.PORTABLE,
                pipelineOptions: [
                        job_name             : 'load-tests-python-flink-batch-pardo-4-' + now,
                        project              : 'apache-beam-testing',
                        publish_to_big_query : true,
                        metrics_dataset      : datasetName,
                        metrics_table        : 'python_flink_batch_pardo_4',
                        input_options        : '\'{' +
                                '"num_records": 20000000,' +
                                '"key_size": 10,' +
                                '"value_size": 90}\'',
                        iterations           : 1,
                        number_of_counter_operations: 100,
                        number_of_counters   : 1,
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
  Flink flink = new Flink(scope, 'beam_LoadTests_Python_ParDo_Flink_Batch')
  flink.setUp([pythonHarnessImageTag], numberOfWorkers, publisher.getFullImageName('flink1.9_job_server'))

  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.PYTHON, testScenarios, 'ParDo', 'batch')
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
  'beam_LoadTests_Python_ParDo_Flink_Batch',
  'Run Python Load Tests ParDo Flink Batch',
  'Load Tests Python ParDo Flink Batch suite',
  this
) {
  loadTest(delegate, CommonTestProperties.TriggeringContext.PR)
}

CronJobBuilder.cronJob('beam_LoadTests_Python_ParDo_Flink_Batch', 'H 13 * * *', this) {
  loadTest(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT)
}
