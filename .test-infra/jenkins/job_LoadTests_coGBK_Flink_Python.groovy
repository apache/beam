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
import Infrastructure as infra

String jenkinsJobName = 'beam_LoadTests_Python_coGBK_Flink_Batch'
String now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))
String dockerRegistryRoot = 'gcr.io/apache-beam-testing/beam_portability'
String dockerTag = 'latest'
String jobServerImageTag = "${dockerRegistryRoot}/flink-job-server:${dockerTag}"
String pythonHarnessImageTag = "${dockerRegistryRoot}/python:${dockerTag}"

String flinkVersion = '1.7'
String flinkDownloadUrl = 'https://archive.apache.org/dist/flink/flink-1.7.0/flink-1.7.0-bin-hadoop28-scala_2.11.tgz'

def loadTestConfigurations = { datasetName -> [
        [
                title        : 'CoGroupByKey Python Load test: 2GB of 100B records with a single key',
                itClass      : 'apache_beam.testing.load_tests.co_group_by_key_test:CoGroupByKeyTest.testCoGroupByKey',
                runner       : CommonTestProperties.Runner.PORTABLE,
                jobProperties: [
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
                        environment_config   : pythonHarnessImageTag,
                        environment_type     : 'DOCKER',
                ]
        ],
        [
                title        : 'CoGroupByKey Python Load test: 2GB of 100B records with multiple keys',
                itClass      : 'apache_beam.testing.load_tests.co_group_by_key_test:CoGroupByKeyTest.testCoGroupByKey',
                runner       : CommonTestProperties.Runner.PORTABLE,
                jobProperties: [
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
                        environment_config   : pythonHarnessImageTag,
                        environment_type     : 'DOCKER',
                ]
        ],
        [
                title        : 'CoGroupByKey Python Load test: reiterate 4 times 10kB values',
                itClass      : 'apache_beam.testing.load_tests.co_group_by_key_test:CoGroupByKeyTest.testCoGroupByKey',
                runner       : CommonTestProperties.Runner.PORTABLE,
                jobProperties: [
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
                        environment_config   : pythonHarnessImageTag,
                        environment_type     : 'DOCKER',
                ]
        ],
        [
                title        : 'CoGroupByKey Python Load test: reiterate 4 times 2MB values',
                itClass      : 'apache_beam.testing.load_tests.co_group_by_key_test:CoGroupByKeyTest.testCoGroupByKey',
                runner       : CommonTestProperties.Runner.PORTABLE,
                jobProperties: [
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
                        environment_config   : pythonHarnessImageTag,
                        environment_type     : 'DOCKER',
                ]
        ],
]}

def loadTest = { scope, triggeringContext ->
  scope.description('Runs Python coGBK load tests on Flink runner in batch mode')
  commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 240)

  def numberOfWorkers = 5
  def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', triggeringContext)

  infra.prepareSDKHarness(scope, CommonTestProperties.SDK.PYTHON, dockerRegistryRoot, dockerTag)
  infra.prepareFlinkJobServer(scope, flinkVersion, dockerRegistryRoot, dockerTag)
  infra.setupFlinkCluster(scope, jenkinsJobName, flinkDownloadUrl, pythonHarnessImageTag, jobServerImageTag, numberOfWorkers)

  for (config in loadTestConfigurations(datasetName)) {
    loadTestsBuilder.loadTest(scope, config.title, config.runner, CommonTestProperties.SDK.PYTHON, config.jobProperties, config.itClass)
  }

  infra.teardownDataproc(scope, jenkinsJobName)
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
