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

String jenkinsJobName = 'beam_LoadTests_Python_GBK_Flink_Batch'
String now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))
String dockerRegistryRoot = 'gcr.io/apache-beam-testing/beam_portability'
String dockerTag = 'latest'
String jobServerImageTag = "${dockerRegistryRoot}/flink-job-server:${dockerTag}"
String pythonHarnessImageTag = "${dockerRegistryRoot}/python:${dockerTag}"

String flinkVersion = '1.7'
String flinkDownloadUrl = 'https://archive.apache.org/dist/flink/flink-1.7.0/flink-1.7.0-bin-hadoop28-scala_2.11.tgz'

int parallelism = 5

def testConfiguration = { datasetName -> [
        [
                title        : 'Load test: 2GB of 10B records',
                itClass      : 'apache_beam.testing.load_tests.group_by_key_test:GroupByKeyTest.testGroupByKey',
                runner       : CommonTestProperties.Runner.PORTABLE,
                sdk          : CommonTestProperties.SDK.PYTHON,
                jobProperties: [
                        job_name            : "load_tests_Python_Flink_Batch_GBK_1_${now}",
                        publish_to_big_query: false,
                        project             : 'apache-beam-testing',
                        metrics_dataset     : datasetName,
                        metrics_table       : "python_flink_batch_GBK_1",
                        input_options       : '\'{"num_records": 200000000,"key_size": 1,"value_size":9}\'',
                        iterations          : 1,
                        fanout              : 1,
                        parallelism         : parallelism,
                        job_endpoint: 'localhost:8099',
                        environment_config : pythonHarnessImageTag,
                        environment_type: 'DOCKER'

                ]
        ],
        [
                title        : 'Load test: 2GB of 100B records',
                itClass      : 'apache_beam.testing.load_tests.group_by_key_test:GroupByKeyTest.testGroupByKey',
                runner       : CommonTestProperties.Runner.PORTABLE,
                sdk          : CommonTestProperties.SDK.PYTHON,
                jobProperties: [
                        job_name            : "load_tests_Python_Flink_Batch_GBK_2_${now}",
                        publish_to_big_query: false,
                        project             : 'apache-beam-testing',
                        metrics_dataset     : datasetName,
                        metrics_table       : "python_flink_batch_GBK_2",
                        input_options       : '\'{"num_records": 20000000,"key_size": 10,"value_size":90}\'',
                        iterations          : 1,
                        fanout              : 1,
                        parallelism         : parallelism,
                        job_endpoint: 'localhost:8099',
                        environment_config : pythonHarnessImageTag,
                        environment_type: 'DOCKER'

                ]
        ],
        [
                title        : 'Load test: 2GB of 100kB records',
                itClass      : 'apache_beam.testing.load_tests.group_by_key_test:GroupByKeyTest.testGroupByKey',
                runner       : CommonTestProperties.Runner.PORTABLE,
                sdk          : CommonTestProperties.SDK.PYTHON,
                jobProperties: [
                        job_name            : "load_tests_Python_Flink_Batch_GBK_3_${now}",
                        publish_to_big_query: false,
                        project             : 'apache-beam-testing',
                        metrics_dataset     : datasetName,
                        metrics_table       : "python_flink_batch_GBK_3",
                        input_options       : '\'{"num_records": 2000,"key_size": 100000,"value_size":900000}\'',
                        iterations          : 1,
                        fanout              : 1,
                        parallelism         : parallelism,
                        job_endpoint: 'localhost:8099',
                        environment_config : pythonHarnessImageTag,
                        environment_type: 'DOCKER'

                ]
        ],
        [
                title        : 'Load test: fanout 4 times with 2GB 10-byte records total',
                itClass      : 'apache_beam.testing.load_tests.group_by_key_test:GroupByKeyTest.testGroupByKey',
                runner       : CommonTestProperties.Runner.PORTABLE,
                sdk          : CommonTestProperties.SDK.PYTHON,
                jobProperties: [
                        job_name            : "load_tests_Python_Flink_Batch_GBK_4_${now}",
                        publish_to_big_query: false,
                        project             : 'apache-beam-testing',
                        metrics_dataset     : datasetName,
                        metrics_table       : "python_flink_batch_GBK_4",
                        input_options       : '\'{"num_records": 5000000,"key_size": 10,"value_size":90}\'',
                        iterations          : 1,
                        fanout              : 4,
                        parallelism         : parallelism,
                        job_endpoint: 'localhost:8099',
                        environment_config : pythonHarnessImageTag,
                        environment_type: 'DOCKER'

                ]
        ],
        [
                title        : 'Load test: fanout 8 times with 2GB 10-byte records total',
                itClass      : 'apache_beam.testing.load_tests.group_by_key_test:GroupByKeyTest.testGroupByKey',
                runner       : CommonTestProperties.Runner.PORTABLE,
                sdk          : CommonTestProperties.SDK.PYTHON,
                jobProperties: [
                        job_name            : "load_tests_Python_Flink_Batch_GBK_5_${now}",
                        publish_to_big_query: false,
                        project             : 'apache-beam-testing',
                        metrics_dataset     : datasetName,
                        metrics_table       : "python_flink_batch_GBK_5",
                        input_options       : '\'{"num_records": 2500000,"key_size": 10,"value_size":90}\'',
                        iterations          : 1,
                        fanout              : 8,
                        parallelism         : parallelism,
                        job_endpoint: 'localhost:8099',
                        environment_config : pythonHarnessImageTag,
                        environment_type: 'DOCKER'

                ]
        ],
        [
                title        : 'Load test: reiterate 4 times 10kB values',
                itClass      : 'apache_beam.testing.load_tests.group_by_key_test:GroupByKeyTest.testGroupByKey',
                runner       : CommonTestProperties.Runner.PORTABLE,
                sdk          : CommonTestProperties.SDK.PYTHON,
                jobProperties: [
                        job_name            : "load_tests_Python_Flink_Batch_GBK_6_${now}",
                        publish_to_big_query: false,
                        project             : 'apache-beam-testing',
                        metrics_dataset     : datasetName,
                        metrics_table       : "python_flink_batch_GBK_5",
                        input_options       : '\'{"num_records": 20000000,"key_size": 10,"value_size":90, "num_hot_keys": 200, "hot_key_fraction": 1}\'',
                        iterations          : 4,
                        fanout              : 1,
                        parallelism         : 5,
                        job_endpoint: 'localhost:8099',
                        environment_config : pythonHarnessImageTag,
                        environment_type: 'DOCKER'

                ]
        ],
        [
                title        : 'Load test: reiterate 4 times 2MB values',
                itClass      : 'apache_beam.testing.load_tests.group_by_key_test:GroupByKeyTest.testGroupByKey',
                runner       : CommonTestProperties.Runner.PORTABLE,
                sdk          : CommonTestProperties.SDK.PYTHON,
                jobProperties: [
                        job_name            : "load_tests_Python_Flink_Batch_GBK_7_${now}",
                        publish_to_big_query: false,
                        project             : 'apache-beam-testing',
                        metrics_dataset     : datasetName,
                        metrics_table       : "python_flink_batch_GBK_5",
                        input_options       : '\'{"num_records": 20000000,"key_size": 10,"value_size":90, "num_hot_keys": 10, "hot_key_fraction": 1}\'',
                        iterations          : 4,
                        fanout              : 1,
                        parallelism         : 5,
                        job_endpoint: 'localhost:8099',
                        environment_config : pythonHarnessImageTag,
                        environment_type: 'DOCKER'

                ]
        ],
    ]}

def loadTest = { scope, triggeringContext ->
  scope.description('Runs Python GBK load tests on Flink runner in batch mode')
  commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 240)

  def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', triggeringContext)
  def sdkName = CommonTestProperties.SDK.PYTHON

  infra.prepareSDKHarness(scope, sdkName, dockerRegistryRoot, 'latest')
  infra.prepareFlinkJobServer(scope, flinkVersion, dockerRegistryRoot, 'latest')
  infra.setupFlinkCluster(scope, jenkinsJobName, flinkDownloadUrl, pythonHarnessImageTag, jobServerImageTag, parallelism)

  loadTestsBuilder.loadTests(scope, sdkName,  testConfiguration(datasetName), "GBK", "batch")

  infra.teardownDataproc(scope, jenkinsJobName)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
        'beam_LoadTests_Python_GBK_Flink_Batch',
        'Run Load Tests Python GBK Flink Batch',
        'Load Tests Python GBK Flink Batch suite',
        this
) {
  loadTest(delegate, CommonTestProperties.TriggeringContext.PR)
}

CronJobBuilder.cronJob('beam_LoadTests_Python_GBK_Flink_Batch', 'H 12 * * *', this) {
  loadTest(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT)
}
