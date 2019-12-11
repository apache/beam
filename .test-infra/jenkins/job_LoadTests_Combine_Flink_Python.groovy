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
                title          : 'Combine Python Load test: 2GB 10 byte records',
                test           : 'apache_beam.testing.load_tests.combine_test:CombineTest.testCombineGlobally',
                runner         : CommonTestProperties.Runner.PORTABLE,
                pipelineOptions: [
                        job_name            : 'load-tests-python-flink-batch-combine-1-' + now,
                        project             : 'apache-beam-testing',
                        publish_to_big_query: true,
                        metrics_dataset     : datasetName,
                        metrics_table       : 'python_flink_batch_combine_1',
                        input_options       : '\'{' +
                                '"num_records": 200000000,' +
                                '"key_size": 1,' +
                                '"value_size": 9}\'',
                        parallelism         : 5,
                        job_endpoint        : 'localhost:8099',
                        environment_config  : sdkHarnessImageTag,
                        environment_type    : 'DOCKER',
                        top_count           : 20,
                ]
        ],
        [
                title          : 'Combine Python Load test: 2GB Fanout 4',
                test           : 'apache_beam.testing.load_tests.combine_test:CombineTest.testCombineGlobally',
                runner         : CommonTestProperties.Runner.PORTABLE,
                pipelineOptions: [
                        job_name            : 'load-tests-python-flink-batch-combine-4-' + now,
                        project             : 'apache-beam-testing',
                        publish_to_big_query: true,
                        metrics_dataset     : datasetName,
                        metrics_table       : 'python_flink_batch_combine_4',
                        input_options       : '\'{' +
                                '"num_records": 5000000,' +
                                '"key_size": 10,' +
                                '"value_size": 90}\'',
                        parallelism         : 16,
                        job_endpoint        : 'localhost:8099',
                        environment_config  : sdkHarnessImageTag,
                        environment_type    : 'DOCKER',
                        fanout              : 4,
                        top_count           : 20,
                ]
        ],
        [
                title          : 'Combine Python Load test: 2GB Fanout 8',
                test           : 'apache_beam.testing.load_tests.combine_test:CombineTest.testCombineGlobally',
                runner         : CommonTestProperties.Runner.PORTABLE,
                pipelineOptions: [
                        job_name            : 'load-tests-python-flink-batch-combine-5-' + now,
                        project             : 'apache-beam-testing',
                        publish_to_big_query: true,
                        metrics_dataset     : datasetName,
                        metrics_table       : 'python_flink_batch_combine_5',
                        input_options       : '\'{' +
                                '"num_records": 2500000,' +
                                '"key_size": 10,' +
                                '"value_size": 90}\'',
                        parallelism         : 16,
                        job_endpoint        : 'localhost:8099',
                        environment_config  : sdkHarnessImageTag,
                        environment_type    : 'DOCKER',
                        fanout              : 8,
                        top_count           : 20,
                ]
        ]
]}

def batchLoadTestJob = { scope, triggeringContext ->
    scope.description('Runs Python Combine load tests on Flink runner in batch mode')
    commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 240)

    Docker publisher = new Docker(scope, loadTestsBuilder.DOCKER_CONTAINER_REGISTRY)
    String pythonHarnessImageTag = publisher.getFullImageName('python2.7_sdk')

    def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', triggeringContext)
    def numberOfWorkers = 16
    List<Map> testScenarios = scenarios(datasetName, pythonHarnessImageTag)

    publisher.publish(':sdks:python:container:py2:docker', 'python2.7_sdk')
    publisher.publish(':runners:flink:1.9:job-server-container:docker', 'flink1.9_job_server')
    def flink = new Flink(scope, 'beam_LoadTests_Python_Combine_Flink_Batch')
    flink.setUp([pythonHarnessImageTag], numberOfWorkers, publisher.getFullImageName('flink1.9_job_server'))

    defineTestSteps(scope, testScenarios, [
            'Combine Python Load test: 2GB Fanout 4',
            'Combine Python Load test: 2GB Fanout 8'
    ])

    numberOfWorkers = 5
    flink.scaleCluster(numberOfWorkers)

    defineTestSteps(scope, testScenarios, ['Combine Python Load test: 2GB 10 byte records'])
}

private List<Map> defineTestSteps(scope, List<Map> testScenarios, List<String> titles) {
    return testScenarios
            .findAll { it.title in titles }
            .forEach {
                loadTestsBuilder.loadTest(scope, it.title, it.runner, CommonTestProperties.SDK.PYTHON, it.pipelineOptions, it.test)
            }
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
        'beam_LoadTests_Python_Combine_Flink_Batch',
        'Run Load Tests Python Combine Flink Batch',
        'Load Tests Python Combine Flink Batch suite',
        this
) {
    batchLoadTestJob(delegate, CommonTestProperties.TriggeringContext.PR)
}

CronJobBuilder.cronJob('beam_LoadTests_Python_Combine_Flink_Batch', 'H 15 * * *', this) {
    batchLoadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT)
}
