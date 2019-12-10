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

import CommonTestProperties
import LoadTestsBuilder as loadTestsBuilder
import PhraseTriggeringPostCommitBuilder
import CronJobBuilder

def loadTestConfigurations = { mode, isStreaming, datasetName, sdkHarnessImageTag ->
    [
            [
                    title          : 'Load test: 2GB of 10B records on Flink in Portable mode',
                    test           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                    runner         : CommonTestProperties.Runner.PORTABLE,
                    pipelineOptions: [
                            project               : 'apache-beam-testing',
                            appName               : "load_tests_Java_Flink_${mode}_GBK_1",
                            tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
                            publishToBigQuery     : true,
                            bigQueryDataset       : datasetName,
                            bigQueryTable         : "java_flink_${mode}_GBK_1",
                            sourceOptions         : """
                                            {
                                              "numRecords": 200000000,
                                              "keySizeBytes": 1,
                                              "valueSizeBytes": 9
                                            }
                                       """.trim().replaceAll("\\s", ""),
                            fanout                : 1,
                            iterations            : 1,
                            sdkWorkerParallelism            : 5,
                            streaming             : isStreaming,
                            jobEndpoint          : 'localhost:8099',
                            defaultEnvironmentConfig    : sdkHarnessImageTag,
                            defaultEnvironmentType      : 'DOCKER'
                    ]
            ],
            [
                    title          : 'Load test: 2GB of 100B records',
                    test           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                    runner         : CommonTestProperties.Runner.PORTABLE,
                    pipelineOptions: [
                            project               : 'apache-beam-testing',
                            appName               : "load_tests_Java_Flink_${mode}_GBK_2",
                            tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
                            publishToBigQuery     : true,
                            bigQueryDataset       : datasetName,
                            bigQueryTable         : "java_flink_${mode}_GBK_2",
                            sourceOptions         : """
                                            {
                                              "numRecords": 20000000,
                                              "keySizeBytes": 10,
                                              "valueSizeBytes": 90
                                            }
                                       """.trim().replaceAll("\\s", ""),
                            fanout                : 1,
                            iterations            : 1,
                            sdkWorkerParallelism            : 5,
                            streaming             : isStreaming,
                            jobEndpoint          : 'localhost:8099',
                            defaultEnvironmentConfig    : sdkHarnessImageTag,
                            defaultEnvironmentType      : 'DOCKER'
                    ]
            ],
            [

                    title          : 'Load test: 2GB of 100kB records',
                    test           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                    runner         : CommonTestProperties.Runner.PORTABLE,
                    pipelineOptions: [
                            project               : 'apache-beam-testing',
                            appName               : "load_tests_Java_Flink_${mode}_GBK_3",
                            tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
                            publishToBigQuery     : true,
                            bigQueryDataset       : datasetName,
                            bigQueryTable         : "java_flink_${mode}_GBK_3",
                            sourceOptions         : """
                                            {
                                              "numRecords": 2000,
                                              "keySizeBytes": 100000,
                                              "valueSizeBytes": 900000
                                            }
                                       """.trim().replaceAll("\\s", ""),
                            fanout                : 1,
                            iterations            : 1,
                            sdkWorkerParallelism            : 5,
                            streaming             : isStreaming,
                            jobEndpoint          : 'localhost:8099',
                            defaultEnvironmentConfig    : sdkHarnessImageTag,
                            defaultEnvironmentType      : 'DOCKER'
                    ]

            ],
            [
                    title          : 'Load test: fanout 4 times with 2GB 10-byte records total',
                    test           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                    runner         : CommonTestProperties.Runner.PORTABLE,
                    pipelineOptions: [
                            project               : 'apache-beam-testing',
                            appName               : "load_tests_Java_Flink_${mode}_GBK_4",
                            tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
                            publishToBigQuery     : true,
                            bigQueryDataset       : datasetName,
                            bigQueryTable         : "java_flink_${mode}_GBK_4",
                            sourceOptions         : """
                                            {
                                              "numRecords": 5000000,
                                              "keySizeBytes": 10,
                                              "valueSizeBytes": 90
                                            }
                                       """.trim().replaceAll("\\s", ""),
                            fanout                : 4,
                            iterations            : 1,
                            sdkWorkerParallelism            : 16,
                            streaming             : isStreaming,
                            jobEndpoint          : 'localhost:8099',
                            defaultEnvironmentConfig    : sdkHarnessImageTag,
                            defaultEnvironmentType      : 'DOCKER'
                    ]
            ],
            [
                    title          : 'Load test: fanout 8 times with 2GB 10-byte records total',
                    test           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                    runner         : CommonTestProperties.Runner.PORTABLE,
                    pipelineOptions: [
                            project               : 'apache-beam-testing',
                            appName               : "load_tests_Java_Flink_${mode}_GBK_5",
                            tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
                            publishToBigQuery     : true,
                            bigQueryDataset       : datasetName,
                            bigQueryTable         : "java_flink_${mode}_GBK_5",
                            sourceOptions         : """
                                            {
                                              "numRecords": 2500000,
                                              "keySizeBytes": 10,
                                              "valueSizeBytes": 90
                                            }
                                       """.trim().replaceAll("\\s", ""),
                            fanout                : 8,
                            iterations            : 1,
                            sdkWorkerParallelism            : 16,
                            streaming             : isStreaming,
                            jobEndpoint          : 'localhost:8099',
                            defaultEnvironmentConfig    : sdkHarnessImageTag,
                            defaultEnvironmentType      : 'DOCKER'
                    ]
            ],
            [
                    title          : 'Load test: reiterate 4 times 10kB values',
                    test           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                    runner         : CommonTestProperties.Runner.PORTABLE,
                    pipelineOptions: [
                            project               : 'apache-beam-testing',
                            appName               : "load_tests_Java_Flink_${mode}_GBK_6",
                            tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
                            publishToBigQuery     : true,
                            bigQueryDataset       : datasetName,
                            bigQueryTable         : "java_flink_${mode}_GBK_6",
                            sourceOptions         : """
                                            {
                                              "numRecords": 20000000,
                                              "keySizeBytes": 10,
                                              "valueSizeBytes": 90,
                                              "numHotKeys": 200,
                                              "hotKeyFraction": 1
                                            }
                                       """.trim().replaceAll("\\s", ""),
                            fanout                : 1,
                            iterations            : 4,
                            sdkWorkerParallelism            : 5,
                            streaming             : isStreaming,
                            jobEndpoint          : 'localhost:8099',
                            defaultEnvironmentConfig    : sdkHarnessImageTag,
                            defaultEnvironmentType      : 'DOCKER'
                    ]
            ],
            [
                    title          : 'Load test: reiterate 4 times 2MB values',
                    test           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                    runner         : CommonTestProperties.Runner.PORTABLE,
                    pipelineOptions: [
                            project               : 'apache-beam-testing',
                            appName               : "load_tests_Java_Flink_${mode}_GBK_7",
                            tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
                            publishToBigQuery     : true,
                            bigQueryDataset       : datasetName,
                            bigQueryTable         : "java_flink_${mode}_GBK_7",
                            sourceOptions         : """
                                            {
                                              "numRecords": 20000000,
                                              "keySizeBytes": 10,
                                              "valueSizeBytes": 90,
                                              "numHotKeys": 10,
                                              "hotKeyFraction": 1
                                            }
                                       """.trim().replaceAll("\\s", ""),
                            fanout                : 1,
                            iterations            : 4,
                            sdkWorkerParallelism            : 5,
                            streaming             : isStreaming,
                            jobEndpoint          : 'localhost:8099',
                            defaultEnvironmentConfig    : sdkHarnessImageTag,
                            defaultEnvironmentType      : 'DOCKER'
                    ]
            ]
    ]
}

def loadTest = { scope, triggeringContext, mode, isStreaming ->
    Docker publisher = new Docker(scope, loadTestsBuilder.DOCKER_CONTAINER_REGISTRY)
    def sdk = CommonTestProperties.SDK.JAVA
    String javaHarnessImageTag = publisher.getFullImageName('java_sdk')

    def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', triggeringContext)
    def numberOfWorkers = 16
    List<Map> testScenarios = loadTestConfigurations(mode, isStreaming, datasetName, javaHarnessImageTag)

    publisher.publish(':sdks:java:container:docker', 'java_sdk')
    publisher.publish(':runners:flink:1.9:job-server-container:docker', 'flink1.9_job_server')
    def flink = new Flink(scope, 'beam_LoadTests_Python_GBK_Flink_Batch')
    flink.setUp([javaHarnessImageTag], numberOfWorkers, publisher.getFullImageName('flink1.9_job_server'))

    def configurations = testScenarios.findAll { it.pipelineOptions?.sdkWorkerParallelism?.value == numberOfWorkers }
    loadTestsBuilder.loadTests(scope, sdk, configurations, "GBK", "batch")
    if (isStreaming) {
        for (config in configurations) {
            config.pipelineOptions << [inputWindowDurationSec: 1200]
        }
    }
    numberOfWorkers = 5
    flink.scaleCluster(numberOfWorkers)

    configurations = testScenarios.findAll { it.pipelineOptions?.sdkWorkerParallelism?.value == numberOfWorkers }
    if (isStreaming) {
        for (config in configurations) {
            config.pipelineOptions << [inputWindowDurationSec: 1200]
        }
    }
    loadTestsBuilder.loadTests(scope, sdk, configurations, "GBK", mode)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
        'beam_LoadTests_Java_GBK_Flink_Streaming',
        'Run Load Tests Java GBK Flink Streaming',
        'Load Tests Java GBK Flink Streaming suite',
        this
) {
  loadTest(delegate, CommonTestProperties.TriggeringContext.PR, 'streaming', true)
}

CronJobBuilder.cronJob('beam_LoadTests_Java_GBK_Flink_Batch', 'H 14 * * *', this) {
    loadTest(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT, 'batch', false)
}

CronJobBuilder.cronJob('beam_LoadTests_Java_GBK_Flink_Streaming', 'H 12 * * *', this) {
    loadTest(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT, 'streaming', true)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
        'beam_LoadTests_Java_GBK_Flink_Batch',
        'Run Load Tests Java GBK Flink Batch',
        'Load Tests Java GBK Flink Batch suite',
        this
) {
    loadTest(delegate, CommonTestProperties.TriggeringContext.PR, 'batch', false)
}
