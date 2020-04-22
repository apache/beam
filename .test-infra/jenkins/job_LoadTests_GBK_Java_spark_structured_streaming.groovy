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
import CronJobBuilder
import LoadTestsBuilder as loadTestsBuilder
import PhraseTriggeringPostCommitBuilder

def loadTestConfigurations = { mode, isStreaming, datasetName ->
    [
            [
                    title          : 'Load test: 2GB of 10B records',
                    test           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                    runner         : CommonTestProperties.Runner.SPARK_STRUCTURED_STREAMING,
                    pipelineOptions: [
                            project               : 'apache-beam-testing',
                            appName               : "load_tests_Java_SparkStructuredStreaming_${mode}_GBK_1",
                            tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
                            publishToBigQuery     : true,
                            bigQueryDataset       : datasetName,
                            bigQueryTable         : "java_sparkstructuredstreaming_${mode}_GBK_1",
                            sourceOptions         : """
                                            {
                                              "numRecords": 200000000,
                                              "keySizeBytes": 1,
                                              "valueSizeBytes": 9
                                            }
                                       """.trim().replaceAll("\\s", ""),
                            fanout                : 1,
                            iterations            : 1,
                            streaming             : isStreaming
                    ]
            ],
            [
                    title          : 'Load test: 2GB of 100B records',
                    test           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                    runner         : CommonTestProperties.Runner.SPARK_STRUCTURED_STREAMING,
                    pipelineOptions: [
                            project               : 'apache-beam-testing',
                            appName               : "load_tests_Java_SparkStructuredStreaming_${mode}_GBK_2",
                            tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
                            publishToBigQuery     : true,
                            bigQueryDataset       : datasetName,
                            bigQueryTable         : "java_SparkStructuredStreaming_${mode}_GBK_2",
                            sourceOptions         : """
                                            {
                                              "numRecords": 20000000,
                                              "keySizeBytes": 10,
                                              "valueSizeBytes": 90
                                            }
                                       """.trim().replaceAll("\\s", ""),
                            fanout                : 1,
                            iterations            : 1,
                            streaming             : isStreaming
                    ]
            ],
            [

                    title          : 'Load test: 2GB of 100kB records',
                    test           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                    runner         : CommonTestProperties.Runner.SPARK_STRUCTURED_STREAMING,
                    pipelineOptions: [
                            project               : 'apache-beam-testing',
                            appName               : "load_tests_Java_SparkStructuredStreaming_${mode}_GBK_3",
                            tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
                            publishToBigQuery     : true,
                            bigQueryDataset       : datasetName,
                            bigQueryTable         : "java_SparkStructuredStreaming_${mode}_GBK_3",
                            sourceOptions         : """
                                            {
                                              "numRecords": 20000,
                                              "keySizeBytes": 10000,
                                              "valueSizeBytes": 90000
                                            }
                                       """.trim().replaceAll("\\s", ""),
                            fanout                : 1,
                            iterations            : 1,
                            streaming             : isStreaming
                    ]

            ],
            [
                    title          : 'Load test: fanout 4 times with 2GB 10-byte records total',
                    test           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                    runner         : CommonTestProperties.Runner.SPARK_STRUCTURED_STREAMING,
                    pipelineOptions: [
                            project               : 'apache-beam-testing',
                            appName               : 'load_tests_Java_SparkStructuredStreaming_${mode}_GBK_4',
                            tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
                            publishToBigQuery     : true,
                            bigQueryDataset       : datasetName,
                            bigQueryTable         : "java_SparkStructuredStreaming_${mode}_GBK_4",
                            sourceOptions         : """
                                            {
                                              "numRecords": 5000000,
                                              "keySizeBytes": 10,
                                              "valueSizeBytes": 90
                                            }
                                       """.trim().replaceAll("\\s", ""),
                            fanout                : 4,
                            iterations            : 1,
                            streaming             : isStreaming
                    ]
            ],
            [
                    title          : 'Load test: fanout 8 times with 2GB 10-byte records total',
                    test           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                    runner         : CommonTestProperties.Runner.SPARK_STRUCTURED_STREAMING,
                    pipelineOptions: [
                            project               : 'apache-beam-testing',
                            appName               : "load_tests_Java_SparkStructuredStreaming_${mode}_GBK_5",
                            tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
                            publishToBigQuery     : true,
                            bigQueryDataset       : datasetName,
                            bigQueryTable         : "java_SparkStructuredStreaming_${mode}_GBK_5",
                            sourceOptions         : """
                                            {
                                              "numRecords": 2500000,
                                              "keySizeBytes": 10,
                                              "valueSizeBytes": 90
                                            }
                                       """.trim().replaceAll("\\s", ""),
                            fanout                : 8,
                            iterations            : 1,
                            streaming             : isStreaming
                    ]
            ],
            [
                    title          : 'Load test: reiterate 4 times 10kB values',
                    test           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                    runner         : CommonTestProperties.Runner.SPARK_STRUCTURED_STREAMING,
                    pipelineOptions: [
                            project               : 'apache-beam-testing',
                            appName               : "load_tests_Java_SparkStructuredStreaming_${mode}_GBK_6",
                            tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
                            publishToBigQuery     : true,
                            bigQueryDataset       : datasetName,
                            bigQueryTable         : "java_SparkStructuredStreaming_${mode}_GBK_6",
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
                            streaming             : isStreaming
                    ]
            ],
            [
                    title          : 'Load test: reiterate 4 times 2MB values',
                    test           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                    runner         : CommonTestProperties.Runner.SPARK_STRUCTURED_STREAMING,
                    pipelineOptions: [
                            project               : 'apache-beam-testing',
                            appName               : "load_tests_Java_SparkStructuredStreaming_${mode}_GBK_7",
                            tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
                            publishToBigQuery     : true,
                            bigQueryDataset       : datasetName,
                            bigQueryTable         : "java_SparkStructuredStreaming_${mode}_GBK_7",
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
                            streaming             : isStreaming
                    ]
            ]
    ]
}

def batchLoadTestJob = { scope, triggeringContext ->
    def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', triggeringContext)
    loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.JAVA, loadTestConfigurations('batch', false, datasetName), "GBK", "batch")
}
CronJobBuilder.cronJob('beam_LoadTests_Java_GBK_SparkStructuredStreaming_Batch', 'H 12 * * *', this) {
    batchLoadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
        'beam_LoadTests_Java_GBK_SparkStructuredStreaming_Batch',
        'Run Load Tests Java GBK SparkStructuredStreaming Batch',
        'Load Tests Java GBK SparkStructuredStreaming Batch suite',
        this
) {
    batchLoadTestJob(delegate, CommonTestProperties.TriggeringContext.PR)
}
