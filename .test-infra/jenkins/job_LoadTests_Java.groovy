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
import CronJobBuilder

def loadTestConfigurations = [
        [
                title        : 'Load test: 2GB of 10B records',
                itClass      : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                runner       : CommonTestProperties.Runner.DATAFLOW,
                jobProperties: [
                        project             : 'apache-beam-testing',
                        appName             : 'load_tests_Java_Dataflow_Batch_GBK_1',
                        tempLocation        : 'gs://temp-storage-for-perf-tests/loadtests',
                        publishToBigQuery   : true,
                        bigQueryDataset     : 'load_test',
                        bigQueryTable       : 'java_dataflow_batch_GBK_1',
                        sourceOptions       : """
                                            {
                                              "numRecords": 200000000,
                                              "keySizeBytes": 1,
                                              "valueSizeBytes": 9
                                            }
                                       """.trim().replaceAll("\\s", ""),
                        fanout              : 1,
                        iterations          : 1,
                        maxNumWorkers       : 5,
                        numWorkers          : 5,
                        autoscalingAlgorithm: "NONE"
                ]
        ],
        [
                title        : 'Load test: 2GB of 100B records',
                itClass      : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                runner       : CommonTestProperties.Runner.DATAFLOW,
                jobProperties: [
                        project             : 'apache-beam-testing',
                        appName             : 'load_tests_Java_Dataflow_Batch_GBK_2',
                        tempLocation        : 'gs://temp-storage-for-perf-tests/loadtests',
                        publishToBigQuery   : true,
                        bigQueryDataset     : 'load_test',
                        bigQueryTable       : 'java_dataflow_batch_GBK_2',
                        sourceOptions       : """
                                            {
                                              "numRecords": 20000000,
                                              "keySizeBytes": 10,
                                              "valueSizeBytes": 90
                                            }
                                       """.trim().replaceAll("\\s", ""),
                        fanout              : 1,
                        iterations          : 1,
                        maxNumWorkers       : 5,
                        numWorkers          : 5,
                        autoscalingAlgorithm: "NONE"
                ]
        ],
        [

                title        : 'Load test: 2GB of 100kB records',
                itClass      : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                runner       : CommonTestProperties.Runner.DATAFLOW,
                jobProperties: [
                        project             : 'apache-beam-testing',
                        appName             : 'load_tests_Java_Dataflow_Batch_GBK_3',
                        tempLocation        : 'gs://temp-storage-for-perf-tests/loadtests',
                        publishToBigQuery   : true,
                        bigQueryDataset     : 'load_test',
                        bigQueryTable       : 'java_dataflow_batch_GBK_3',
                        sourceOptions       : """
                                            {
                                              "numRecords": 2000,
                                              "keySizeBytes": 100000,
                                              "valueSizeBytes": 900000
                                            }
                                       """.trim().replaceAll("\\s", ""),
                        fanout              : 1,
                        iterations          : 1,
                        maxNumWorkers       : 5,
                        numWorkers          : 5,
                        autoscalingAlgorithm: "NONE"
                ]

        ],
        [
                title        : 'Load test: fanout 4 times with 2GB 10-byte records total',
                itClass      : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                runner       : CommonTestProperties.Runner.DATAFLOW,
                jobProperties: [
                        project             : 'apache-beam-testing',
                        appName             : 'load_tests_Java_Dataflow_Batch_GBK_4',
                        tempLocation        : 'gs://temp-storage-for-perf-tests/loadtests',
                        publishToBigQuery   : true,
                        bigQueryDataset     : 'load_test',
                        bigQueryTable       : 'java_dataflow_batch_GBK_4',
                        sourceOptions       : """
                                            {
                                              "numRecords": 5000000,
                                              "keySizeBytes": 10,
                                              "valueSizeBytes": 90
                                            }
                                       """.trim().replaceAll("\\s", ""),
                        fanout              : 4,
                        iterations          : 1,
                        maxNumWorkers       : 16,
                        numWorkers          : 16,
                        autoscalingAlgorithm: "NONE"
                ]
        ],
        [
                title        : 'Load test: fanout 8 times with 2GB 10-byte records total',
                itClass      : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                runner       : CommonTestProperties.Runner.DATAFLOW,
                jobProperties: [
                        project             : 'apache-beam-testing',
                        appName             : 'load_tests_Java_Dataflow_Batch_GBK_5',
                        tempLocation        : 'gs://temp-storage-for-perf-tests/loadtests',
                        publishToBigQuery   : true,
                        bigQueryDataset     : 'load_test',
                        bigQueryTable       : 'java_dataflow_batch_GBK_5',
                        sourceOptions       : """
                                            {
                                              "numRecords": 2500000,
                                              "keySizeBytes": 10,
                                              "valueSizeBytes": 90
                                            }
                                       """.trim().replaceAll("\\s", ""),
                        fanout              : 8,
                        iterations          : 1,
                        maxNumWorkers       : 16,
                        numWorkers          : 16,
                        autoscalingAlgorithm: "NONE"
                ]
        ],
        [
                title        : 'Load test: reiterate 4 times 10kB values',
                itClass      : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                runner       : CommonTestProperties.Runner.DATAFLOW,
                jobProperties: [
                        project             : 'apache-beam-testing',
                        appName             : 'load_tests_Java_Dataflow_Batch_GBK_6',
                        tempLocation        : 'gs://temp-storage-for-perf-tests/loadtests',
                        publishToBigQuery   : true,
                        bigQueryDataset     : 'load_test',
                        bigQueryTable       : 'java_dataflow_batch_GBK_6',
                        sourceOptions       : """
                                            {
                                              "numRecords": 20000000,
                                              "keySizeBytes": 10,
                                              "valueSizeBytes": 90,
                                              "numHotKeys": 200,
                                              "hotKeyFraction": 1
                                            }
                                       """.trim().replaceAll("\\s", ""),
                        fanout              : 1,
                        iterations          : 4,
                        maxNumWorkers       : 5,
                        numWorkers          : 5,
                        autoscalingAlgorithm: "NONE"
                ]
        ],
        [
                title        : 'Load test: reiterate 4 times 2MB values',
                itClass      : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                runner       : CommonTestProperties.Runner.DATAFLOW,
                jobProperties: [
                        project             : 'apache-beam-testing',
                        appName             : 'load_tests_Java_Dataflow_Batch_GBK_7',
                        tempLocation        : 'gs://temp-storage-for-perf-tests/loadtests',
                        publishToBigQuery   : true,
                        bigQueryDataset     : 'load_test',
                        bigQueryTable       : 'java_dataflow_batch_GBK_7',
                        sourceOptions       : """
                                            {
                                              "numRecords": 20000000,
                                              "keySizeBytes": 10,
                                              "valueSizeBytes": 90,
                                              "numHotKeys": 10,
                                              "hotKeyFraction": 1
                                            }
                                       """.trim().replaceAll("\\s", ""),
                        fanout              : 1,
                        iterations          : 4,
                        maxNumWorkers       : 5,
                        numWorkers          : 5,
                        autoscalingAlgorithm: "NONE"
                ]
        ]
]

def loadTestJob = { scope, triggeringContext ->
  scope.description('Runs Java GBK load tests on Dataflow runner in batch mode')
  commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 240)

  for (testConfiguration in loadTestConfigurations) {
    loadTestsBuilder.loadTest(scope, testConfiguration.title, testConfiguration.runner, CommonTestProperties.SDK.JAVA, testConfiguration.jobProperties, testConfiguration.itClass, triggeringContext)
  }
}

CronJobBuilder.cronJob('beam_LoadTests_Java_GBK_Dataflow_Batch', 'H 12 * * *', this) {
  loadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
        'beam_LoadTests_Java_GBK_Dataflow_Batch',
        'Run Load Tests Java GBK Dataflow Batch',
        'Load Tests Java GBK Dataflow Batch suite',
        this
) {
  loadTestJob(delegate, CommonTestProperties.TriggeringContext.PR)
}


