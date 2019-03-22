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
import TestingInfra as infrastructure

def loadTestConfigurations = [
        [
                title        : 'Load test: 2GB of 10B records',
                itClass      : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                runner       : CommonTestProperties.Runner.FLINK,
                jobProperties: [
                        project             : 'apache-beam-testing',
                        appName             : 'load_tests_Java_Dataflow_Batch_Flink_GBK_1',
                        tempLocation        : 'gs://temp-storage-for-perf-tests/loadtests',
                        publishToBigQuery   : true,
                        bigQueryDataset     : 'load_test',
                        bigQueryTable       : 'java_dataflow_batch_flink_GBK_1',
                        sourceOptions       : """
                                            {
                                              "numRecords": 1000,
                                              "keySizeBytes": 1,
                                              "valueSizeBytes": 9
                                            }
                                       """.trim().replaceAll("\\s", ""),
                        fanout              : 1,
                        iterations          : 1,
                        parallelism         : 1,
                        flinkMaster         : 'localhost:8081'
                ]
        ]
]

def loadTestJob = { scope, jobName, triggeringContext ->
  scope.description('Runs Java GBK load tests on Flink runner in batch mode')
  commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 240)

  String repositoryRoot = 'gcr.io/apache-beam-io-testing/beam_fnapi'
  String javaImage = infrastructure.prepareSDKHarness(scope, CommonTestProperties.SDK.JAVA, repositoryRoot, 'latest')

  infrastructure.setupFlinkCluster(scope, jobName, 5, javaImage)

  for (testConfiguration in loadTestConfigurations) {
    loadTestsBuilder.loadTest(scope, testConfiguration.title, testConfiguration.runner, CommonTestProperties.SDK.JAVA, testConfiguration.jobProperties, testConfiguration.itClass, triggeringContext)
  }

  infrastructure.teardownDataproc(scope, jobName)
}


def jobName = 'beam_LoadTests_Java_GBK_Flink_Batch'

PhraseTriggeringPostCommitBuilder.postCommitJob(
        jobName,
        'Run Load Tests Java GBK Flink Batch',
        'Load Tests Java GBK Flink Batch suite',
        this
) {
  loadTestJob(delegate, jobName, CommonTestProperties.TriggeringContext.PR)
}


