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

def loadTestConfigurations = { mode, isStreaming, datasetName, sdkHarnessImageTag ->
  [
          [
                  title          : 'Load test: 2GB of 10B records on Flink in Portable mode',
                  test           : 'org.apache.beam.sdk.loadtests.CombineLoadTest',
                  runner         : CommonTestProperties.Runner.PORTABLE,
                  pipelineOptions: [
                          project             : 'apache-beam-testing',
                          appName             : "load_tests_Java_Portable_Flink_${mode}_Combine_1",
                          tempLocation        : 'gs://temp-storage-for-perf-tests/loadtests',
                          publishToBigQuery   : true,
                          bigQueryDataset     : datasetName,
                          bigQueryTable       : "java_portable_flink_${mode}_Combine_1",
                          sourceOptions       : """
                                            {
                                              "numRecords": 200000000,
                                              "keySizeBytes": 1,
                                              "valueSizeBytes": 9,
                                                "delayDistribution": {
                                                  "type":"const",
                                                  "const":1
                                                  }
                                            }
                                       """.trim().replaceAll("\\s", ""),
                          fanout              : 1,
                          iterations          : 1,
                          topCount            : 20,
                          sdkWorkerParallelism: 5,
                          perKeyCombiner      : "TOP_LARGEST",
                          streaming           : isStreaming,
                          jobEndpoint         : 'localhost:8099',
                          defaultEnvironmentConfig    : sdkHarnessImageTag,
                          defaultEnvironmentType      : 'DOCKER',
                          experiments         : 'beam_fn_api'
                  ]
          ],
            [
                    title          : 'Load test: fanout 8 times with 1GB 10-byte records total on Flink in Portable mode',
                    test           : 'org.apache.beam.sdk.loadtests.CombineLoadTest',
                    runner         : CommonTestProperties.Runner.PORTABLE,
                    pipelineOptions: [
                            project             : 'apache-beam-testing',
                            appName             : "load_tests_Java_Portable_Flink_${mode}_Combine_5",
                            tempLocation        : 'gs://temp-storage-for-perf-tests/loadtests',
                            publishToBigQuery   : true,
                            bigQueryDataset     : datasetName,
                            bigQueryTable       : "java_portable_flink_${mode}_Combine_5",
                            sourceOptions       : """
                                                    {
                                                      "numRecords": 12500000,
                                                      "keySizeBytes": 1,
                                                      "valueSizeBytes": 9,
                                                      "delayDistribution": {
                                                        "type":"const",
                                                        "const":1
                                                        }
                                                    }
                                               """.trim().replaceAll("\\s", ""),
                            fanout              : 8,
                            iterations          : 1,
                            topCount            : 20,
                            sdkWorkerParallelism          : 16,
                            perKeyCombiner      : "TOP_LARGEST",
                            streaming           : isStreaming,
                            jobEndpoint          : 'localhost:8099',
                            defaultEnvironmentConfig    : sdkHarnessImageTag,
                            defaultEnvironmentType      : 'DOCKER',
                            experiments         : 'beam_fn_api'
                    ]
            ]
    ]
}


def loadTest = { scope, triggeringContext, mode, isStreaming ->
    Docker publisher = new Docker(scope, loadTestsBuilder.DOCKER_CONTAINER_REGISTRY)
    def sdk = CommonTestProperties.SDK.JAVA
    String javaHarnessImageTag = publisher.getFullImageName('beam_java_sdk')

    def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', triggeringContext)
    def numberOfWorkers = 16
    List<Map> testScenarios = loadTestConfigurations(mode, isStreaming, datasetName, javaHarnessImageTag)

    publisher.publish(':sdks:java:container:docker', 'beam_java_sdk')
    publisher.publish(':runners:flink:1.10:job-server-container:docker', 'beam_flink1.10_job_server')
    def flink = new Flink(scope, "beam_LoadTests_Java_Portable_Flink_$mode")
    flink.setUp([javaHarnessImageTag], numberOfWorkers, publisher.getFullImageName('beam_flink1.10_job_server'))

    def configurations = testScenarios.findAll { it.pipelineOptions?.sdkWorkerParallelism?.value == numberOfWorkers }
    if (isStreaming) {
      for (config in configurations) {
        config.pipelineOptions << [inputWindowDurationSec: 1200]
      }
    }
  loadTestsBuilder.loadTests(scope, sdk, configurations, "Combine", mode)
  numberOfWorkers = 5
    flink.scaleCluster(numberOfWorkers)

    configurations = testScenarios.findAll { it.pipelineOptions?.sdkWorkerParallelism?.value == numberOfWorkers }
    if (isStreaming) {
        for (config in configurations) {
            config.pipelineOptions << [inputWindowDurationSec: 1200]
        }
    }
    loadTestsBuilder.loadTests(scope, sdk, configurations, "Combine", mode)
}

CronJobBuilder.cronJob('beam_LoadTests_Java_Combine_Portable_Flink_Batch', 'H 12 * * *', this) {
    loadTest(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT, 'batch', false)
}

CronJobBuilder.cronJob('beam_LoadTests_Java_Combine_Portable_Flink_Streaming', 'H 12 * * *', this) {
    loadTest(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT, 'streaming', true)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
        'beam_LoadTests_Java_Combine_Portable_Flink_Batch',
        'Run Load Tests Java Combine Portable Flink Batch',
        'Load Tests Java Combine Flink Batch suite in Portable mode',
        this
) {
    loadTest(delegate, CommonTestProperties.TriggeringContext.PR, 'batch', false)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
        'beam_LoadTests_Java_Combine_Portable_Flink_Streaming',
        'Run Load Tests Java Combine Portable Flink Streaming',
        'Load Tests Java Combine Flink Streaming suite in Portable mode',
        this
) {
    loadTest(delegate, CommonTestProperties.TriggeringContext.PR, 'streaming', true)
}
