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
import LoadTestsBuilder
import PhraseTriggeringPostCommitBuilder
import LoadTestConfig
import SideInputTestSuite

import static LoadTestConfig.templateConfig
import static LoadTestConfig.fromTemplate


LoadTestConfig template = templateConfig {
    test 'apache_beam.testing.load_tests.sideinput_test'
    dataflow()
    pipelineOptions {
        python()
        project 'apache-beam-testing'
        publishToBigQuery true
        numWorkers 5
        autoscalingAlgorithm 'NONE'
        tempLocation 'gs://temp-storage-for-perf-tests/loadtests'
    }
}

def batchLoadTestJob = { scope, triggeringContext ->
    String datasetName = LoadTestsBuilder.getBigQueryDataset('load_test', triggeringContext)
    def configurations = SideInputTestSuite.configurations(template)
    def now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

    def finalConfigurations = []
    configurations.eachWithIndex { configuration, i ->
        finalConfigurations.add(
            fromTemplate(configuration) {
                pipelineOptions {
                    jobName "load-tests-python-dataflow-batch-sideinput-${i + 1}-" + now
                    metricsDataset datasetName
                    metricsTable "python_dataflow_batch_sideinput_${i + 1}"
                }
            }
        )
    }

    LoadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.PYTHON, finalConfigurations, 'SideInput', 'batch')
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
        'beam_LoadTests_Python_SideInput_Dataflow_Batch',
        'Run Python Load Tests SideInput Dataflow Batch',
        'Load Tests Python SideInput Dataflow Batch suite',
        this
) {
    batchLoadTestJob(delegate, CommonTestProperties.TriggeringContext.PR)
}

CronJobBuilder.cronJob('beam_LoadTests_Python_SideInput_Dataflow_Batch', 'H 11 * * *', this) {
    batchLoadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT)
}
