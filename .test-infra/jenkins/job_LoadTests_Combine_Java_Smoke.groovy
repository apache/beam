
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

import static LoadTestConfig.fromTemplate
import static LoadTestConfig.templateConfig

def smokeTestConfigurations = { datasetName ->
  def template = templateConfig {
    title 'CombineLoadTest load test Dataflow-1'
    test 'org.apache.beam.sdk.loadtests.CombineLoadTest'
    dataflow()
    pipelineOptions {
      java()
      appName 'smoke-dsl-java'
      project 'apache-beam-testing'
      tempLocation 'gs://temp-storage-for-perf-tests/smoketests'
      publishToBigQuery true
      bigQueryDataset datasetName
      bigQueryTable 'dataflow_combine'
      numWorkers 5
      autoscalingAlgorithm 'NONE'
      sourceOptions {
        numRecords 100000
        splitPointFrequencyRecords 1
      }
      stepOptions {
        outputRecordsPerInputRecord 1
        preservesInputKeyDistribution true
      }
      specificParameters([
        fanout: 10,
        iterations: 1
      ])
    }
  }
  [
    fromTemplate(template),
    fromTemplate(template) {
      title 'CombineLoadTest load test Dataflow-2'
      pipelineOptions {
        numWorkers 3
        specificParameters([
          fanout: 1
        ])
      }
    },
    fromTemplate(template) {
      title 'CombineLoadTest load test Dataflow-3'
      pipelineOptions {
        sourceOptions {
          numRecords 20000
        }
      }
    },
  ]
}



// Runs a tiny version load test suite to ensure nothing is broken.
PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_Java_LoadTests_Combine_Smoke',
    'Run Java Load Tests Combine Smoke',
    'Java Load Tests Combine Smoke',
    this
    ) {
      def datasetName = loadTestsBuilder.getBigQueryDataset('load_test_SMOKE', CommonTestProperties.TriggeringContext.PR)
      loadTestsBuilder.loadTests(delegate, CommonTestProperties.SDK.JAVA, smokeTestConfigurations(datasetName), "Combine", "smoke")
    }
