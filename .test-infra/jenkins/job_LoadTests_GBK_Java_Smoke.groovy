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

def smokeTestConfigurations = { datasetName ->
  [
    [
      title          : 'GroupByKey load test Direct',
      test           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
      runner         : CommonTestProperties.Runner.DIRECT,
      pipelineOptions: [
        publishToBigQuery: true,
        bigQueryDataset  : datasetName,
        bigQueryTable    : 'direct_gbk',
        sourceOptions    : '{"numRecords":100000,"splitPointFrequencyRecords":1}',
        stepOptions      : '{"outputRecordsPerInputRecord":1,"preservesInputKeyDistribution":true}',
        fanout           : 10,
        iterations       : 1,
      ]
    ],
    [
      title          : 'GroupByKey load test Dataflow',
      test           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        project          : 'apache-beam-testing',
        region           : 'us-central1',
        tempLocation     : 'gs://temp-storage-for-perf-tests/smoketests',
        publishToBigQuery: true,
        bigQueryDataset  : datasetName,
        bigQueryTable    : 'dataflow_gbk',
        sourceOptions    : '{"numRecords":100000,"splitPointFrequencyRecords":1}',
        stepOptions      : '{"outputRecordsPerInputRecord":1,"preservesInputKeyDistribution":true}',
        fanout           : 10,
        iterations       : 1,
      ]
    ],
    [
      title          : 'GroupByKey load test Flink',
      test           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
      runner         : CommonTestProperties.Runner.FLINK,
      pipelineOptions: [
        publishToBigQuery: true,
        bigQueryDataset  : datasetName,
        bigQueryTable    : 'flink_gbk',
        sourceOptions    : '{"numRecords":100000,"splitPointFrequencyRecords":1}',
        stepOptions      : '{"outputRecordsPerInputRecord":1,"preservesInputKeyDistribution":true}',
        fanout           : 10,
        iterations       : 1,
      ]
    ],
    [
      title          : 'GroupByKey load test Spark',
      test           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
      runner         : CommonTestProperties.Runner.SPARK,
      pipelineOptions: [
        sparkMaster      : 'local[4]',
        publishToBigQuery: true,
        bigQueryDataset  : datasetName,
        bigQueryTable    : 'spark_gbk',
        sourceOptions    : '{"numRecords":100000,"splitPointFrequencyRecords":1}',
        stepOptions      : '{"outputRecordsPerInputRecord":1,"preservesInputKeyDistribution":true}',
        fanout           : 10,
        iterations       : 1,
      ]
    ]
  ]
}


// Runs a tiny version load test suite to ensure nothing is broken.
PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_Java_LoadTests_GBK_Smoke',
    'Run Java Load Tests GBK Smoke',
    'Java Load Tests GBK Smoke',
    this
    ) {
      def datasetName = loadTestsBuilder.getBigQueryDataset('load_test_SMOKE', CommonTestProperties.TriggeringContext.PR)
      loadTestsBuilder.loadTests(delegate, CommonTestProperties.SDK.JAVA, smokeTestConfigurations(datasetName), "GBK", "smoke")
    }
