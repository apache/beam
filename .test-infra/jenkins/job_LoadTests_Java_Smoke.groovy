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

def smokeTestConfigurations = [
        [
                title        : 'GroupByKey load test Direct',
                itClass      : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                runner       : CommonTestProperties.Runner.DIRECT,
                jobProperties: [
                        publishToBigQuery: true,
                        bigQueryDataset  : 'load_test_SMOKE',
                        bigQueryTable    : 'direct_gbk',
                        sourceOptions    : '{"numRecords":100000,"splitPointFrequencyRecords":1}',
                        stepOptions      : '{"outputRecordsPerInputRecord":1,"preservesInputKeyDistribution":true}',
                        fanout           : 10,
                        iterations       : 1,
                ]
        ],
        [
                title        : 'GroupByKey load test Dataflow',
                itClass      : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                runner       : CommonTestProperties.Runner.DATAFLOW,
                jobProperties: [
                        project          : 'apache-beam-testing',
                        tempLocation     : 'gs://temp-storage-for-perf-tests/smoketests',
                        publishToBigQuery: true,
                        bigQueryDataset  : 'load_test_SMOKE',
                        bigQueryTable    : 'dataflow_gbk',
                        sourceOptions    : '{"numRecords":100000,"splitPointFrequencyRecords":1}',
                        stepOptions      : '{"outputRecordsPerInputRecord":1,"preservesInputKeyDistribution":true}',
                        fanout           : 10,
                        iterations       : 1,
                ]
        ],
        [
                title        : 'GroupByKey load test Flink',
                itClass      : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                runner       : CommonTestProperties.Runner.FLINK,
                jobProperties: [
                        publishToBigQuery: true,
                        bigQueryDataset  : 'load_test_SMOKE',
                        bigQueryTable    : 'flink_gbk',
                        sourceOptions    : '{"numRecords":100000,"splitPointFrequencyRecords":1}',
                        stepOptions      : '{"outputRecordsPerInputRecord":1,"preservesInputKeyDistribution":true}',
                        fanout           : 10,
                        iterations       : 1,
                ]
        ],
        [
                title        : 'GroupByKey load test Spark',
                itClass      : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                runner       : CommonTestProperties.Runner.SPARK,
                jobProperties: [
                        sparkMaster      : 'local[4]',
                        publishToBigQuery: true,
                        bigQueryDataset  : 'load_test_SMOKE',
                        bigQueryTable    : 'spark_gbk',
                        sourceOptions    : '{"numRecords":100000,"splitPointFrequencyRecords":1}',
                        stepOptions      : '{"outputRecordsPerInputRecord":1,"preservesInputKeyDistribution":true}',
                        fanout           : 10,
                        iterations       : 1,
                ]
        ]
]


// Runs a tiny version load test suite to ensure nothing is broken.
PhraseTriggeringPostCommitBuilder.postCommitJob(
        'beam_Java_LoadTests_Smoke',
        'Run Java Load Tests Smoke',
        'Java Load Tests Smoke',
        this
) {
  loadTestsBuilder.loadTests(delegate, CommonTestProperties.SDK.JAVA, smokeTestConfigurations, CommonTestProperties.TriggeringContext.PR, "GBK", "smoke")
}
