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

import LoadTestsBuilder as loadTestsBuilder
import PhraseTriggeringPostCommitBuilder

def now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

def smokeTestConfigurations = [
        [
                title        : 'GroupByKey Python load test Direct',
                itClass      : 'apache_beam.testing.load_tests.group_by_key_test:GroupByKeyTest.testGroupByKey',
                runner       : CommonTestProperties.Runner.DIRECT,
                sdk          : CommonTestProperties.SDK.PYTHON,
                jobProperties: [
                        publish_to_big_query: true,
                        project             : 'apache-beam-testing',
                        metrics_dataset     : 'load_test_SMOKE',
                        metrics_table       : 'python_direct_gbk',
                        input_options       : '\'{"num_records": 100000,' +
                                '"key_size": 1,' +
                                '"value_size":1}\'',

                ]
        ],
        [
                title        : 'GroupByKey Python load test Dataflow',
                itClass      : 'apache_beam.testing.load_tests.group_by_key_test:GroupByKeyTest.testGroupByKey',
                runner       : CommonTestProperties.Runner.DATAFLOW,
                sdk          : CommonTestProperties.SDK.PYTHON,
                jobProperties: [
                        job_name            : 'load-tests-python-dataflow-batch-gbk-smoke-' + now,
                        project             : 'apache-beam-testing',
                        temp_location       : 'gs://temp-storage-for-perf-tests/smoketests',
                        publish_to_big_query: true,
                        metrics_dataset     : 'load_test_SMOKE',
                        metrics_table       : 'python_dataflow_gbk',
                        input_options       : '\'{"num_records": 100000,' +
                                '"key_size": 1,' +
                                '"value_size":1}\'',
                        max_num_workers       : 1,
                ]
        ],
]

// Runs a tiny version load test suite to ensure nothing is broken.
PhraseTriggeringPostCommitBuilder.postCommitJob(
        'beam_Python_LoadTests_Smoke',
        'Run Python Load Tests Smoke',
        'Python Load Tests Smoke',
        this
) {
    loadTestsBuilder.loadTests(delegate, CommonTestProperties.SDK.PYTHON, smokeTestConfigurations, CommonTestProperties.TriggeringContext.PR, "GBK", "smoke")
}