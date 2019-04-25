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

def loadTestConfigurations = [
        [
                title        : 'GroupByKey Python Load test: 2GB of 10B records',
                itClass      : 'apache_beam.testing.load_tests.group_by_key_test:GroupByKeyTest.testGroupByKey',
                runner       : CommonTestProperties.Runner.DATAFLOW,
                sdk          : CommonTestProperties.SDK.PYTHON,
                jobProperties: [
                        job_name             : 'load-tests-python-dataflow-batch-gbk-1-' + now,
                        project              : 'apache-beam-testing',
                        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
                        publish_to_big_query : true,
                        metrics_dataset      : 'load_test',
                        metrics_table        : 'python_dataflow_batch_gbk_1',
                        input_options        : '\'{"num_records": 200000000,' +
                                '"key_size": 1,' +
                                '"value_size": 9}\'',
                        iterations           : 1,
                        fanout               : 1,
                        max_num_workers      : 5,
                        num_workers          : 5,
                        autoscaling_algorithm: "NONE"
                ]
        ],
        [
                title        : 'GroupByKey Python Load test: 2GB of 100B records',
                itClass      : 'apache_beam.testing.load_tests.group_by_key_test:GroupByKeyTest.testGroupByKey',
                runner       : CommonTestProperties.Runner.DATAFLOW,
                sdk          : CommonTestProperties.SDK.PYTHON,
                jobProperties: [
                        job_name             : 'load-tests-python-dataflow-batch-gbk-2-' + now,
                        project              : 'apache-beam-testing',
                        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
                        publish_to_big_query : true,
                        metrics_dataset      : 'load_test',
                        metrics_table        : 'python_dataflow_batch_gbk_2',
                        input_options        : '\'{"num_records": 20000000,' +
                                '"key_size": 10,' +
                                '"value_size": 90}\'',
                        iterations           : 1,
                        fanout               : 1,
                        max_num_workers      : 5,
                        num_workers          : 5,
                        autoscaling_algorithm: "NONE"
                ]
        ],
        [
                title        : 'GroupByKey Python Load test: 2GB of 100kB records',
                itClass      : 'apache_beam.testing.load_tests.group_by_key_test:GroupByKeyTest.testGroupByKey',
                runner       : CommonTestProperties.Runner.DATAFLOW,
                sdk          : CommonTestProperties.SDK.PYTHON,
                jobProperties: [
                        job_name             : 'load-tests-python-dataflow-batch-gbk-3-' + now,
                        project              : 'apache-beam-testing',
                        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
                        publish_to_big_query : true,
                        metrics_dataset      : 'load_test',
                        metrics_table        : 'python_dataflow_batch_gbk_3',
                        input_options        : '\'{"num_records": 2000,' +
                                '"key_size": 100000,' +
                                '"value_size": 900000}\'',
                        iterations           : 1,
                        fanout               : 1,
                        max_num_workers      : 5,
                        num_workers          : 5,
                        autoscaling_algorithm: "NONE"
                ]
        ],
        [
                title        : 'GroupByKey Python Load test: fanout 4 times with 2GB 10-byte records total',
                itClass      : 'apache_beam.testing.load_tests.group_by_key_test:GroupByKeyTest.testGroupByKey',
                runner       : CommonTestProperties.Runner.DATAFLOW,
                sdk          : CommonTestProperties.SDK.PYTHON,
                jobProperties: [
                        job_name             : 'load-tests-python-dataflow-batch-gbk-4-' + now,
                        project              : 'apache-beam-testing',
                        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
                        publish_to_big_query : true,
                        metrics_dataset      : 'load_test',
                        metrics_table        : 'python_dataflow_batch_gbk_4',
                        input_options        : '\'{"num_records": 5000000,' +
                                '"key_size": 10,' +
                                '"value_size": 90}\'',
                        iterations           : 1,
                        fanout               : 4,
                        max_num_workers      : 5,
                        num_workers          : 5,
                        autoscaling_algorithm: "NONE"
                ]
        ],
        [
                title        : 'GroupByKey Python Load test: fanout 8 times with 2GB 10-byte records total',
                itClass      : 'apache_beam.testing.load_tests.group_by_key_test:GroupByKeyTest.testGroupByKey',
                runner       : CommonTestProperties.Runner.DATAFLOW,
                sdk          : CommonTestProperties.SDK.PYTHON,
                jobProperties: [
                        job_name             : 'load-tests-python-dataflow-batch-gbk-5-' + now,
                        project              : 'apache-beam-testing',
                        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
                        publish_to_big_query : true,
                        metrics_dataset      : 'load_test',
                        metrics_table        : 'python_dataflow_batch_gbk_5',
                        input_options        : '\'{"num_records": 2500000,' +
                                '"key_size": 10,' +
                                '"value_size": 90}\'',
                        iterations           : 1,
                        fanout               : 8,
                        max_num_workers      : 5,
                        num_workers          : 5,
                        autoscaling_algorithm: "NONE"
                ]
        ],
]

PhraseTriggeringPostCommitBuilder.postCommitJob(
        'beam_Python_LoadTests_GBK_Dataflow_Batch',
        'Run Python Load Tests GBK Dataflow Batch',
        'Load Tests Python GBK Dataflow Batch suite',
        this
) {
        loadTestsBuilder.loadTests(delegate, CommonTestProperties.SDK.PYTHON, loadTestConfigurations, CommonTestProperties.TriggeringContext.PR, "GBK", "batch")
}

CronJobBuilder.cronJob('beam_LoadTests_Python_GBK_Dataflow_Batch', 'H 12 * * *', this) {
        loadTestsBuilder.loadTests(delegate, CommonTestProperties.SDK.PYTHON, loadTestConfigurations, CommonTestProperties.TriggeringContext.POST_COMMIT, "GBK", "batch")
}