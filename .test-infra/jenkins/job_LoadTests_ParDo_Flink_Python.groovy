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
import Flink
import Docker
import InfluxDBCredentialsHelper

String now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

/**
 * The test results for these load tests reside in BigQuery in the load_test/load_test_PRs table of the
 * apache-beam-testing project. A dashboard is available here:
 * https://apache-beam-testing.appspot.com/explore?dashboard=5751884853805056
 *
 * For example:
 *   SELECT
 * 	  timestamp, value
 *   FROM
 * 	  apache-beam-testing.load_test_PRs.python_flink_batch_pardo_1
 *   ORDER BY
 *    timestamp
 *
 * The following query has been been used to visualize the checkpoint results of python_flink_streaming_pardo_6:
 *   Select timestamp, min, sum/count as avg, max
 *   FROM (
 *     SELECT
 *       timestamp,
 *       MAX(IF(metric LIKE "%\\_min\\_%", value, null)) min,
 *       MAX(IF(metric LIKE "%\\_sum\\_%", value, null)) sum,
 *       MAX(IF(metric LIKE "%\\_count\\_%", value, null)) count,
 *       MAX(IF(metric LIKE "%\\_max\\_%", value, null)) max
 *     FROM apache-beam-testing.load_test_PRs.python_flink_streaming_pardo_6
 *     WHERE metric like "%loadgenerator/impulse%"
 *     GROUP BY test_id, timestamp
 *     ORDER BY timestamp
 *   );
 */

def batchScenarios = { datasetName -> [
        [
                title          : 'ParDo Python Load test: 20M 100 byte records 10 times',
                test           : 'apache_beam.testing.load_tests.pardo_test',
                runner         : CommonTestProperties.Runner.PORTABLE,
                pipelineOptions: [
                        job_name             : 'load-tests-python-flink-batch-pardo-1-' + now,
                        project              : 'apache-beam-testing',
                        publish_to_big_query : true,
                        metrics_dataset      : datasetName,
                        metrics_table        : 'python_flink_batch_pardo_1',
                        influx_measurement   : 'python_batch_pardo_1',
                        input_options        : '\'{' +
                                '"num_records": 20000000,' +
                                '"key_size": 10,' +
                                '"value_size": 90}\'',
                        iterations           : 10,
                        number_of_counter_operations: 0,
                        number_of_counters   : 0,
                        parallelism          : 5,
                        job_endpoint         : 'localhost:8099',
                        environment_type     : 'DOCKER',
                ]
        ],
// TODO(BEAM-10270): Takes too long time to execute (currently more than 3 hours). Re-enable
// the test after its overhead is reduced.
//         [
//                 title          : 'ParDo Python Load test: 20M 100 byte records 200 times',
//                 test           : 'apache_beam.testing.load_tests.pardo_test',
//                 runner         : CommonTestProperties.Runner.PORTABLE,
//                 pipelineOptions: [
//                         job_name             : 'load-tests-python-flink-batch-pardo-2-' + now,
//                         project              : 'apache-beam-testing',
//                         publish_to_big_query : true,
//                         metrics_dataset      : datasetName,
//                         metrics_table        : 'python_flink_batch_pardo_2',
//                         influx_measurement   : 'python_batch_pardo_2',
//                         input_options        : '\'{' +
//                                 '"num_records": 20000000,' +
//                                 '"key_size": 10,' +
//                                 '"value_size": 90}\'',
//                         iterations           : 200,
//                         number_of_counter_operations: 0,
//                         number_of_counters   : 0,
//                         parallelism          : 5,
//                         job_endpoint         : 'localhost:8099',
//                         environment_type     : 'DOCKER',
//                 ]
//         ],
        [
                title          : 'ParDo Python Load test: 20M 100 byte records 10 counters',
                test           : 'apache_beam.testing.load_tests.pardo_test',
                runner         : CommonTestProperties.Runner.PORTABLE,
                pipelineOptions: [
                        job_name             : 'load-tests-python-flink-batch-pardo-3-' + now,
                        project              : 'apache-beam-testing',
                        publish_to_big_query : true,
                        metrics_dataset      : datasetName,
                        metrics_table        : 'python_flink_batch_pardo_3',
                        influx_measurement   : 'python_batch_pardo_3',
                        input_options        : '\'{' +
                                '"num_records": 20000000,' +
                                '"key_size": 10,' +
                                '"value_size": 90}\'',
                        iterations           : 1,
                        number_of_counter_operations: 10,
                        number_of_counters   : 1,
                        parallelism          : 5,
                        job_endpoint         : 'localhost:8099',
                        environment_type     : 'DOCKER',
                ]
        ],
        [
                title          : 'ParDo Python Load test: 20M 100 byte records 100 counters',
                test           : 'apache_beam.testing.load_tests.pardo_test',
                runner         : CommonTestProperties.Runner.PORTABLE,
                pipelineOptions: [
                        job_name             : 'load-tests-python-flink-batch-pardo-4-' + now,
                        project              : 'apache-beam-testing',
                        publish_to_big_query : true,
                        metrics_dataset      : datasetName,
                        metrics_table        : 'python_flink_batch_pardo_4',
                        influx_measurement   : 'python_batch_pardo_4',
                        input_options        : '\'{' +
                                '"num_records": 20000000,' +
                                '"key_size": 10,' +
                                '"value_size": 90}\'',
                        iterations           : 1,
                        number_of_counter_operations: 100,
                        number_of_counters   : 1,
                        parallelism          : 5,
                        job_endpoint         : 'localhost:8099',
                        environment_type     : 'DOCKER',
                ]
        ],
    ].each { test -> test.pipelineOptions.putAll(additionalPipelineArgs) }
}

def streamingScenarios = { datasetName -> [
    [
        title          : 'ParDo Python Stateful Streaming Load test: 2M 100 byte records',
        test           : 'apache_beam.testing.load_tests.pardo_test',
        runner         : CommonTestProperties.Runner.PORTABLE,
        pipelineOptions: [
            job_name             : 'load-tests-python-flink-streaming-pardo-5-' + now,
            project              : 'apache-beam-testing',
            publish_to_big_query : true,
            metrics_dataset      : datasetName,
            metrics_table        : 'python_flink_streaming_pardo_5',
            influx_measurement   : 'python_streaming_pardo_5',
            input_options        : '\'{' +
                '"num_records": 2000000,' +
                '"key_size": 10,' +
                '"value_size": 90}\'',
            iterations           : 5,
            number_of_counter_operations: 10,
            number_of_counters   : 3,
            parallelism          : 5,
            // Turn on streaming mode (flags are indicated with null values)
            streaming            : null,
            stateful             : null,
            job_endpoint         : 'localhost:8099',
            environment_type     : 'DOCKER',
        ]
    ],
    [
        title          : 'ParDo Python Stateful Streaming with Checkpointing test: 2M 100 byte records',
        test           : 'apache_beam.testing.load_tests.pardo_test',
        runner         : CommonTestProperties.Runner.PORTABLE,
        pipelineOptions: [
            job_name             : 'load-tests-python-flink-streaming-pardo-6-' + now,
            project              : 'apache-beam-testing',
            publish_to_big_query : true,
            metrics_dataset      : datasetName,
            metrics_table        : 'python_flink_streaming_pardo_6',
            influx_measurement   : 'python_streaming_pardo_6',
            input_options        : '\'{' +
                '"num_records": 2000000,' +
                '"key_size": 10,' +
                '"value_size": 90}\'',
            iterations           : 5,
            number_of_counter_operations: 10,
            number_of_counters   : 3,
            parallelism          : 5,
            // Turn on streaming mode (flags are indicated with null values)
            streaming            : null,
            stateful             : null,
            // Enable checkpointing every 10 seconds
            checkpointing_interval : 10000,
            // Report checkpointing stats to this namespace
            report_checkpoint_duration : 'python_flink_streaming_pardo_6',
            // Ensure that we can checkpoint the pipeline for at least 5 minutes to gather checkpointing stats
            shutdown_sources_after_idle_ms: 300000,
            job_endpoint         : 'localhost:8099',
            environment_type     : 'DOCKER',
        ]
    ],
  ].each { test -> test.pipelineOptions.putAll(additionalPipelineArgs) }
}

def loadBatchTests = { scope, triggeringContext ->
  Docker publisher = new Docker(scope, loadTestsBuilder.DOCKER_CONTAINER_REGISTRY)
  String pythonHarnessImageTag = publisher.getFullImageName('beam_python3.7_sdk')

  def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', triggeringContext)
  def numberOfWorkers = 5
  additionalPipelineArgs << [environment_config: pythonHarnessImageTag]
  List<Map> batchTestScenarios = batchScenarios(datasetName)

  publisher.publish(':sdks:python:container:py37:docker', 'beam_python3.7_sdk')
  publisher.publish(':runners:flink:1.10:job-server-container:docker', 'beam_flink1.10_job_server')

  Flink flink = new Flink(scope, 'beam_LoadTests_Python_ParDo_Flink_Batch')
  flink.setUp([pythonHarnessImageTag], numberOfWorkers, publisher.getFullImageName('beam_flink1.10_job_server'))
  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.PYTHON_37, batchTestScenarios, 'ParDo', 'batch')
}

def loadStreamingTests = { scope, triggeringContext ->
  Docker publisher = new Docker(scope, loadTestsBuilder.DOCKER_CONTAINER_REGISTRY)
  String pythonHarnessImageTag = publisher.getFullImageName('beam_python3.7_sdk')

  def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', triggeringContext)
  def numberOfWorkers = 5
  additionalPipelineArgs << [environment_config: pythonHarnessImageTag]
  List<Map> streamingTestScenarios = streamingScenarios(datasetName)

  publisher.publish(':sdks:python:container:py37:docker', 'beam_python3.7_sdk')
  publisher.publish(':runners:flink:1.10:job-server-container:docker', 'beam_flink1.10_job_server', 'streaming-load-tests')

  Flink flink = new Flink(scope, 'beam_LoadTests_Python_ParDo_Flink_Streaming')
  flink.setUp([pythonHarnessImageTag], numberOfWorkers, publisher.getFullImageName('beam_flink1.10_job_server', 'streaming-load-tests'))
  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.PYTHON_37, streamingTestScenarios, 'ParDo', 'streaming')
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
  'beam_LoadTests_Python_ParDo_Flink_Batch',
  'Run Python Load Tests ParDo Flink Batch',
  'Load Tests Python ParDo Flink Batch suite',
  this
) {
  additionalPipelineArgs = [:]
  loadBatchTests(delegate, CommonTestProperties.TriggeringContext.PR)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Python_ParDo_Flink_Streaming',
    'Run Python Load Tests ParDo Flink Streaming',
    'Load Tests Python ParDo Flink Streaming suite',
    this
) {
  additionalPipelineArgs = [:]
  loadStreamingTests(delegate, CommonTestProperties.TriggeringContext.PR)
}

CronJobBuilder.cronJob('beam_LoadTests_Python_ParDo_Flink_Batch', 'H 13 * * *', this) {
  additionalPipelineArgs = [
      influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
      influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostname,
  ]
  loadBatchTests(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT)
}

CronJobBuilder.cronJob('beam_LoadTests_Python_ParDo_Flink_Streaming', 'H 13 * * *', this) {
  additionalPipelineArgs = [
      influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
      influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostname,
  ]
  loadStreamingTests(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT)
}
