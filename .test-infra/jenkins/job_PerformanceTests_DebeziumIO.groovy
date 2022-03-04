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
import LoadTestsBuilder as loadTestsBuilder
import PhraseTriggeringPostCommitBuilder
import InfluxDBCredentialsHelper

def now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

def debezium_read_test = [
  title          : 'DebeziumIO Read Performance Test',
  test           : 'apache_beam.io.gcp.experimental.debezium_read_perf_test',
  runner         : CommonTestProperties.Runner.DATAFLOW, // Ask for runner
  pipelineOptions: [
    job_name             : 'performance-tests-debezium-read' + now,
    project              : 'apache-beam-testing',
    // Run in us-west1 to colocate with beam-test spanner instance (BEAM-13222)
    region               : 'us-west1',
    temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
    spanner_instance     : 'beam-test', // Ask what instance for debezium
    spanner_database     : 'pyspanner_read_2gb', // Ask in which database debezium will read
    publish_to_big_query : true, // Ask this too
    metrics_dataset      : 'beam_performance',
    metrics_table        : 'debezium_read_results',
    influx_measurement   : 'debezium_read_results', // Ask what is influx  
    influx_db_name       : InfluxDBCredentialsHelper.InfluxDBDatabaseName, 
    influx_hostname      : InfluxDBCredentialsHelper.InfluxDBHostUrl,
    input_options        : '\'{' + // Ask what input options 
    '"num_records": 2097152,' +
    '"key_size": 1,' +
    '"value_size": 1024}\'',
    num_workers          : 5, // How many workers will need
    autoscaling_algorithm: 'NONE',  // Disable autoscale the worker pool.
  ]
]

def debezium_write_test = [
  title          : 'Debezium Write Performance Test',
  test           : 'apache_beam.io.gcp.experimental.debezium_write_perf_test',
  runner         : CommonTestProperties.Runner.DATAFLOW, // Ask for runner
  pipelineOptions: [
    job_name             : 'performance-tests-debezium-write' + now,
    project              : 'apache-beam-testing',
    // Run in us-west1 to colocate with beam-test spanner instance (BEAM-13222)
    region               : 'us-west1',
    temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
    spanner_instance     : 'beam-test', // Ask what instance for debezium 
    spanner_database     : 'debezium_write', // Ask in which database debezium
    publish_to_big_query : true, // Ask this too
    metrics_dataset      : 'beam_performance', 
    metrics_table        : 'debezium_write_results',
    influx_measurement   : 'debezium_write_results', // Ask what is influx
    influx_db_name       : InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname      : InfluxDBCredentialsHelper.InfluxDBHostUrl,
    input_options        : '\'{' + // Ask what input options 
    '"num_records": 2097152,' +
    '"key_size": 1,' +
    '"value_size": 1024}\'',
    num_workers          : 5, // Ask many workers will need 
    autoscaling_algorithm: 'NONE',  // Disable autoscale the worker pool.
  ]
]

def executeJob = { scope, testConfig ->
  commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 480)

  loadTestsBuilder.loadTest(scope, testConfig.title, testConfig.runner, CommonTestProperties.SDK.PYTHON, testConfig.pipelineOptions, testConfig.test)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_performanceTests_Debezium_read',
    'Run DebeziumIO Read Performance Test',
    'DebeziumIO Read Performance Test',
    this
    ) {
      executeJob(delegate, debezium_read_test)
    }

CronJobBuilder.cronJob('beam_performance_tests_debezium_read', 'H 15 * * *', this) {
  executeJob(delegate, debezium_read_test)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_PerformanceTests_Debezium_Write',
    'Run DebeziumIO Write Performance Test',
    'DebeziumIO Write Performance Test',
    this
    ) {
      executeJob(delegate, debezium_write_test)
    }

CronJobBuilder.cronJob('beam_PerformanceTests_Debezium_Write', 'H 15 * * *', this) {
  executeJob(delegate, debezium_write_test)
}
