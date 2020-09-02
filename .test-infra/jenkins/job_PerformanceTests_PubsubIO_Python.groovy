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

import static java.util.UUID.randomUUID

def now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

def withDataflowWorkerJar = true

def psio_test = [
  title          : 'PubsubIO Write Performance Test Python 2GB',
  test           : 'apache_beam.io.gcp.pubsub_io_perf_test',
  runner         : CommonTestProperties.Runner.TEST_DATAFLOW,
  pipelineOptions: [
    job_name                  : 'performance-tests-psio-python-2gb' + now,
    project                   : 'apache-beam-testing',
    region                    : 'us-central1',
    temp_location             : 'gs://temp-storage-for-perf-tests/loadtests',
    publish_to_big_query      : true,
    metrics_dataset           : 'beam_performance',
    metrics_table             : 'psio_io_2GB_results',
    influx_measurement        : 'python_psio_2GB_results',
    influx_db_name            : InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname           : InfluxDBCredentialsHelper.InfluxDBHostUrl,
    input_options             : '\'{' +
    '"num_records": 2097152,' +
    '"key_size": 1,' +
    '"value_size": 1024}\'',
    num_workers               : 5,
    autoscaling_algorithm     : 'NONE',  // Disable autoscale the worker pool.
    pubsub_namespace_prefix   : 'pubsub_io_performance_',
    wait_until_finish_duration: 1000 * 60 * 10, // in milliseconds
  ]
]

def executeJob = { scope, testConfig ->
  commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 240)

  loadTestsBuilder.loadTest(scope, testConfig.title, testConfig.runner,
      CommonTestProperties.SDK.PYTHON_37, testConfig.pipelineOptions, testConfig.test, withDataflowWorkerJar)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_PerformanceTests_PubsubIOIT_Python_Streaming',
    'Run PubsubIO Performance Test Python',
    'PubsubIO Performance Test Python',
    this
    ) {
      executeJob(delegate, psio_test)
    }

CronJobBuilder.cronJob('beam_PerformanceTests_PubsubIOIT_Python_Streaming', 'H 15 * * *', this) {
  executeJob(delegate, psio_test)
}
