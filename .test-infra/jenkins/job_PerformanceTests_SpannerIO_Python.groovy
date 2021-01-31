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

def spannerio_read_test = [
  title          : 'SpannerIO Read Performance Test Python 10 GB',
  test           : 'apache_beam.io.gcp.experimental.spannerio_read_perf_test',
  runner         : CommonTestProperties.Runner.DATAFLOW,
  pipelineOptions: [
    job_name             : 'performance-tests-spanner-read-python-10gb' + now,
    project              : 'apache-beam-testing',
    region               : 'us-central1',
    temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
    spanner_instance     : 'beam-test',
    spanner_database     : 'pyspanner_read_10gb',
    publish_to_big_query : true,
    metrics_dataset      : 'beam_performance',
    metrics_table        : 'pyspanner_read_10GB_results',
    influx_measurement   : 'python_spannerio_read_10GB_results',
    influx_db_name       : InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname      : InfluxDBCredentialsHelper.InfluxDBHostUrl,
    input_options        : '\'{' +
    '"num_records": 10485760,' +
    '"key_size": 1,' +
    '"value_size": 1024}\'',
    num_workers          : 5,
    autoscaling_algorithm: 'NONE',  // Disable autoscale the worker pool.
  ]
]

def spannerio_write_test = [
  title          : 'SpannerIO Write Performance Test Python Batch 10 GB',
  test           : 'apache_beam.io.gcp.experimental.spannerio_write_perf_test',
  runner         : CommonTestProperties.Runner.DATAFLOW,
  pipelineOptions: [
    job_name             : 'performance-tests-spannerio-write-python-batch-10gb' + now,
    project              : 'apache-beam-testing',
    region               : 'us-central1',
    temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
    spanner_instance     : 'beam-test',
    spanner_database     : 'pyspanner_write_10gb',
    publish_to_big_query : true,
    metrics_dataset      : 'beam_performance',
    metrics_table        : 'pyspanner_write_10GB_results',
    influx_measurement   : 'python_spanner_write_10GB_results',
    influx_db_name       : InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname      : InfluxDBCredentialsHelper.InfluxDBHostUrl,
    input_options        : '\'{' +
    '"num_records": 10485760,' +
    '"key_size": 1,' +
    '"value_size": 1024}\'',
    num_workers          : 5,
    autoscaling_algorithm: 'NONE',  // Disable autoscale the worker pool.
  ]
]

def executeJob = { scope, testConfig ->
  commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 240)

  loadTestsBuilder.loadTest(scope, testConfig.title, testConfig.runner, CommonTestProperties.SDK.PYTHON, testConfig.pipelineOptions, testConfig.test)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_PerformanceTests_SpannerIO_Read_Python',
    'Run SpannerIO Read Performance Test Python',
    'SpannerIO Read Performance Test Python',
    this
    ) {
      executeJob(delegate, spannerio_read_test)
    }

CronJobBuilder.cronJob('beam_PerformanceTests_SpannerIO_Read_Python', 'H 15 * * *', this) {
  executeJob(delegate, spannerio_read_test)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_PerformanceTests_SpannerIO_Write_Python_Batch',
    'Run SpannerIO Write Performance Test Python Batch',
    'SpannerIO Write Performance Test Python Batch',
    this
    ) {
      executeJob(delegate, spannerio_write_test)
    }

CronJobBuilder.cronJob('beam_PerformanceTests_SpannerIO_Write_Python_Batch', 'H 15 * * *', this) {
  executeJob(delegate, spannerio_write_test)
}
