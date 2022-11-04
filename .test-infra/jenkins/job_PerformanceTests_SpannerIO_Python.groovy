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

def spannerio_read_test_2gb = [
  title          : 'SpannerIO Read Performance Test Python 2 GB',
  test           : 'apache_beam.io.gcp.experimental.spannerio_read_perf_test',
  runner         : CommonTestProperties.Runner.DATAFLOW,
  pipelineOptions: [
    job_name             : 'performance-tests-spanner-read-python-2gb' + now,
    project              : 'apache-beam-testing',
    // Run in us-west1 to colocate with beam-test spanner instance (BEAM-13222)
    region               : 'us-west1',
    temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
    spanner_instance     : 'beam-test',
    spanner_database     : 'pyspanner_read_2gb',
    publish_to_big_query : true,
    metrics_dataset      : 'beam_performance',
    metrics_table        : 'pyspanner_read_2GB_results',
    influx_measurement   : 'python_spannerio_read_2GB_results',
    influx_db_name       : InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname      : InfluxDBCredentialsHelper.InfluxDBHostUrl,
    input_options        : '\'{' +
    '"num_records": 2097152,' +
    '"key_size": 1,' +
    '"value_size": 1024}\'',
    num_workers          : 5,
    autoscaling_algorithm: 'NONE',  // Disable autoscale the worker pool.
  ]
]

def spannerio_write_test_2gb = [
  title          : 'SpannerIO Write Performance Test Python Batch 2 GB',
  test           : 'apache_beam.io.gcp.experimental.spannerio_write_perf_test',
  runner         : CommonTestProperties.Runner.DATAFLOW,
  pipelineOptions: [
    job_name             : 'performance-tests-spannerio-write-python-batch-2gb' + now,
    project              : 'apache-beam-testing',
    // Run in us-west1 to colocate with beam-test spanner instance (BEAM-13222)
    region               : 'us-west1',
    temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
    spanner_instance     : 'beam-test',
    spanner_database     : 'pyspanner_write_2gb',
    publish_to_big_query : true,
    metrics_dataset      : 'beam_performance',
    metrics_table        : 'pyspanner_write_2GB_results',
    influx_measurement   : 'python_spanner_write_2GB_results',
    influx_db_name       : InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname      : InfluxDBCredentialsHelper.InfluxDBHostUrl,
    input_options        : '\'{' +
    '"num_records": 2097152,' +
    '"key_size": 1,' +
    '"value_size": 1024}\'',
    num_workers          : 5,
    autoscaling_algorithm: 'NONE',  // Disable autoscale the worker pool.
  ]
]

def executeJob = { scope, testConfig ->
  commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 480)

  loadTestsBuilder.loadTest(scope, testConfig.title, testConfig.runner, CommonTestProperties.SDK.PYTHON, testConfig.pipelineOptions, testConfig.test)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_PerformanceTests_SpannerIO_Read_2GB_Python',
    'Run SpannerIO Read 2GB Performance Test Python',
    'SpannerIO Read 2GB Performance Test Python',
    this
    ) {
      executeJob(delegate, spannerio_read_test_2gb)
    }

CronJobBuilder.cronJob('beam_PerformanceTests_SpannerIO_Read_2GB_Python', 'H H * * *', this) {
  executeJob(delegate, spannerio_read_test_2gb)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_PerformanceTests_SpannerIO_Write_2GB_Python_Batch',
    'Run SpannerIO Write 2GB Performance Test Python Batch',
    'SpannerIO Write 2GB Performance Test Python Batch',
    this
    ) {
      executeJob(delegate, spannerio_write_test_2gb)
    }

CronJobBuilder.cronJob('beam_PerformanceTests_SpannerIO_Write_2GB_Python_Batch', 'H H * * *', this) {
  executeJob(delegate, spannerio_write_test_2gb)
}
