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
import InfluxDBCredentialsHelper

def now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

// Common pipeline args for Dataflow job.
def dataflowPipelineArgs = [
  project         : 'apache-beam-testing',
  region          : 'us-central1',
  staging_location: 'gs://temp-storage-for-end-to-end-tests/staging-it',
  temp_location   : 'gs://temp-storage-for-end-to-end-tests/temp-it',
]

testConfigurations = []
pythonVersions = ['37']

for (pythonVersion in pythonVersions) {
  testConfigurations.add([
    jobName           : "beam_PerformanceTests_WordCountIT_Py${pythonVersion}",
    jobDescription    : "Python SDK Performance Test - Run WordCountIT in Py${pythonVersion} with 1Gb files",
    jobTriggerPhrase  : "Run Python${pythonVersion} WordCountIT Performance Test",
    test              : "apache_beam.examples.wordcount_it_test:WordCountIT.test_wordcount_it",
    gradleTaskName    : ":sdks:python:test-suites:dataflow:py${pythonVersion}:runPerformanceTest",
    pipelineOptions   : dataflowPipelineArgs + [
      job_name             : "performance-tests-wordcount-python${pythonVersion}-batch-1gb${now}",
      runner               : 'TestDataflowRunner',
      publish_to_big_query : true,
      metrics_dataset      : 'beam_performance',
      metrics_table        : "wordcount_py${pythonVersion}_pkb_results",
      influx_measurement   : "wordcount_py${pythonVersion}_results",
      influx_db_name       : InfluxDBCredentialsHelper.InfluxDBDatabaseName,
      influx_hostname      : InfluxDBCredentialsHelper.InfluxDBHostUrl,
      input                : "gs://apache-beam-samples/input_small_files/ascii_sort_1MB_input.0000*", // 1Gb
      output               : "gs://temp-storage-for-end-to-end-tests/py-it-cloud/output",
      expect_checksum      : "ea0ca2e5ee4ea5f218790f28d0b9fe7d09d8d710",
      num_workers          : '10',
      autoscaling_algorithm: "NONE",  // Disable autoscale the worker pool.
    ]
  ])
}

for (testConfig in testConfigurations) {
  createPythonPerformanceTestJob(testConfig)
}

private void createPythonPerformanceTestJob(Map testConfig) {
  // This job runs the Beam Python performance tests
  job(testConfig.jobName) {
    // Set default Beam job properties.
    commonJobProperties.setTopLevelMainJobProperties(delegate)

    InfluxDBCredentialsHelper.useCredentials(delegate)

    // Run job in postcommit, don't trigger every push.
    commonJobProperties.setAutoJob(delegate, 'H */6 * * *')

    // Allows triggering this build against pull requests.
    commonJobProperties.enablePhraseTriggeringFromPullRequest(
        delegate,
        testConfig.jobDescription,
        testConfig.jobTriggerPhrase,
        )

    publishers {
      archiveJunit('**/nosetests*.xml')
    }

    steps {
      gradle {
        rootBuildScriptDir(commonJobProperties.checkoutDir)
        switches("--info")
        switches("-Ptest-pipeline-options=\"${commonJobProperties.mapToArgString(testConfig.pipelineOptions)}\"")
        switches("-Ptest=${testConfig.test}")
        tasks(testConfig.gradleTaskName)
        commonJobProperties.setGradleSwitches(delegate)
      }
    }
  }
}
