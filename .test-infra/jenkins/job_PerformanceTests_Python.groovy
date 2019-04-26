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


class PerformanceTestConfigurations {
  String jobName
  String jobDescription
  String jobTriggerPhrase
  String buildSchedule = 'H */6 * * *'  // every 6 hours
  String benchmarkName = 'beam_integration_benchmark'
  String sdk = 'python'
  String bigqueryTable
  String itClass
  String itModule
  Boolean skipPrebuild = false
  String pythonSdkLocation
  String runner = 'TestDataflowRunner'
  Integer itTimeout = 1200
  Map extraPipelineArgs
}


def testConfigurations = [
    new PerformanceTestConfigurations(
        jobName           : 'beam_PerformanceTests_Python',
        jobDescription    : 'Python SDK Performance Test',
        jobTriggerPhrase  : 'Run Python Performance Test',
        bigqueryTable     : 'beam_performance.wordcount_py_pkb_results',
        skipPrebuild      : true,
        pythonSdkLocation : 'build/apache-beam.tar.gz',
        itClass           : 'apache_beam.examples.wordcount_it_test:WordCountIT.test_wordcount_it',
        itModule          : 'sdks/python',
        extraPipelineArgs : [
            project         : 'apache-beam-testing',
            staging_location: 'gs://temp-storage-for-end-to-end-tests/staging-it',
            temp_location   : 'gs://temp-storage-for-end-to-end-tests/temp-it',
            output          : 'gs://temp-storage-for-end-to-end-tests/py-it-cloud/output',
        ],
    ),
    new PerformanceTestConfigurations(
        jobName           : 'beam_PerformanceTests_Python35',
        jobDescription    : 'Python35 SDK Performance Test',
        jobTriggerPhrase  : 'Run Python35 Performance Test',
        bigqueryTable     : 'beam_performance.wordcount_py35_pkb_results',
        skipPrebuild      : true,
        pythonSdkLocation : 'test-suites/dataflow/py35/build/apache-beam.tar.gz',
        itClass           : 'apache_beam.examples.wordcount_it_test:WordCountIT.test_wordcount_it',
        itModule          : 'sdks/python/test-suites/dataflow/py35',
        extraPipelineArgs : [
            project         : 'apache-beam-testing',
            staging_location: 'gs://temp-storage-for-end-to-end-tests/staging-it',
            temp_location   : 'gs://temp-storage-for-end-to-end-tests/temp-it',
            output          : 'gs://temp-storage-for-end-to-end-tests/py-it-cloud/output',
        ],
    )
]


for (testConfig in testConfigurations) {
  createPythonPerformanceTestJob(testConfig)
}


private void createPythonPerformanceTestJob(PerformanceTestConfigurations testConfig) {
  // This job runs the Beam Python performance tests on PerfKit Benchmarker.
  job(testConfig.jobName) {
    // Set default Beam job properties.
    commonJobProperties.setTopLevelMainJobProperties(delegate)

    // Run job in postcommit, don't trigger every push.
    commonJobProperties.setAutoJob(
        delegate,
        testConfig.buildSchedule)

    // Allows triggering this build against pull requests.
    commonJobProperties.enablePhraseTriggeringFromPullRequest(
        delegate,
        testConfig.jobDescription,
        testConfig.jobTriggerPhrase)

    // Helper function to join pipeline args from a map.
    def joinPipelineArgs = { pipelineArgs ->
      def pipelineArgList = []
      pipelineArgs.each({
        key, value -> pipelineArgList.add("--$key=$value")
      })
      return pipelineArgList.join(',')
    }

    def argMap = [
        beam_sdk                : testConfig.sdk,
        benchmarks              : testConfig.benchmarkName,
        bigquery_table          : testConfig.bigqueryTable,
        beam_it_class           : testConfig.itClass,
        beam_it_module          : testConfig.itModule,
        beam_prebuilt           : testConfig.skipPrebuild.toString(),
        beam_python_sdk_location: testConfig.pythonSdkLocation,
        beam_runner             : testConfig.runner,
        beam_it_timeout         : testConfig.itTimeout.toString(),
        beam_it_args            : joinPipelineArgs(testConfig.extraPipelineArgs),
    ]

    commonJobProperties.buildPerformanceTest(delegate, argMap)
  }
}
