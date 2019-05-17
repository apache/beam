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
  // Name of the Jenkins job
  String jobName
  // Description of the Jenkins job
  String jobDescription
  // Phrase to trigger this Jenkins job
  String jobTriggerPhrase
  // Frequency of the job build, default to every 6 hours
  String buildSchedule = 'H */6 * * *'
  // A benchmark defined flag, will pass to benchmark as "--benchmarkName"
  String benchmarkName = 'beam_integration_benchmark'
  // A benchmark defined flag, will pass to benchmark as "--bigqueryTable"
  String resultTable
  // A benchmark defined flag, will pass to benchmark as "--beam_it_class"
  String itClass
  // A benchmark defined flag, will pass to benchmark as "--beam_it_module".
  // It's a Gradle project that defines 'integrationTest' task. This task is executed by Perfkit
  // Beam benchmark launcher and can be added by enablePythonPerformanceTest() defined in
  // BeamModulePlugin.
  String itModule
  // A benchmark defined flag, will pass to benchmark as "--beam_python_sdk_location".
  // It's the location of Python SDK distribution archive which is required for TestDataflowRunner.
  String pythonSdkLocation = ''
  // A benchmark defined flag, will pass to benchmark as "--beam_runner"
  String runner = 'TestDataflowRunner'
  // A benchmark defined flag, will pass to benchmark as "--beam_it_timeout"
  Integer itTimeoutSec = 1200
  // A benchmark defined flag, will pass to benchmark as "--beam_it_args"
  Map extraPipelineArgs
}

// Common pipeline args for Dataflow job.
def dataflowPipelineArgs = [
    project         : 'apache-beam-testing',
    staging_location: 'gs://temp-storage-for-end-to-end-tests/staging-it',
    temp_location   : 'gs://temp-storage-for-end-to-end-tests/temp-it',
]


// Configurations of each Jenkins job.
def testConfigurations = [
    new PerformanceTestConfigurations(
        jobName           : 'beam_PerformanceTests_WordCountIT_Py27',
        jobDescription    : 'Python SDK Performance Test - Run WordCountIT in Py27',
        jobTriggerPhrase  : 'Run Python27 WordCountIT Performance Test',
        resultTable       : 'beam_performance.wordcount_py27_pkb_results',
        itClass           : 'apache_beam.examples.wordcount_it_test:WordCountIT.test_wordcount_it',
        itModule          : 'sdks/python',
        extraPipelineArgs : dataflowPipelineArgs + [
            output: 'gs://temp-storage-for-end-to-end-tests/py-it-cloud/output'
        ],
    ),
    new PerformanceTestConfigurations(
        jobName           : 'beam_PerformanceTests_WordCountIT_Py35',
        jobDescription    : 'Python SDK Performance Test - Run WordCountIT in Py35',
        jobTriggerPhrase  : 'Run Python35 WordCountIT Performance Test',
        resultTable       : 'beam_performance.wordcount_py35_pkb_results',
        itClass           : 'apache_beam.examples.wordcount_it_test:WordCountIT.test_wordcount_it',
        itModule          : 'sdks/python/test-suites/dataflow/py35',
        extraPipelineArgs : dataflowPipelineArgs + [
            output: 'gs://temp-storage-for-end-to-end-tests/py-it-cloud/output'
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

    def argMap = [
        beam_sdk                : 'python',
        benchmarks              : testConfig.benchmarkName,
        bigquery_table          : testConfig.resultTable,
        beam_it_class           : testConfig.itClass,
        beam_it_module          : testConfig.itModule,
        beam_prebuilt           : 'true',   // Python benchmark don't need to prebuild repo before running
        beam_python_sdk_location: getSDKLocationFromModule(testConfig.pythonSdkLocation,
                                                           testConfig.itModule),
        beam_runner             : testConfig.runner,
        beam_it_timeout         : testConfig.itTimeoutSec.toString(),
        beam_it_args            : joinPipelineArgs(testConfig.extraPipelineArgs),
    ]

    commonJobProperties.buildPerformanceTest(delegate, argMap)
  }
}


// Helper function to join pipeline args from a map.
private static String joinPipelineArgs(Map pipelineArgs) {
  def pipelineArgList = []
  pipelineArgs.each({
    key, value -> pipelineArgList.add("--$key=$value")
  })
  return pipelineArgList.join(',')
}


// Get relative path of sdk location based on itModule if the location is not provided.
private static String getSDKLocationFromModule(String pythonSDKLocation, String itModule) {
  if (!pythonSDKLocation && itModule.startsWith("sdks/python")) {
    return (itModule.substring("sdks/python".length()) + "/build/apache-beam.tar.gz").substring(1)
  }
  return pythonSDKLocation
}
