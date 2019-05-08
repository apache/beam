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

// This job runs the Beam Python35 performance benchmark on PerfKit Benchmarker.
job('beam_PerformanceTests_Python35'){
  // Set default Beam job properties.
  commonJobProperties.setTopLevelMainJobProperties(delegate)

  // Run job in postcommit every 6 hours, don't trigger every push.
  commonJobProperties.setAutoJob(
      delegate,
      'H */6 * * *')

  // Allows triggering this build against pull requests.
  commonJobProperties.enablePhraseTriggeringFromPullRequest(
      delegate,
      'Python35 SDK Performance Test',
      'Run Python35 Performance Test')

  def pipelineArgs = [
      project: 'apache-beam-testing',
      staging_location: 'gs://temp-storage-for-end-to-end-tests/staging-it',
      temp_location: 'gs://temp-storage-for-end-to-end-tests/temp-it',
      output: 'gs://temp-storage-for-end-to-end-tests/py-it-cloud/output'
  ]
  def pipelineArgList = []
  pipelineArgs.each({
    key, value -> pipelineArgList.add("--$key=$value")
  })
  def pipelineArgsJoined = pipelineArgList.join(',')

  def argMap = [
      beam_sdk                 : 'python',
      benchmarks               : 'beam_integration_benchmark',
      bigquery_table           : 'beam_performance.wordcount_py35_pkb_results',
      beam_it_class            : 'apache_beam.examples.wordcount_it_test:WordCountIT.test_wordcount_it',
      beam_it_module           : 'sdks/python/test-suites/dataflow/py35',
      beam_prebuilt            : 'true',  // skip beam prebuild
      beam_python_sdk_location : 'test-suites/dataflow/py35/build/apache-beam.tar.gz',
      beam_runner              : 'TestDataflowRunner',
      beam_it_timeout          : '1200',
      beam_it_args             : pipelineArgsJoined,
  ]

  commonJobProperties.buildPerformanceTest(delegate, argMap)
}
