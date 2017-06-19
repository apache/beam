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

import common_job_properties

// This job runs the Beam Python performance tests on PerfKit Benchmarker.
job('beam_PerformanceTests_Python'){
  // Set default Beam job properties.
  common_job_properties.setTopLevelMainJobProperties(delegate)

  // Run job in postcommit every 6 hours, don't trigger every push, and
  // don't email individual committers.
  common_job_properties.setPostCommit(
      delegate,
      '0 */6 * * *',
      false,
//      'commits@beam.apache.org',
      '',
      false)

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
      beam_sdk : 'python',
      benchmarks: 'beam_integration_benchmark',
      beam_it_args: pipelineArgsJoined
  ]

  common_job_properties.buildPerformanceTest(delegate, argMap)
}

