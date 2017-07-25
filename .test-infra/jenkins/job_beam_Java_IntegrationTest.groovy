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

// This is the Java Jenkins job which runs the set of precommit integration tests.
mavenJob('beam_Java_IntegrationTest') {
  description('Runs Java Failsafe integration tests. Designed to be run as part of a pipeline.')

  // Set standard properties for a job which is part of a pipeline.
  common_job_properties.setPipelineJobProperties(delegate, 25, "Java Integration Tests")
  // Set standard properties for a job which pulls artifacts from an upstream job.
  common_job_properties.setPipelineDownstreamJobProperties(delegate, 'beam_Java_Build')

  // Profiles to activate in order to ensure runners are available at test time.
  profiles = [
    'jenkins-precommit',
    'direct-runner',
    'dataflow-runner',
    'spark-runner',
    'flink-runner',
    'apex-runner'
  ]
  // In the case of the precommit integration tests, we are currently only running the integration
  // tests in the examples directory. By directly invoking failsafe with an execution name (which we
  // do in order to avoid building artifacts again) we are required to enumerate each execution we
  // want to run, something which is feasible in this case.
  examples_integration_executions = [
    'apex-runner-integration-tests',
    'dataflow-runner-integration-tests',
    'dataflow-runner-integration-tests-streaming',
    'direct-runner-integration-tests',
    'flink-runner-integration-tests',
    'spark-runner-integration-tests',
  ]
  // Arguments to provide Maven.
  args = [
    '-B',
    '-e',
    "-P${profiles.join(',')}",
    "-pl examples/java",
  ]
  // This adds executions for each of the failsafe invocations listed above to the list of goals.
  examples_integration_executions.each({
    value -> args.add("failsafe:integration-test@${value}")
  })
  goals(args.join(' '))
}
