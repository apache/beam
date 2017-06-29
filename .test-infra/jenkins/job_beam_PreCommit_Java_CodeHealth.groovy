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

// This is the Java precommit which runs a maven install, and the current set
// of precommit tests.
mavenJob('beam_PreCommit_Java_CodeHealth') {
  description('Part of the PreCommit Pipeline. Runs Java code health checks.')

  parameters {
    stringParam(
      'buildNum',
      'N/A',
      'Build number of beam_PreCommit_Java_Build to copy from.')
    stringParam(
      'ghprbGhRepository',
      'N/A',
      'Repository name for use by ghprb plugin.')
    stringParam(
      'ghprbActualCommit',
      'N/A',
      'Commit ID for use by ghprb plugin.')
    stringParam(
      'ghprbPullId',
      'N/A',
      'PR # for use by ghprb plugin.')
  }

  // Set JDK version.
  jdk('JDK 1.8 (latest)')

  // Restrict this project to run only on Jenkins executors as specified
  label('beam')

  // Execute concurrent builds if necessary.
  concurrentBuild()

  wrappers {
    timeout {
      absolute(20)
      abortBuild()
    }
    credentialsBinding {
      string("COVERALLS_REPO_TOKEN", "beam-coveralls-token")
    }
    downstreamCommitStatus {
      context('Jenkins: Java Code Health')
      triggeredStatus("Java Code Health Pending")
      startedStatus("Running Java Code Health")
      statusUrl()
      completedStatus('SUCCESS', "Java Code Health Passed")
      completedStatus('FAILURE', "Java Code Health Failed")
      completedStatus('ERROR', "Error Executing Java Code Health")
    }
    // Set SPARK_LOCAL_IP for spark tests.
    environmentVariables {
      env('SPARK_LOCAL_IP', '127.0.0.1')
    }
  }

  // Set Maven parameters.
  common_job_properties.setMavenConfig(delegate)

  preBuildSteps {
    copyArtifacts("beam_PreCommit_Java_Build") {
      buildSelector {
        buildNumber('${buildNum}')
      }
    }
  }

  // Construct Maven goals for this job.
  profiles = [
    'release',
    'include-runners',
    'jenkins-precommit',
    'direct-runner',
    'dataflow-runner',
    'spark-runner',
    'flink-runner',
    'apex-runner'
  ]
  args = [
    '-B',
    '-e',
    '-P' + profiles.join(',')
    '-pl \'!sdks/python\'',
    'checkstyle:check',
    'findbugs:check',
    'rat:check'
  ]
  goals(args.join(' '))
}
