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
mavenJob('beam_PreCommit_Java_UnitTest') {
  description('Part of the PreCommit Pipeline. Runs Java Surefire unit tests.')

  parameters {
    stringParam(
      'buildNum',
      'N/A',
      'Build number of beam_PreCommit_Java_Build to copy from.')
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
      context('Jenkins: Java Unit Tests')
      triggeredStatus("Java Unit Tests Pending")
      startedStatus("Running Java Unit Tests")
      statusUrl()
      completedStatus('SUCCESS', "Java Unit Tests Passed")
      completedStatus('FAILURE', "Some Java Unit Tests Failed")
      completedStatus('ERROR', "Error Executing Java Unit Tests")
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
        buildNumber("${buildNum}")
      }
    }
  }

  // Maven goals for this job.
  goals('-B -e -Prelease,include-runners,jenkins-precommit,direct-runner,dataflow-runner,spark-runner,flink-runner,apex-runner surefire:test@default-test -pl \'!sdks/python\' -DrepoToken=$COVERALLS_REPO_TOKEN -DpullRequest=$ghprbPullId coveralls:report')
}
