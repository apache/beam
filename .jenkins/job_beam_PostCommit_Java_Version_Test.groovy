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

// This job runs the Java multi-JDK tests in postcommit, including WordCountIT.
mavenJob('beam_PostCommit_Java_Version_Test') {
  description('Runs postcommit tests on the Java SDK in multiple Jdk versions.')

  // Execute concurrent builds if necessary.
  concurrentBuild()

  // Set common parameters.
  common_job_properties.setTopLevelMainJobProperties(delegate)

  // Override jdk version here
  axes {
    label('label', 'linux')
    jdk('JDK 1.7 (latest)',
        'OpenJDK 7 (on Ubuntu only)',
        'OpenJDK 8 (on Ubuntu only)')
  }

  // Set maven parameters.
  common_job_properties.setMavenConfig(delegate)

  // Sets that this is a PostCommit job.
  common_job_properties.setPostCommit(delegate)

  // Allows triggering this build against pull requests.
  common_job_properties.enablePhraseTriggeringFromPullRequest(
      delegate,
      'Java JDK Versions Test',
      'Run Java Versions Test')

  // Maven build for this job.
  steps {
    // Maven build Java SDK related project
    maven('-B -e -P release clean install coveralls:report -DrepoToken=$COVERALLS_REPO_TOKEN')

    // Run WordCountIT

  }


  // goals('-B -e -P release,dataflow-runner clean install coveralls:report -DrepoToken=$COVERALLS_REPO_TOKEN -DskipITs=false -DintegrationTestPipelineOptions=\'[ "--project=apache-beam-testing", "--tempRoot=gs://temp-storage-for-end-to-end-tests", "--runner=org.apache.beam.runners.dataflow.testing.TestDataflowRunner" ]\'')
}
