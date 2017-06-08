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

// This job runs the Java postcommit tests cross multiple JDK versions.
matrixJob('beam_PostCommit_Java_JDK_Versions_Test') {
  description('Runs postcommit tests on the Java SDK in multiple Jdk versions.')

  // Set common parameters.
  common_job_properties.setTopLevelMainJobProperties(delegate)

  // Set JDK versions.
  axes {
    label('label', 'beam')
    jdk('JDK 1.7 (latest)',
        'OpenJDK 7 (on Ubuntu only)',
        'OpenJDK 8 (on Ubuntu only)')
  }

  // Sets that this is a PostCommit job.
  common_job_properties.setPostCommit(
      delegate,
      '0 */6 * * *',
      false)

  // Allows triggering this build against pull requests.
  common_job_properties.enablePhraseTriggeringFromPullRequest(
      delegate,
      'Java JDK Version Test',
      'Run Java JDK Version Test')

  // Maven build for this job.
  steps {
    maven {
      // Set maven parameters.
      common_job_properties.setMavenConfig(delegate)

      // Maven build project.
      // Skip beam-sdks-python since this test is only apply to Java.
      // TODO[BEAM-2322,BEAM-2323,BEAM-2324]: Re-enable beam-runners-apex once the build is passed.
      goals('-B -e -P dataflow-runner clean install -pl \'!org.apache.beam:beam-sdks-python,!org.apache.beam:beam-runners-apex\' -DskipITs=false -DintegrationTestPipelineOptions=\'[ "--project=apache-beam-testing", "--tempRoot=gs://temp-storage-for-end-to-end-tests", "--runner=TestDataflowRunner" ]\'')
    }
  }
}
