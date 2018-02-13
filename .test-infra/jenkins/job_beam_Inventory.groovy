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

// These jobs list details about each beam runner, to clarify what software
// is on each machine.
def nums = 1..8
nums.each {
  def machine = "beam${it}"
  job("beam_Inventory_${machine}") {
    description("Run inventory on ${machine}")

    // Set common parameters.
    common_job_properties.setTopLevelMainJobProperties(delegate)

    // Sets that this is a cron job.
    common_job_properties.setCronJob(delegate, '45 18 * * *')

    // Allows triggering this build against pull requests.
    common_job_properties.enablePhraseTriggeringFromPullRequest(
      delegate,
      'Machine Inventory',
      "Run Inventory ${machine}")

    parameters {
      nodeParam('TEST_HOST') {
        description("Select test host ${machine}")
        defaultNodes([machine])
        allowedNodes([machine])
        trigger('multiSelectionDisallowed')
        eligibility('IgnoreOfflineNodeEligibility')
      }
    }

    steps {
      shell('ls /home/jenkins/tools')
      shell('ls /home/jenkins/tools/*')
      shell('python --version || echo "python not found"')
      shell('python3 --version || echo "python3 not found"')
      shell('/home/jenkins/tools/maven/latest/mvn -v || echo "mvn not found"')
      shell('/home/jenkins/tools/gradle4.3/gradle -v || echo "gradle not found"')
      shell('gcloud -v || echo "gcloud not found"')
      shell('kubectl version -c || echo "kubectl not found"')
      shell('virtualenv -p python2.7 test2 && . ./test2/bin/activate && python --version && deactivate || echo "python 2.7 not found"')
      shell('virtualenv -p python3 test3 && . ./test3/bin/activate && python --version && deactivate || echo "python 3 not found"')
      shell('echo "Maven home $MAVEN_HOME"')
      shell('env')
    }
  }
}
