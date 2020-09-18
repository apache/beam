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

// These jobs list details about each beam runner, to clarify what software
// is on each machine.
def nums = 1..16
nums.each {
  def machine = "apache-beam-jenkins-${it}"
  job("beam_Inventory_${machine}") {
    description("Run inventory on ${machine}")

    // Set common parameters.
    commonJobProperties.setTopLevelMainJobProperties(delegate)

    // Sets that this is a cron job.
    commonJobProperties.setCronJob(delegate, '45 6,18 * * *')

    // Allows triggering this build against pull requests.
    commonJobProperties.enablePhraseTriggeringFromPullRequest(
        delegate,
        "Machine Inventory ${machine}",
        "Run Inventory ${machine}")

    parameters {
      nodeParam('TEST_HOST') {
        description("Select test host ${machine}")
        defaultNodes([machine])
        allowedNodes([machine])
        trigger('multiSelectionDisallowed')
        eligibility('IgnoreOfflineNodeEligibility')
      }
      stringParam {
        name("tmp_unaccessed_for")
        defaultValue("48")
        description("Files from /tmp dir that were not accessed for last `tmp_unaccessed_for` hours will be deleted.")
        trim(true)
      }
    }

    steps {
      shell('ls /home/jenkins/tools')
      shell('ls /home/jenkins/tools/*')
      shell('python --version || echo "python not found"')
      shell('python3 --version || echo "python3 not found"')
      shell('python3.5 --version || echo "python3.5 not found"')
      shell('python3.6 --version || echo "python3.6 not found"')
      shell('python3.7 --version || echo "python3.7 not found"')
      shell('/home/jenkins/tools/maven/latest/mvn -v || echo "mvn not found"')
      shell('/home/jenkins/tools/gradle4.3/gradle -v || echo "gradle not found"')
      shell('gcloud -v || echo "gcloud not found"')
      shell('kubectl version || echo "kubectl not found"')
      shell('virtualenv -p python3.5 test35 && . ./test35/bin/activate && python --version && deactivate || echo "python 3.5 not found"')
      shell('virtualenv -p python3.6 test36 && . ./test36/bin/activate && python --version && deactivate || echo "python 3.6 not found"')
      shell('virtualenv -p python3.7 test37 && . ./test37/bin/activate && python --version && deactivate || echo "python 3.7 not found"')
      shell('echo "Maven home $MAVEN_HOME"')
      shell('env')
      shell('docker system prune --all --filter until=24h --force')
      shell('echo "Current size of /tmp dir is \$(sudo du -sh /tmp)"')
      shell('echo "Deleting files accessed later than \${tmp_unaccessed_for} hours ago"')
      shell('sudo find /tmp -type f -amin +\$((60*\${tmp_unaccessed_for})) -print -delete')
      shell('echo "Size of /tmp dir after cleanup is \$(sudo du -sh /tmp)"')
    }
  }
}
