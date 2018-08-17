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
def nums = 6//1..16
nums.each {
  def machine = "beam${it}"

  job("beam_Docker_Image_cleanup_${machine}") {
    description('Runs Docker Image Cleanup.')

    // Set common parameters.
    commonJobProperties.setTopLevelMainJobProperties(delegate)

    // Allows triggering this build against pull requests.
    commonJobProperties.enablePhraseTriggeringFromPullRequest(
            delegate,
            'Jenkins Docker Image Cleanup',
            "Run Docker Image Cleanup ${machine}")

    // This is a job that runs weekly.
//    commonJobProperties.setAutoJob(
//            delegate,
//            '0 12 * * 1')

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
      shell('docker image ls && docker rmi $(docker images --filter "dangling=true" -q --no-trunc)')
    }
  }
}
