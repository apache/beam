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

job('beam_PostRelease_Python36_Candidate_MobileGame_Dataflow') {
    description('Runs mobile game verification of the Python36 release candidate with dataflow runner by using tar and wheel.')

    // Set common parameters.
    commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 360)

    // Allows triggering this build against pull requests.
    commonJobProperties.enablePhraseTriggeringFromPullRequest(
            delegate,
            'Run Py36 ReleaseCandidate Dataflow MobileGame')

    // Execute shell command to test Python SDK.
    steps {
        shell('cd ' + commonJobProperties.checkoutDir +
                ' && bash release/src/main/python-release/python_release_automation.sh 3.6 dataflow mobile_game')
    }
}