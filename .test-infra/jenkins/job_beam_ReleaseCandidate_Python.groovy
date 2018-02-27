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

job('beam_PostRelease_Python_Candidate') {
    description('Runs verification of the Python release candidate.')

    // Execute concurrent builds if necessary.
    concurrentBuild()

    // Set common parameters.
    common_job_properties.setTopLevelMainJobProperties(delegate)

    parameters {
        nodeParam('TEST_HOST') {
            description('select test host as either beam1, 2 or 3')
            defaultNodes(['beam1', 'beam2', 'beam3'])
            allowedNodes(['beam1', 'beam2', 'beam3'])
            trigger('multiSelectionDisallowed')
            eligibility('IgnoreOfflineNodeEligibility')
        }
    }

    // Allows triggering this build against pull requests.
    common_job_properties.enablePhraseTriggeringFromPullRequest(
            delegate,
            'Python SDK Release Candidates Validation',
            'Run Python ReleaseCandidate')

    // Execute shell command to test Python SDK.
    steps {
        shell('cd ' + common_job_properties.checkoutDir +
                ' && bash release/src/main/groovy/run_release_candidate_python_validation.sh')
    }
}
